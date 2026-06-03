package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// Nested Top Action kinds. Each value identifies the kind of structural
// change being logged so future recovery code can choose how to handle
// a loser NTA (today: restore the captured before-images regardless of
// kind; the kind is informational / for diagnostics).
const (
	NTAKindBTreeSplit  uint8 = 1
	NTAKindBTreeMerge  uint8 = 2
	NTAKindHeapCompact uint8 = 3
)

// NTAPage captures the (path, pageID) of a page touched by a structural
// op, together with the full before-image bytes of the page. Recovery
// writes the before-image back when the enclosing NTA did not commit.
type NTAPage struct {
	Path     string
	PageID   pagestore.PageID
	PreImage []byte // length == pagestore.PageSize
}

// StructuralLogger is the interface a structural store (B+ tree heap)
// uses to wrap atomic page-group mutations in WAL-durable Begin/Commit
// pairs. The store calls BeginNTA before any in-memory mutation, with
// the before-images of every page it intends to touch; on success it
// calls CommitNTA. Failures between Begin and Commit are recovered by
// the engine on the next open: recovery restores the captured
// before-images so the on-disk pages return to their pre-NTA state.
type StructuralLogger interface {
	BeginNTA(kind uint8, pages []NTAPage) (ntaID uint64, err error)
	CommitNTA(ntaID uint64) error
}

// btreeStructuralLogger wraps the engine so it satisfies the
// btreev2.StructuralLogger interface. Defined here because both types
// have identical shape; we translate the btreev2.NTAPage values into
// engine NTAPage values at the boundary.
type btreeStructuralLogger struct {
	se *StorageEngine
}

func (l btreeStructuralLogger) BeginNTA(kind uint8, pages []btreev2.NTAPage) (uint64, error) {
	out := make([]NTAPage, len(pages))
	for i, p := range pages {
		out[i] = NTAPage{Path: p.Path, PageID: p.PageID, PreImage: p.PreImage}
	}
	return l.se.writeNTABegin(kind, out)
}

func (l btreeStructuralLogger) CommitNTA(ntaID uint64) error {
	return l.se.writeNTACommit(ntaID)
}

// pageWriter is the subset of the engine's persistence layer we need
// to flush a recovered page back to disk during NTA rollback. The
// concrete implementations are HeapV2 / BTreeV2 (their PageFile).
type pageWriter interface {
	Path() string
	WritePageBytes(pageID pagestore.PageID, page []byte) error
}

// rollbackPartialNTAs restores the captured before-images for every
// nested top action that did not see its NTACommit. Called once,
// during recovery, after the redo / logical-undo passes finish.
//
// The writes go directly to the underlying page file (bypassing the
// buffer pool) because recovery still owns the engine exclusively at
// this point; any in-memory frame for the affected pages will be
// re-fetched fresh on the first read after recovery completes.
func (se *StorageEngine) rollbackPartialNTAs(analysis *recoveryAnalysis) (int, error) {
	if se == nil || analysis == nil || len(analysis.PartialNTAs) == 0 {
		return 0, nil
	}
	writers := se.ntaPageWriters()
	rolledBack := 0
	for ntaLSN, nta := range analysis.PartialNTAs {
		for _, p := range nta.Pages {
			writer, ok := writers[p.Path]
			if !ok {
				return rolledBack, fmt.Errorf("storage: NTA rollback: no writer registered for path %q (nta lsn %d)", p.Path, ntaLSN)
			}
			if err := writer.WritePageBytes(p.PageID, p.PreImage); err != nil {
				return rolledBack, fmt.Errorf("storage: NTA rollback page %s/%d: %w", p.Path, p.PageID, err)
			}
		}
		rolledBack++
	}
	return rolledBack, nil
}

// ntaPageWriters returns a path-keyed view of every page file the
// engine currently knows about (one per heap, one per index tree).
func (se *StorageEngine) ntaPageWriters() map[string]pageWriter {
	writers := make(map[string]pageWriter)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}
		if w, ok := table.Heap.(pageWriter); ok {
			writers[w.Path()] = w
		}
		for _, idx := range table.GetIndices() {
			if w, ok := idx.Tree.(pageWriter); ok {
				writers[w.Path()] = w
			}
		}
	}
	return writers
}

// Suppress the unused-import lint for sync (kept available for future
// evolution of NTA counters).
var _ sync.Mutex

// writeNTABegin emits an EntryNTABegin to the WAL and force-syncs it so
// the before-images are durable before any in-memory page mutation
// completes. Returns the LSN, which is also the NTA id.
func (se *StorageEngine) writeNTABegin(kind uint8, pages []NTAPage) (uint64, error) {
	if se == nil || se.WAL == nil {
		return 0, nil
	}
	for _, p := range pages {
		if len(p.PreImage) != pagestore.PageSize {
			return 0, fmt.Errorf("storage: NTA page %s/%d before-image must be %d bytes, got %d", p.Path, p.PageID, pagestore.PageSize, len(p.PreImage))
		}
	}
	payload := serializeNTABeginPayload(kind, pages)

	lsn := se.lsnTracker.Next()
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryNTABegin
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // bounded by pagestore.PageSize * pages
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)

	err := se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("storage: write NTA begin: %w", err)
	}
	if err := se.WAL.Sync(); err != nil {
		return 0, fmt.Errorf("storage: sync NTA begin: %w", err)
	}
	return lsn, nil
}

// writeNTACommit emits an EntryNTACommit referencing the Begin's LSN.
// Force-synced so the commit is durable before the structural op is
// considered atomic.
func (se *StorageEngine) writeNTACommit(ntaID uint64) error {
	if se == nil || se.WAL == nil || ntaID == 0 {
		return nil
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, ntaID)

	lsn := se.lsnTracker.Next()
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryNTACommit
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // fixed 8 bytes
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)

	err := se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return fmt.Errorf("storage: write NTA commit: %w", err)
	}
	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("storage: sync NTA commit: %w", err)
	}
	return nil
}

// Payload layout for EntryNTABegin:
//
//	[kind:1][pageCount:2][page1][page2]...[pageN]
//
// Each page: [pathLen:2][path:N][pageID:8][pageBytes:PageSize]
func serializeNTABeginPayload(kind uint8, pages []NTAPage) []byte {
	size := 1 + 2
	for _, p := range pages {
		size += 2 + len(p.Path) + 8 + pagestore.PageSize
	}
	buf := make([]byte, 0, size)
	buf = append(buf, kind)
	var u16 [2]byte
	var u64 [8]byte
	binary.LittleEndian.PutUint16(u16[:], uint16(len(pages))) //nolint:gosec // limited by structural fan-out
	buf = append(buf, u16[:]...)
	for _, p := range pages {
		binary.LittleEndian.PutUint16(u16[:], uint16(len(p.Path))) //nolint:gosec // path length bounded by filesystem
		buf = append(buf, u16[:]...)
		buf = append(buf, []byte(p.Path)...)
		binary.LittleEndian.PutUint64(u64[:], uint64(p.PageID))
		buf = append(buf, u64[:]...)
		buf = append(buf, p.PreImage...)
	}
	return buf
}

func deserializeNTABeginPayload(payload []byte) (kind uint8, pages []NTAPage, err error) {
	if len(payload) < 3 {
		return 0, nil, fmt.Errorf("nta begin payload too short")
	}
	kind = payload[0]
	count := int(binary.LittleEndian.Uint16(payload[1:3]))
	off := 3
	pages = make([]NTAPage, 0, count)
	for i := 0; i < count; i++ {
		if off+2 > len(payload) {
			return 0, nil, fmt.Errorf("nta begin truncated at page %d", i)
		}
		pathLen := int(binary.LittleEndian.Uint16(payload[off : off+2]))
		off += 2
		if off+pathLen+8+pagestore.PageSize > len(payload) {
			return 0, nil, fmt.Errorf("nta begin truncated body at page %d", i)
		}
		p := NTAPage{
			Path: string(payload[off : off+pathLen]),
		}
		off += pathLen
		p.PageID = pagestore.PageID(binary.LittleEndian.Uint64(payload[off : off+8]))
		off += 8
		p.PreImage = make([]byte, pagestore.PageSize)
		copy(p.PreImage, payload[off:off+pagestore.PageSize])
		off += pagestore.PageSize
		pages = append(pages, p)
	}
	return kind, pages, nil
}

func deserializeNTACommitPayload(payload []byte) (ntaID uint64, err error) {
	if len(payload) < 8 {
		return 0, fmt.Errorf("nta commit payload too short")
	}
	return binary.LittleEndian.Uint64(payload[:8]), nil
}
