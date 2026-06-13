package storage

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

const pageRedoPathPrefixSize = 2

type redoHookable interface {
	SetBeforeFlushHook(func(pageID pagestore.PageID, page *pagestore.Page) error)
	DirtyPages() []pagestore.DirtyPageInfo
}

type pageRedoTarget interface {
	ApplyPageRedo(pageID pagestore.PageID, page *pagestore.Page, lsn uint64) (bool, error)
}

func serializePageRedoPayload(path string, pageID pagestore.PageID, page *pagestore.Page) ([]byte, error) {
	if len(path) > 0xFFFF {
		return nil, fmt.Errorf("storage: redo path too long: %d", len(path))
	}
	payload := make([]byte, pageRedoPathPrefixSize+len(path)+8+pagestore.PageSize)
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(path))) //nolint:gosec // path length checked above against 0xFFFF
	copy(payload[2:2+len(path)], path)
	offset := 2 + len(path)
	binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(pageID))
	copy(payload[offset+8:], page[:])
	return payload, nil
}

func deserializePageRedoPayload(payload []byte) (string, pagestore.PageID, *pagestore.Page, error) {
	if len(payload) < pageRedoPathPrefixSize+8+pagestore.PageSize {
		return "", 0, nil, fmt.Errorf("storage: redo payload too short: %d", len(payload))
	}
	pathLen := int(binary.LittleEndian.Uint16(payload[0:2]))
	if len(payload) < 2+pathLen+8+pagestore.PageSize {
		return "", 0, nil, fmt.Errorf("storage: redo payload truncated")
	}
	path := string(payload[2 : 2+pathLen])
	offset := 2 + pathLen
	pageID := pagestore.PageID(binary.LittleEndian.Uint64(payload[offset : offset+8]))
	var page pagestore.Page
	copy(page[:], payload[offset+8:offset+8+pagestore.PageSize])
	return path, pageID, &page, nil
}

func (se *StorageEngine) registerPageRedoHooks() {
	if se == nil {
		return
	}
	seenHeaps := make(map[*heapv2.HeapV2]struct{})
	seenTrees := make(map[*btreev2.BTreeV2]struct{})

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		if heapV2, ok := table.Heap.(*heapv2.HeapV2); ok {
			if _, done := seenHeaps[heapV2]; !done {
				heapPath := heapV2.Path()
				heapV2.SetBeforeFlushHook(func(pageID pagestore.PageID, page *pagestore.Page) error {
					return se.writePageRedoRecord(heapPath, pageID, page)
				})
				seenHeaps[heapV2] = struct{}{}
			}
		}

		for _, idx := range table.GetIndices() {
			treeV2, ok := idx.Tree.(*btreev2.BTreeV2)
			if !ok {
				continue
			}
			if _, done := seenTrees[treeV2]; done {
				continue
			}
			treePath := treeV2.Path()
			treeV2.SetBeforeFlushHook(func(pageID pagestore.PageID, page *pagestore.Page) error {
				return se.writePageRedoRecord(treePath, pageID, page)
			})
			treeV2.SetStructuralLogger(btreeStructuralLogger{se: se})
			seenTrees[treeV2] = struct{}{}
		}
	}
}

func (se *StorageEngine) writePageRedoRecord(path string, pageID pagestore.PageID, page *pagestore.Page) error {
	if se == nil || se.WAL == nil || page == nil {
		return nil
	}

	hdr, err := page.GetHeader()
	if err != nil {
		return err
	}
	if hdr.PageLSN == 0 {
		return nil
	}
	// A page can only be dirtied by an operation that allocated an LSN, so a
	// PageLSN beyond the engine's current LSN is corrupt metadata (e.g. the
	// MaxUint64 horizon a pre-clamp vacuum stamped). Writing it verbatim would
	// poison the WAL: the next open adopts the log's max LSN as its counter.
	entryLSN := hdr.PageLSN
	if current := se.lsnTracker.Current(); entryLSN > current {
		entryLSN = current
	}

	payload, err := serializePageRedoPayload(path, pageID, page)
	if err != nil {
		return err
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = entryLSN
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by PageSize
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)

	if err := se.WAL.WriteEntry(entry); err != nil {
		wal.ReleaseEntry(entry)
		return fmt.Errorf("storage: write page redo: %w", err)
	}
	wal.ReleaseEntry(entry)
	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("storage: sync page redo: %w", err)
	}
	return nil
}

// resolvePageRedoTarget finds the target a WAL-recorded page path refers to.
// Recovery normally runs in the directory that wrote the log, so the full
// path matches exactly; after the directory was copied or restored somewhere
// else the prefix changed, and we fall back to the basename when exactly one
// registered target carries it (a database directory holds each file name
// once). Ambiguity returns nil — silently skipping is safe, writing to the
// wrong file is not.
func resolvePageRedoTarget(targets map[string]pageRedoTarget, path string) pageRedoTarget {
	if target, ok := targets[path]; ok {
		return target
	}
	base := filepath.Base(path)
	var match pageRedoTarget
	for p, t := range targets {
		if filepath.Base(p) != base {
			continue
		}
		if match != nil {
			return nil
		}
		match = t
	}
	return match
}

// recordedFlushMatches reports whether a registered file path appears in the
// set of WAL-recorded flush paths, tolerating a relocated directory the same
// way resolvePageRedoTarget does. Here ambiguity may return true: the only
// consequence is a conservative index rebuild.
func recordedFlushMatches(recorded map[string]struct{}, path string) bool {
	if _, ok := recorded[path]; ok {
		return true
	}
	base := filepath.Base(path)
	for p := range recorded {
		if filepath.Base(p) == base {
			return true
		}
	}
	return false
}

func (se *StorageEngine) pageRedoTargets() map[string]pageRedoTarget {
	targets := make(map[string]pageRedoTarget)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}
		if heapV2, ok := table.Heap.(*heapv2.HeapV2); ok {
			targets[heapV2.Path()] = heapV2
		}
		for _, idx := range table.GetIndices() {
			if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
				targets[treeV2.Path()] = treeV2
			}
		}
	}
	return targets
}
