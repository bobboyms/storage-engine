package storage

import (
	"encoding/binary"
	"fmt"

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
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(path)))
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

	payload, err := serializePageRedoPayload(path, pageID, page)
	if err != nil {
		return err
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = hdr.PageLSN
	entry.Header.PayloadLen = uint32(len(payload))
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
