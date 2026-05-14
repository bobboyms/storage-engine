package main

import (
	"context"
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXAMPLE: Streaming Iterator + GetBytes

Demonstrates the streaming read API introduced as part of the
iterator/raw-bytes refactor:

  - StorageEngine.NewIterator(context.Background(), ...): one row at a time, constant memory,
    Close() cancels mid-scan.
  - StorageEngine.GetBytes(context.Background(), ...): raw heap bytes (no codec round-trip).

The legacy Get / Scan / RangeScan methods still work — they are now thin
deprecated wrappers around the new API for backwards compatibility.
*/
func main() {
	cleanup()
	defer cleanup()

	heap, err := storage.NewHeapForTable(storage.HeapFormatV2, "iterator.heap")
	if err != nil {
		panic(err)
	}
	meta := storage.NewTableMenager()
	if err := meta.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, heap); err != nil {
		panic(err)
	}
	ww, err := wal.NewWALWriter("iterator.wal", wal.DefaultOptions())
	if err != nil {
		panic(err)
	}
	se, err := storage.NewStorageEngine(meta, ww)
	if err != nil {
		panic(err)
	}
	defer se.Close()

	for i := 1; i <= 5; i++ {
		doc := fmt.Sprintf(`{"id":%d,"name":"user-%d"}`, i, i)
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			panic(err)
		}
	}

	fmt.Println("== Streaming full scan via NewIterator ==")
	it, err := se.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		panic(err)
	}
	for it.Next() {
		fmt.Printf("  key=%v lsn=%d raw_bytes=%d\n", it.Key(), it.LSN(), len(it.Value()))
	}
	if err := it.Err(); err != nil {
		panic(err)
	}
	_ = it.Close()

	fmt.Println("== Range [2,4] via NewIterator ==")
	rit, err := se.NewIterator(context.Background(), "users", "id", storage.IterOptions{
		Lower: types.IntKey(2),
		Upper: types.IntKey(4),
	})
	if err != nil {
		panic(err)
	}
	for rit.Next() {
		fmt.Printf("  key=%v\n", rit.Key())
	}
	_ = rit.Close()

	fmt.Println("== Raw bytes via GetBytes ==")
	raw, found, err := se.GetBytes(context.Background(), "users", "id", types.IntKey(3))
	if err != nil {
		panic(err)
	}
	fmt.Printf("  found=%v len=%d\n", found, len(raw))
}

func cleanup() {
	_ = os.Remove("iterator.heap")
	_ = os.Remove("iterator.wal")
	_ = os.Remove("id.btree.v2")
}
