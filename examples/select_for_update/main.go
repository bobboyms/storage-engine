// Demonstrates GetForUpdate (the engine's SELECT ... FOR UPDATE) to
// prevent write skew, using the classic on-call scheduling invariant:
// "at least one doctor must remain on call".
//
// The engine's maximum isolation is snapshot isolation, which allows
// write skew: two transactions read the same rows, each sees the
// invariant hold, and each updates a different row — together breaking
// the rule with no error. A plain Get would not stop this.
//
// GetForUpdate fixes it: it locks the rows the decision depends on and
// reads their latest committed value. The second transaction blocks
// until the first commits, then sees the updated state and refuses to
// violate the invariant.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// docCodec decodes the engine's stored bytes (BSON on disk) back to JSON
// text so the demo can inspect fields.
var docCodec = bsoncodec.New()

// isOnCall reports whether the stored document marks the doctor on call.
func isOnCall(raw []byte) bool {
	text, err := docCodec.DecodeToText(raw)
	if err != nil {
		return false
	}
	return strings.Contains(text, `"on_call":true`)
}

func main() {
	tmpDir, err := os.MkdirTemp("", "select_for_update_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("Database: %s\n\n", tmpDir)

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "shifts.heap"))
	if err != nil {
		log.Fatalf("NewHeapForTable: %v", err)
	}
	meta := storage.NewTableMenager()
	if err := meta.NewTable("shifts", []storage.Index{
		{Name: "id", Type: storage.TypeInt, Primary: true},
	}, 4, hm); err != nil {
		log.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(tmpDir, "shifts.wal"), wal.DefaultOptions())
	if err != nil {
		log.Fatalf("NewWALWriter: %v", err)
	}
	se, err := storage.NewProductionStorageEngine(meta, ww)
	if err != nil {
		log.Fatalf("NewProductionStorageEngine: %v", err)
	}
	defer se.Close()

	ctx := context.Background()

	// Two doctors, both on call.
	for _, id := range []int{1, 2} {
		if err := se.Put(ctx, "shifts", "id", types.IntKey(id),
			fmt.Sprintf(`{"id":%d,"on_call":true}`, id)); err != nil {
			log.Fatalf("seed %d: %v", id, err)
		}
	}
	fmt.Println("initial: doctor 1 on call, doctor 2 on call")

	// goOffCall takes both rows FOR UPDATE, and only removes `self` from
	// the on-call list if at least one other doctor remains on call.
	goOffCall := func(label string, self int) (bool, error) {
		tx := se.BeginWriteTransaction()
		onCall := 0
		for _, id := range []int{1, 2} {
			raw, found, err := tx.GetForUpdate(ctx, "shifts", "id", types.IntKey(id))
			if err != nil {
				_ = tx.Rollback(ctx)
				return false, err
			}
			if found && isOnCall(raw) {
				onCall++
			}
		}
		fmt.Printf("%s: sees %d doctor(s) on call\n", label, onCall)
		if onCall <= 1 {
			_ = tx.Rollback(ctx)
			return false, nil // refuse: would leave nobody on call
		}
		if err := tx.Put(ctx, "shifts", "id", types.IntKey(self),
			`{"id":`+strconv.Itoa(self)+`,"on_call":false}`); err != nil {
			_ = tx.Rollback(ctx)
			return false, err
		}
		return true, tx.Commit(ctx)
	}

	// Doctor 1 goes off call; this transaction holds the row locks.
	changed1, err := goOffCall("doctor 1", 1)
	if err != nil {
		log.Fatalf("doctor 1: %v", err)
	}
	fmt.Printf("doctor 1 went off call: %v\n", changed1)

	// Doctor 2 tries concurrently. With GetForUpdate it observes the
	// updated state and must refuse. (Run in a goroutine to show it is a
	// normal concurrent transaction; here doctor 1 already committed, so
	// doctor 2 proceeds immediately and sees only itself on call.)
	var (
		wg       sync.WaitGroup
		changed2 bool
		err2     error
	)
	wg.Go(func() {
		changed2, err2 = goOffCall("doctor 2", 2)
	})
	wg.Wait()
	if err2 != nil && !errors.Is(err2, storage.ErrSerializationConflict) {
		log.Fatalf("doctor 2: %v", err2)
	}
	fmt.Printf("doctor 2 went off call: %v\n", changed2)

	// Final state: exactly one doctor still on call.
	onCall := 0
	for _, id := range []int{1, 2} {
		raw, found, err := se.GetBytes(ctx, "shifts", "id", types.IntKey(id))
		if err != nil {
			log.Fatalf("final read %d: %v", id, err)
		}
		if found && isOnCall(raw) {
			onCall++
		}
	}
	fmt.Printf("\nfinal: %d doctor(s) on call (invariant held: %v)\n", onCall, onCall >= 1)
}
