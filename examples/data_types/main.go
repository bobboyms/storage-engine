package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXAMPLE: Supported Data Types

Demonstrates every comparable type the engine supports via the new
streaming API:

 1. TypeInt     - int64 (IntKey)
 2. TypeVarchar - strings (VarcharKey)
 3. TypeFloat   - float64 (FloatKey)
 4. TypeBoolean - bool (BoolKey)
 5. TypeDate    - time.Time (DateKey)

Reads use NewIterator (range) and GetBytes (point lookup). Documents are
stored as bytes; this example decodes them through the default BSON codec
so the printed output stays readable.
*/

var docCodec = bsoncodec.New()

func decodeDoc(raw []byte) string {
	if raw == nil {
		return ""
	}
	text, err := docCodec.DecodeToText(raw)
	if err != nil {
		return string(raw)
	}
	return text
}

func fetchDoc(engine *storage.StorageEngine, table, idx string, key types.Comparable) (string, bool) {
	raw, found, err := engine.GetBytes(table, idx, key)
	if err != nil || !found {
		return "", false
	}
	return decodeDoc(raw), true
}

func collectRange(engine *storage.StorageEngine, table, idx string, lo, hi types.Comparable) []string {
	it, err := engine.NewIterator(table, idx, storage.IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		fmt.Printf("iterator error: %v\n", err)
		return nil
	}
	defer it.Close()
	var out []string
	for it.Next() {
		out = append(out, decodeDoc(it.Value()))
	}
	return out
}

func main() {
	walPath := "data.wal"
	heapPath := "data.heap"

	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	engine := setupEngine(heapPath, walPath)
	defer engine.Close()

	// 1. TypeInt
	fmt.Println("=== TypeInt ===")
	for i := int64(1); i <= 5; i++ {
		doc := fmt.Sprintf(`{"id": %d, "type": "integer", "value": %d}`, i, i*100)
		engine.Put("int_table", "id", types.IntKey(i), doc)
	}
	if doc, ok := fetchDoc(engine, "int_table", "id", types.IntKey(3)); ok {
		fmt.Printf("  IntKey(3): %s\n", doc)
	}
	fmt.Printf("  Range [2-4]: %v\n", collectRange(engine, "int_table", "id", types.IntKey(2), types.IntKey(4)))

	// 2. TypeVarchar
	fmt.Println("\n=== TypeVarchar ===")
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	for _, name := range names {
		doc := fmt.Sprintf(`{"name": "%s", "type": "string"}`, name)
		engine.Put("string_table", "name", types.VarcharKey(name), doc)
	}
	if doc, ok := fetchDoc(engine, "string_table", "name", types.VarcharKey("Charlie")); ok {
		fmt.Printf("  VarcharKey(\"Charlie\"): %s\n", doc)
	}
	fmt.Printf("  Range [Bob-Diana]: %v\n", collectRange(engine, "string_table", "name", types.VarcharKey("Bob"), types.VarcharKey("Diana")))

	// 3. TypeFloat
	fmt.Println("\n=== TypeFloat ===")
	prices := []float64{1.99, 5.50, 10.00, 25.75, 99.99}
	for _, price := range prices {
		doc := fmt.Sprintf(`{"price": %.2f, "type": "float"}`, price)
		engine.Put("float_table", "price", types.FloatKey(price), doc)
	}
	if doc, ok := fetchDoc(engine, "float_table", "price", types.FloatKey(10.00)); ok {
		fmt.Printf("  FloatKey(10.00): %s\n", doc)
	}
	// "> 5.00" expressed as a streamed iteration starting at 5.00 and
	// filtering the cursor key. The engine itself only understands range
	// bounds; consumer-side predicates live up here now.
	var aboveFive []string
	it, _ := engine.NewIterator("float_table", "price", storage.IterOptions{Lower: types.FloatKey(5.00)})
	for it.Next() {
		if it.Key().Compare(types.FloatKey(5.00)) <= 0 {
			continue
		}
		aboveFive = append(aboveFive, decodeDoc(it.Value()))
	}
	_ = it.Close()
	fmt.Printf("  Prices > 5.00: %v\n", aboveFive)

	// 4. TypeBoolean
	fmt.Println("\n=== TypeBoolean ===")
	engine.Put("bool_table", "active", types.BoolKey(false), `{"user": "inactive1", "active": false}`)
	engine.Put("bool_table", "active", types.BoolKey(true), `{"user": "active1", "active": true}`)
	if doc, ok := fetchDoc(engine, "bool_table", "active", types.BoolKey(true)); ok {
		fmt.Printf("  BoolKey(true):  %s\n", doc)
	}
	if doc, ok := fetchDoc(engine, "bool_table", "active", types.BoolKey(false)); ok {
		fmt.Printf("  BoolKey(false): %s\n", doc)
	}

	// 5. TypeDate
	fmt.Println("\n=== TypeDate ===")
	now := time.Now()
	dates := []time.Time{
		now.AddDate(0, 0, -7),
		now.AddDate(0, 0, -3),
		now,
		now.AddDate(0, 0, 3),
		now.AddDate(0, 0, 7),
	}
	for _, date := range dates {
		doc := fmt.Sprintf(`{"date": "%s", "type": "date"}`, date.Format("2006-01-02"))
		engine.Put("date_table", "date", types.DateKey(date), doc)
	}
	if doc, ok := fetchDoc(engine, "date_table", "date", types.DateKey(now)); ok {
		fmt.Printf("  DateKey(today): %s\n", doc)
	}
	var future []string
	dit, _ := engine.NewIterator("date_table", "date", storage.IterOptions{Lower: types.DateKey(now)})
	for dit.Next() {
		if dit.Key().Compare(types.DateKey(now)) <= 0 {
			continue
		}
		future = append(future, decodeDoc(dit.Value()))
	}
	_ = dit.Close()
	fmt.Printf("  Future dates: %v\n", future)

	// 6. Comparison demo
	fmt.Println("\n=== Comparable.Compare ===")
	fmt.Printf("IntKey(5) vs IntKey(10): %d\n", types.IntKey(5).Compare(types.IntKey(10)))
	fmt.Printf("VarcharKey(\"abc\") vs VarcharKey(\"xyz\"): %d\n", types.VarcharKey("abc").Compare(types.VarcharKey("xyz")))
	fmt.Printf("BoolKey(false) vs BoolKey(true): %d\n", types.BoolKey(false).Compare(types.BoolKey(true)))
	yesterday := now.AddDate(0, 0, -1)
	tomorrow := now.AddDate(0, 0, 1)
	fmt.Printf("DateKey(yesterday) vs DateKey(tomorrow): %d\n", types.DateKey(yesterday).Compare(types.DateKey(tomorrow)))
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		os.Exit(1)
	}

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("int_table", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)
	tableMgr.NewTable("string_table", []storage.Index{{Name: "name", Primary: true, Type: storage.TypeVarchar}}, 3, hm)
	tableMgr.NewTable("float_table", []storage.Index{{Name: "price", Primary: true, Type: storage.TypeFloat}}, 3, hm)
	tableMgr.NewTable("bool_table", []storage.Index{{Name: "active", Primary: true, Type: storage.TypeBoolean}}, 3, hm)
	tableMgr.NewTable("date_table", []storage.Index{{Name: "date", Primary: true, Type: storage.TypeDate}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	engine, _ := storage.NewStorageEngine(tableMgr, walWriter)
	return engine
}

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
	os.RemoveAll("checkpoints")
}
