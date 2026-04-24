package storage_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// newCipherFor gera um AES-GCM com chave aleatória pra usar em testes.
func newCipherFor(t testing.TB) crypto.Cipher {
	t.Helper()
	key := make([]byte, crypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}
	c, err := crypto.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestBTreeV2_Integration_BasicCRUD prova que uma tabela criada com
// índice BTreeFormatV2 funciona drop-in no engine. O engine usa
// `index.Tree.Get/Upsert/Replace` — as mesmas chamadas que antes
// (v1), agora resolvidas via interface btree.Tree.
func TestBTreeV2_Integration_BasicCRUD(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "idx_id.btree.v2")

	// Heap v1 (default)
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		t.Fatal(err)
	}

	// Índice v2 (opt-in) pra primary key TypeInt
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	if err != nil {
		t.Fatal(err)
	}

	tableMgr := storage.NewTableMenager()
	err = tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 3, hm)
	if err != nil {
		t.Fatal(err)
	}

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatal(err)
	}
	defer se.Close()

	// INSERT
	if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"nome":"alice"}`); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := se.Put("users", "id", types.IntKey(2), `{"id":2,"nome":"bob"}`); err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	// GET — exercita index.Tree.Get (v2)
	doc, found, err := se.Get("users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("Get 1: found=%v err=%v", found, err)
	}
	if doc == "" {
		t.Fatal("doc 1 vazio")
	}

	// UPDATE — exercita index.Tree.Upsert (v2)
	if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"nome":"alice-updated"}`); err != nil {
		t.Fatalf("Update 1: %v", err)
	}
	doc, _, _ = se.Get("users", "id", types.IntKey(1))
	if doc != `{"id":1,"nome":"alice-updated"}` {
		t.Fatalf("update não refletiu: %q", doc)
	}
}

// TestEngine_AutoScanWALForMaxLSN prova o fix: reopen de um engine com
// WAL já populado deve auto-sincronizar o lsnTracker. Sem isso, snapshots
// novos (SnapshotLSN=0) não enxergam records persistidos (CreateLSN>=1).
//
// Teste usa heap v1 + btree v2 — combinação onde o heap persiste em
// claro (tree v2 é o único persistente além do WAL). Prova que o fix
// é do engine, não específico do v2.
func TestEngine_AutoScanWALForMaxLSN(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	// Sessão 1: insere com LSN progressivo
	{
		hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
		idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
		tm := storage.NewTableMenager()
		tm.NewTable("t", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
		}, 3, hm)
		ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
		se, err := storage.NewStorageEngine(tm, ww)
		if err != nil {
			ww.Close()
			t.Fatal(err)
		}
		for i := int64(1); i <= 5; i++ {
			se.Put("t", "id", types.IntKey(i), fmt.Sprintf(`{"id":%d}`, i))
		}
		se.Close()
	}

	// Sessão 2: reabre — auto-scan deve advance lsnTracker para 5
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	idxTree2, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)
	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewStorageEngine(tm2, ww2)
	if err != nil {
		ww2.Close()
		t.Fatal(err)
	}
	defer se2.Close()

	// SEM chamar Recover: todos os 5 registros devem ser visíveis
	for i := int64(1); i <= 5; i++ {
		doc, found, err := se2.Get("t", "id", types.IntKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !found {
			t.Fatalf("key %d invisível após reopen (LSN tracker não sincronizou)", i)
		}
		want := fmt.Sprintf(`{"id":%d}`, i)
		if doc != want {
			t.Fatalf("key %d: esperado %q, recebi %q", i, want, doc)
		}
	}
}

// TestBTreeV2_Integration_Varchar prova que um índice de VARCHAR em v2
// (layout variable-key) funciona drop-in no engine. VarcharKey entra
// pelo path separado NewBTreeV2Varchar, mas a interface btree.Tree é
// idêntica — o engine nem percebe.
func TestBTreeV2_Integration_Varchar(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "idx_email.btree.v2")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	idxTree, err := storage.NewBTreeForIndex(
		storage.BTreeFormatV2, true,
		storage.TypeVarchar, // antes retornava erro; agora funciona
		btreePath, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	tm := storage.NewTableMenager()
	err = tm.NewTable("contacts", []storage.Index{
		{Name: "email", Primary: true, Type: storage.TypeVarchar, Tree: idxTree},
	}, 3, hm)
	if err != nil {
		t.Fatal(err)
	}

	ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		ww.Close()
		t.Fatal(err)
	}
	defer se.Close()

	// Insere algumas entries com emails (variáveis em tamanho)
	rows := map[string]string{
		"alice@example.com":                        `{"email":"alice@example.com","nome":"Alice"}`,
		"bob@b.co":                                 `{"email":"bob@b.co","nome":"Bob"}`,
		"carlos+longsufix+lista@empresa.com.br":    `{"email":"carlos+longsufix+lista@empresa.com.br","nome":"Carlos"}`,
	}
	for email, doc := range rows {
		if err := se.Put("contacts", "email", types.VarcharKey(email), doc); err != nil {
			t.Fatalf("Put %q: %v", email, err)
		}
	}

	// Get de cada
	for email, wantDoc := range rows {
		doc, found, err := se.Get("contacts", "email", types.VarcharKey(email))
		if err != nil || !found {
			t.Fatalf("Get %q: found=%v err=%v", email, found, err)
		}
		if doc != wantDoc {
			t.Fatalf("Get %q: doc divergente", email)
		}
	}

	// Update (exercita Upsert em VarcharKey)
	if err := se.Put("contacts", "email", types.VarcharKey("bob@b.co"),
		`{"email":"bob@b.co","nome":"Bob Updated"}`); err != nil {
		t.Fatal(err)
	}
	doc, _, _ := se.Get("contacts", "email", types.VarcharKey("bob@b.co"))
	if doc != `{"email":"bob@b.co","nome":"Bob Updated"}` {
		t.Fatalf("Update não refletiu: %q", doc)
	}
}

// TestBTreeV2_Integration_MVCC: chain walks via BTreeV2 + HeapV1 funcionam.
func TestBTreeV2_Integration_MVCC(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "idx_id.btree.v2")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("mvcc_btv2", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatal(err)
	}
	defer se.Close()

	// v1 da row
	se.Put("mvcc_btv2", "id", types.IntKey(1), `{"id":1,"v":1}`)
	txA := se.BeginRead()

	// update — cria v2 no heap, atualiza BTreeV2 via Upsert (chainada por PrevRecordID)
	se.Put("mvcc_btv2", "id", types.IntKey(1), `{"id":1,"v":2}`)
	txB := se.BeginRead()

	se.Put("mvcc_btv2", "id", types.IntKey(1), `{"id":1,"v":3}`)

	// txA vê v1 (snapshot antes dos updates)
	doc, found, _ := txA.Get("mvcc_btv2", "id", types.IntKey(1))
	if !found || doc != `{"id":1,"v":1}` {
		t.Errorf("txA esperava v1, recebi found=%v doc=%q", found, doc)
	}

	// txB vê v2
	doc, found, _ = txB.Get("mvcc_btv2", "id", types.IntKey(1))
	if !found || doc != `{"id":1,"v":2}` {
		t.Errorf("txB esperava v2, recebi found=%v doc=%q", found, doc)
	}

	// Engine novo snapshot vê v3
	doc, found, _ = se.Get("mvcc_btv2", "id", types.IntKey(1))
	if !found || doc != `{"id":1,"v":3}` {
		t.Errorf("engine esperava v3, recebi found=%v doc=%q", found, doc)
	}
}

// TestBTreeV2_Integration_ReopenWithTDE prova que o índice persiste
// com cifra através do ciclo fechar/reabrir — exatamente o buraco
// do .chk em claro que fase 5 fecha.
func TestBTreeV2_Integration_ReopenWithTDE(t *testing.T) {
	// Nota: usa cipher só no B+ tree. O heap v1 continua em claro
	// (pra provar que o cipher do btree é independente).
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "idx_id.btree.v2")

	// Cifra aleatória de 32 bytes (ver pkg/crypto)
	cipher := newCipherFor(t)

	// Cria tabela com índice v2 cifrado
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, cipher)
	if err != nil {
		t.Fatal(err)
	}

	tm := storage.NewTableMenager()
	tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 3, hm)

	ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		ww.Close()
		t.Fatal(err)
	}

	if err := se.Put("t", "id", types.IntKey(42), `{"id":42,"payload":"confidencial"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reabre com a MESMA chave — dados voltam
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	idxTree2, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, cipher)
	if err != nil {
		t.Fatal(err)
	}

	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)

	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewStorageEngine(tm2, ww2)
	if err != nil {
		ww2.Close()
		t.Fatal(err)
	}
	defer se2.Close()

	// Não precisamos mais de Recover manual — NewStorageEngine agora faz
	// auto-scan do WAL pra avançar o lsnTracker. Records persistidos ficam
	// imediatamente visíveis pra novos snapshots.

	doc, found, _ := se2.Get("t", "id", types.IntKey(42))
	if !found {
		t.Fatal("key 42 sumiu após reopen")
	}
	// Recover pode gerar nova versão via WAL replay; o doc deve ser o mesmo
	if doc != `{"id":42,"payload":"confidencial"}` {
		t.Fatalf("doc corrompido: %q", doc)
	}
}
