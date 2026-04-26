package storage_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	enginecrypto "github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func tdeMasterKey(t testing.TB) []byte {
	t.Helper()
	key := make([]byte, enginecrypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}
	return key
}

func tdeCiphers(t testing.TB, keyStorePath string, masterKey []byte) (enginecrypto.Cipher, enginecrypto.Cipher, enginecrypto.Cipher) {
	t.Helper()
	ks, err := enginecrypto.NewKeyStore(keyStorePath, masterKey)
	if err != nil {
		t.Fatalf("NewKeyStore: %v", err)
	}
	heapCipher, err := ks.GetOrCreateDEK("heap:accounts")
	if err != nil {
		t.Fatalf("heap DEK: %v", err)
	}
	indexCipher, err := ks.GetOrCreateDEK("btree:accounts:email")
	if err != nil {
		t.Fatalf("index DEK: %v", err)
	}
	walCipher, err := ks.GetOrCreateDEK("wal:accounts")
	if err != nil {
		t.Fatalf("wal DEK: %v", err)
	}
	return heapCipher, indexCipher, walCipher
}

func openTDEAccountsEngine(t testing.TB, heapPath, walPath, keyStorePath string, masterKey []byte) (*storage.StorageEngine, string) {
	t.Helper()

	heapCipher, indexCipher, walCipher := tdeCiphers(t, keyStorePath, masterKey)

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, heapCipher)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}

	tm := storage.NewEncryptedTableMenager(indexCipher)
	if err := tm.NewTable("accounts", []storage.Index{
		{Name: "email", Primary: true, Type: storage.TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	opts := wal.DefaultOptions()
	opts.Cipher = walCipher
	ww, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}

	se, err := storage.NewProductionStorageEngine(tm, ww)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("NewProductionStorageEngine: %v", err)
	}

	indexPath := filepath.Join(filepath.Dir(heapPath), filepath.Base(heapPath)+".accounts.email.btree.v2")
	return se, indexPath
}

func assertFileDoesNotContain(t testing.TB, path string, plaintext []byte) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if bytes.Contains(raw, plaintext) {
		t.Fatalf("%s contém plaintext %q em disco", path, plaintext)
	}
}

func TestTDEProduction_EncryptsHeapAutoIndexAndWALAndReopens(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "accounts.heap")
	walPath := filepath.Join(dir, "accounts.wal")
	keyStorePath := filepath.Join(dir, "keys.json")
	masterKey := tdeMasterKey(t)

	se, indexPath := openTDEAccountsEngine(t, heapPath, walPath, keyStorePath, masterKey)

	secretKey := "prod-tde-secret-card-4111@example.com"
	doc := `{"email":"prod-tde-secret-card-4111@example.com","balance":1000,"note":"prod-tde-secret-card-4111"}`
	updated := `{"email":"prod-tde-secret-card-4111@example.com","balance":2000,"note":"prod-tde-secret-card-4111-updated"}`

	if err := se.Put("accounts", "email", types.VarcharKey(secretKey), doc); err != nil {
		t.Fatalf("Put initial: %v", err)
	}
	if err := se.Put("accounts", "email", types.VarcharKey(secretKey), updated); err != nil {
		t.Fatalf("Put update: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	secretBytes := []byte("prod-tde-secret-card-4111")
	assertFileDoesNotContain(t, heapPath, secretBytes)
	assertFileDoesNotContain(t, indexPath, []byte(secretKey))
	assertFileDoesNotContain(t, walPath, secretBytes)

	info, err := os.Stat(keyStorePath)
	if err != nil {
		t.Fatalf("Stat keystore: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("keystore must ser 0600, got %v", got)
	}

	se2, _ := openTDEAccountsEngine(t, heapPath, walPath, keyStorePath, masterKey)
	defer se2.Close()

	got, found, err := se2.Get("accounts", "email", types.VarcharKey(secretKey))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found || got != updated {
		t.Fatalf("after reopen: found=%v got=%q", found, got)
	}
}

func TestTDEProduction_WrongMasterKeyCannotOpenExistingDEKs(t *testing.T) {
	dir := t.TempDir()
	keyStorePath := filepath.Join(dir, "keys.json")

	_, _, _ = tdeCiphers(t, keyStorePath, tdeMasterKey(t))

	wrongKS, err := enginecrypto.NewKeyStore(keyStorePath, tdeMasterKey(t))
	if err != nil {
		t.Fatalf("NewKeyStore wrong master: %v", err)
	}
	if _, err := wrongKS.GetOrCreateDEK("wal:accounts"); err == nil {
		t.Fatal("wrong master key decifrou DEK existsnte; expected erro")
	}
}

func TestTDEProduction_EncryptedWALRecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "accounts.heap")
	walPath := filepath.Join(dir, "accounts.wal")
	keyStorePath := filepath.Join(dir, "keys.json")
	masterKey := tdeMasterKey(t)

	se, _ := openTDEAccountsEngine(t, heapPath, walPath, keyStorePath, masterKey)

	secretKey := "crash-tde-secret@example.com"
	want := `{"email":"crash-tde-secret@example.com","balance":777,"note":"wal-only-secret"}`
	if err := se.Put("accounts", "email", types.VarcharKey(secretKey), want); err != nil {
		t.Fatalf("Put before crash: %v", err)
	}

	// Simula crash: fecha apenas o WAL para persistir a page corrente.
	// Not fecha o engine, heap ou BTree, evitando flush limpo do estado de dados.
	if err := se.WAL.Close(); err != nil {
		t.Fatalf("close WAL before simulated crash: %v", err)
	}

	assertFileDoesNotContain(t, walPath, []byte("wal-only-secret"))

	recovered, _ := openTDEAccountsEngine(t, heapPath, walPath, keyStorePath, masterKey)
	defer recovered.Close()

	got, found, err := recovered.Get("accounts", "email", types.VarcharKey(secretKey))
	if err != nil {
		t.Fatalf("Get after encrypted WAL recovery: %v", err)
	}
	if !found || got != want {
		t.Fatalf("encrypted WAL recovery failed: found=%v got=%q", found, got)
	}
}
