package crypto

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"path/filepath"
	"testing"
)

func mustKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestAESGCM_RoundTrip(t *testing.T) {
	c, err := NewAESGCM(mustKey(t))
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"id":42,"name":"thiago"}`)
	aad := make([]byte, 8)
	binary.LittleEndian.PutUint64(aad, 12345)

	ct, err := c.Encrypt(plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ct, plaintext) {
		t.Fatal("ciphertext contém plaintext em claro")
	}

	pt, err := c.Decrypt(ct, aad)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatalf("round-trip falhou: %q != %q", pt, plaintext)
	}
}

func TestAESGCM_TamperDetection(t *testing.T) {
	c, _ := NewAESGCM(mustKey(t))
	ct, _ := c.Encrypt([]byte("dado sensível"), nil)

	// Flip um bit no ciphertext
	ct[len(ct)-5] ^= 0x01

	if _, err := c.Decrypt(ct, nil); err == nil {
		t.Fatal("esperava falha de autenticação após adulteração")
	}
}

func TestAESGCM_AADMismatch(t *testing.T) {
	c, _ := NewAESGCM(mustKey(t))
	ct, _ := c.Encrypt([]byte("dado"), []byte("lsn=1"))

	// AAD diferente = autenticação falha (impede record-swap)
	if _, err := c.Decrypt(ct, []byte("lsn=2")); err == nil {
		t.Fatal("esperava falha por AAD divergente")
	}
}

func TestKeyStore_WrapUnwrap(t *testing.T) {
	dir := t.TempDir()
	master := mustKey(t)

	ks, err := NewKeyStore(filepath.Join(dir, "keys.json"), master)
	if err != nil {
		t.Fatal(err)
	}

	cipher1, err := ks.GetOrCreateDEK("heap:users")
	if err != nil {
		t.Fatal(err)
	}
	ct, _ := cipher1.Encrypt([]byte("segredo"), nil)

	// Reabre com a mesma master key — deve recuperar a DEK e decifrar
	ks2, err := NewKeyStore(filepath.Join(dir, "keys.json"), master)
	if err != nil {
		t.Fatal(err)
	}
	cipher2, err := ks2.GetOrCreateDEK("heap:users")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := cipher2.Decrypt(ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != "segredo" {
		t.Fatalf("esperava 'segredo', recebi %q", pt)
	}
}

func TestKeyStore_WrongMasterKey(t *testing.T) {
	dir := t.TempDir()
	ks, _ := NewKeyStore(filepath.Join(dir, "keys.json"), mustKey(t))
	if _, err := ks.GetOrCreateDEK("heap:users"); err != nil {
		t.Fatal(err)
	}

	// Outro processo tentando abrir com master key errada
	wrongKS, _ := NewKeyStore(filepath.Join(dir, "keys.json"), mustKey(t))
	if _, err := wrongKS.GetOrCreateDEK("heap:users"); err == nil {
		t.Fatal("esperava falha ao decifrar DEK com master key errada")
	}
}

func TestKeyStore_RotateMasterKey(t *testing.T) {
	dir := t.TempDir()
	oldMaster := mustKey(t)
	newMaster := mustKey(t)

	ks, _ := NewKeyStore(filepath.Join(dir, "keys.json"), oldMaster)
	cipher, _ := ks.GetOrCreateDEK("wal")
	ct, _ := cipher.Encrypt([]byte("entry"), nil)

	if err := ks.RotateMasterKey(newMaster); err != nil {
		t.Fatal(err)
	}

	// Reabrir com a master key NOVA deve funcionar; com a velha, não
	rotated, err := NewKeyStore(filepath.Join(dir, "keys.json"), newMaster)
	if err != nil {
		t.Fatal(err)
	}
	cipher2, err := rotated.GetOrCreateDEK("wal")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := cipher2.Decrypt(ct, nil)
	if err != nil || string(pt) != "entry" {
		t.Fatalf("dados antigos devem continuar legíveis após rotação: pt=%q err=%v", pt, err)
	}

	stale, _ := NewKeyStore(filepath.Join(dir, "keys.json"), oldMaster)
	if _, err := stale.GetOrCreateDEK("wal"); err == nil {
		t.Fatal("master key antiga não deveria mais funcionar")
	}
}
