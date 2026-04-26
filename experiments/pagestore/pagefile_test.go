package pagestore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

func mustKey(t testing.TB) []byte {
	k := make([]byte, crypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatal(err)
	}
	return k
}

func newCipher(t testing.TB) crypto.Cipher {
	c, err := crypto.NewAESGCM(mustKey(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTempPF(t testing.TB, cipher crypto.Cipher) *PageFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pages.db")
	pf, err := OpenPageFile(path, cipher)
	if err != nil {
		t.Fatal(err)
	}
	return pf
}

func fillBody(p *Page, seed byte, n int) {
	body := p.Body()
	for i := 0; i < n; i++ {
		body[i] = seed + byte(i%251)
	}
}

func TestRoundTrip_NoCipher(t *testing.T) {
	pf := openTempPF(t, nil)
	defer pf.Close()

	var p Page
	id := pf.Allocate()
	fillBody(&p, 0x42, BodySize)

	if err := pf.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}

	got, err := pf.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Body(), p.Body()) {
		t.Fatal("body divergente after round-trip")
	}
}

func TestRoundTrip_Encrypted(t *testing.T) {
	cipher := newCipher(t)
	pf := openTempPF(t, cipher)
	defer pf.Close()

	usable := UsableBodySize(cipher)
	var p Page
	id := pf.Allocate()
	fillBody(&p, 0xAB, usable)

	if err := pf.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}

	got, err := pf.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Body()[:usable], p.Body()[:usable]) {
		t.Fatal("plaintext divergente after round-trip encrypted")
	}
}

func TestEncrypted_DoesNotLeakPlaintext(t *testing.T) {
	cipher := newCipher(t)
	pf := openTempPF(t, cipher)
	defer pf.Close()

	secret := []byte("PII-CPF-123-456-789")
	var p Page
	copy(p.Body(), secret)
	id := pf.Allocate()
	if err := pf.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	pf.Close()

	raw, _ := os.ReadFile(pf.path)
	if bytes.Contains(raw, secret) {
		t.Fatal("plaintext encontrado em plaintext no file")
	}
}

func TestTamperDetection_Checksum(t *testing.T) {
	pf := openTempPF(t, nil)
	defer pf.Close()

	var p Page
	id := pf.Allocate()
	fillBody(&p, 1, BodySize)
	_ = pf.WritePage(id, &p)
	pf.Close()

	raw, _ := os.ReadFile(pf.path)
	// Adultera byte no meio do body (not no header)
	raw[HeaderSize+100] ^= 0xFF
	os.WriteFile(pf.path, raw, 0644)

	pf2, _ := OpenPageFile(pf.path, nil)
	defer pf2.Close()
	if _, err := pf2.ReadPage(id); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("esperava ErrChecksumMismatch, recebi: %v", err)
	}
}

func TestEncrypted_WrongKeyFails(t *testing.T) {
	cipher1 := newCipher(t)
	pf := openTempPF(t, cipher1)

	var p Page
	id := pf.Allocate()
	fillBody(&p, 7, UsableBodySize(cipher1))
	_ = pf.WritePage(id, &p)
	pf.Close()

	pf2, _ := OpenPageFile(pf.path, newCipher(t))
	defer pf2.Close()
	if _, err := pf2.ReadPage(id); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("esperava ErrDecryptFailed, recebi: %v", err)
	}
}

func TestEncrypted_AADBoundToPageID(t *testing.T) {
	cipher := newCipher(t)
	pf := openTempPF(t, cipher)

	var p Page
	fillBody(&p, 9, UsableBodySize(cipher))
	id1 := pf.Allocate()
	id2 := pf.Allocate()
	_ = pf.WritePage(id1, &p)
	_ = pf.WritePage(id2, &p) // conteúdo idêntico, AAD diferente
	pf.Close()

	// Troca fisicamente os bodies das duas pages (mantém headers).
	// Se AAD amarra ciphertext ao pageID, deencryptionr a page id1 com o
	// body da id2 deve failurer (mesmo sendo do mesmo dono).
	raw, _ := os.ReadFile(pf.path)
	body1 := make([]byte, BodySize)
	copy(body1, raw[HeaderSize:HeaderSize+BodySize])
	copy(raw[HeaderSize:HeaderSize+BodySize], raw[PageSize+HeaderSize:PageSize+HeaderSize+BodySize])
	copy(raw[PageSize+HeaderSize:PageSize+HeaderSize+BodySize], body1)

	// Recalcula checksums dos headers para bypassar a defesa de checksum
	// e confirmar que é a encryption autenticada que pega o swap.
	sum1 := crc32Sum(raw[HeaderSize : HeaderSize+BodySize])
	sum2 := crc32Sum(raw[PageSize+HeaderSize : PageSize+HeaderSize+BodySize])
	raw[24] = byte(sum1)
	raw[25] = byte(sum1 >> 8)
	raw[26] = byte(sum1 >> 16)
	raw[27] = byte(sum1 >> 24)
	raw[PageSize+24] = byte(sum2)
	raw[PageSize+25] = byte(sum2 >> 8)
	raw[PageSize+26] = byte(sum2 >> 16)
	raw[PageSize+27] = byte(sum2 >> 24)
	os.WriteFile(pf.path, raw, 0644)

	pf2, _ := OpenPageFile(pf.path, cipher)
	defer pf2.Close()
	if _, err := pf2.ReadPage(id1); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("esperava ErrDecryptFailed por AAD divergente, recebi: %v", err)
	}
}

func TestPageOutOfRange(t *testing.T) {
	pf := openTempPF(t, nil)
	defer pf.Close()
	if _, err := pf.ReadPage(99); !errors.Is(err, ErrPageOutOfRange) {
		t.Fatalf("esperava ErrPageOutOfRange, recebi: %v", err)
	}
}
