package pagestore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

func mustKey(t testing.TB) []byte {
	t.Helper()
	k := make([]byte, crypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatal(err)
	}
	return k
}

func newCipher(t testing.TB) crypto.Cipher {
	t.Helper()
	c, err := crypto.NewAESGCM(mustKey(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTemp(t testing.TB, cipher crypto.Cipher) (*PageFile, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pages.db")
	pf, err := NewPageFile(path, cipher)
	if err != nil {
		t.Fatal(err)
	}
	return pf, path
}

func fillBody(p *Page, seed byte, n int) {
	body := p.Body()
	for i := 0; i < n; i++ {
		body[i] = seed + byte(i%251)
	}
}

// TestRoundTrip_1000Pages valida o critério de pronto da Fase 1.
func TestRoundTrip_1000Pages(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher crypto.Cipher
	}{
		{"plain", nil},
		{"encrypted", newCipher(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pf, _ := openTemp(t, tc.cipher)
			defer pf.Close()

			usable := pf.cipher.UsableBodySize()
			ids := make([]PageID, 1000)
			expected := make([][]byte, 1000)

			for i := 0; i < 1000; i++ {
				var p Page
				fillBody(&p, byte(i), usable)
				expected[i] = append([]byte(nil), p.Body()[:usable]...)

				id, err := pf.AllocatePage()
				if err != nil {
					t.Fatal(err)
				}
				ids[i] = id

				if err := pf.WritePage(id, &p); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
			}

			for i, id := range ids {
				got, err := pf.ReadPage(id)
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				if !bytes.Equal(got.Body()[:usable], expected[i]) {
					t.Fatalf("page %d: body divergente after round-trip", i)
				}
				hdr, _ := got.GetHeader()
				if hdr.PageID != id {
					t.Fatalf("page %d: header.PageID = %d, expected %d", i, hdr.PageID, id)
				}
			}
		})
	}
}

func TestPersistence_CloseAndReopen(t *testing.T) {
	cipher := newCipher(t)
	path := filepath.Join(t.TempDir(), "pages.db")

	pf, err := NewPageFile(path, cipher)
	if err != nil {
		t.Fatal(err)
	}

	var p Page
	fillBody(&p, 0x77, pf.cipher.UsableBodySize())
	id, _ := pf.AllocatePage()
	if err := pf.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	if err := pf.Sync(); err != nil {
		t.Fatal(err)
	}
	pf.Close()

	// Reabre com a mesma key
	pf2, err := NewPageFile(path, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer pf2.Close()

	got, err := pf2.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Body()[:pf2.cipher.UsableBodySize()], p.Body()[:pf.cipher.UsableBodySize()]) {
		t.Fatal("body divergente after reopen")
	}
}

func TestEncrypted_DoesNotLeakPlaintext(t *testing.T) {
	cipher := newCipher(t)
	pf, path := openTemp(t, cipher)

	secret := []byte("PII-email-alice@empresa.com.br")
	var p Page
	copy(p.Body(), secret)
	id, _ := pf.AllocatePage()
	_ = pf.WritePage(id, &p)
	pf.Close()

	raw, _ := os.ReadFile(path)
	if bytes.Contains(raw, secret) {
		t.Fatal("plaintext encontrado em claro no arquivo cifrado")
	}
}

func TestTamperDetection_ChecksumCatchesBodyCorruption(t *testing.T) {
	pf, path := openTemp(t, nil)
	var p Page
	fillBody(&p, 1, BodySize)
	id, _ := pf.AllocatePage()
	_ = pf.WritePage(id, &p)
	pf.Close()

	raw, _ := os.ReadFile(path)
	// Flip um bit no body (offset do slot 0 + header + algum byte interno)
	raw[int(id)*PageSize+HeaderSize+100] ^= 0xFF
	os.WriteFile(path, raw, 0644)

	pf2, _ := NewPageFile(path, nil)
	defer pf2.Close()
	if _, err := pf2.ReadPage(id); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}
}

func TestEncrypted_WrongKeyFails(t *testing.T) {
	pf, path := openTemp(t, newCipher(t))
	var p Page
	fillBody(&p, 3, pf.cipher.UsableBodySize())
	id, _ := pf.AllocatePage()
	_ = pf.WritePage(id, &p)
	pf.Close()

	pf2, _ := NewPageFile(path, newCipher(t))
	defer pf2.Close()
	if _, err := pf2.ReadPage(id); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("expected ErrDecryptFailed, got: %v", err)
	}
}

func TestEncrypted_AADBoundToPageID(t *testing.T) {
	cipher := newCipher(t)
	pf, path := openTemp(t, cipher)

	var p Page
	fillBody(&p, 9, pf.cipher.UsableBodySize())
	id1, _ := pf.AllocatePage()
	id2, _ := pf.AllocatePage()
	_ = pf.WritePage(id1, &p)
	_ = pf.WritePage(id2, &p)
	pf.Close()

	// Troca bodies entre pages e ajusta checksum pra passar na defesa
	// de checksum. Também corrige o PageID de cada header pra bypassar
	// a defesa "hdr.PageID != pageID". Sobra AAD amarrado a pageID.
	raw, _ := os.ReadFile(path)
	off1 := int64(id1) * PageSize
	off2 := int64(id2) * PageSize

	tmp := make([]byte, BodySize)
	copy(tmp, raw[off1+HeaderSize:off1+PageSize])
	copy(raw[off1+HeaderSize:off1+PageSize], raw[off2+HeaderSize:off2+PageSize])
	copy(raw[off2+HeaderSize:off2+PageSize], tmp)

	// Recalcula checksums
	sum1 := checksum(raw[off1+HeaderSize : off1+PageSize])
	sum2 := checksum(raw[off2+HeaderSize : off2+PageSize])
	// bytes 24..28 no header
	binaryPutU32(raw[off1+24:off1+28], sum1)
	binaryPutU32(raw[off2+24:off2+28], sum2)
	os.WriteFile(path, raw, 0644)

	pf2, _ := NewPageFile(path, cipher)
	defer pf2.Close()
	if _, err := pf2.ReadPage(id1); !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("expected ErrDecryptFailed por AAD divergente, got: %v", err)
	}
}

func TestInvalidMagic(t *testing.T) {
	pf, path := openTemp(t, nil)
	var p Page
	fillBody(&p, 1, BodySize)
	id, _ := pf.AllocatePage()
	_ = pf.WritePage(id, &p)
	pf.Close()

	raw, _ := os.ReadFile(path)
	// Zera o magic
	off := int64(id) * PageSize
	raw[off] = 0
	raw[off+1] = 0
	raw[off+2] = 0
	raw[off+3] = 0
	os.WriteFile(path, raw, 0644)

	pf2, _ := NewPageFile(path, nil)
	defer pf2.Close()
	if _, err := pf2.ReadPage(id); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got: %v", err)
	}
}

func TestOutOfRange(t *testing.T) {
	pf, _ := openTemp(t, nil)
	defer pf.Close()
	if _, err := pf.ReadPage(PageID(99)); !errors.Is(err, ErrPageOutOfRange) {
		t.Fatalf("expected ErrPageOutOfRange, got: %v", err)
	}
}

func TestPageID_ZeroReserved(t *testing.T) {
	pf, _ := openTemp(t, nil)
	defer pf.Close()

	var p Page
	if err := pf.WritePage(InvalidPageID, &p); err == nil {
		t.Fatal("write em pageID 0 should fail")
	}
	if _, err := pf.ReadPage(InvalidPageID); err == nil {
		t.Fatal("read em pageID 0 should fail")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	pf, _ := openTemp(t, nil)
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatalf("segundo Close should be no-op, got: %v", err)
	}
	if _, err := pf.AllocatePage(); !errors.Is(err, ErrClosed) {
		t.Fatalf("AllocatePage pós-Close should return ErrClosed, got: %v", err)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	// Roda com `go test -race` para detectar data races.
	pf, _ := openTemp(t, newCipher(t))
	defer pf.Close()

	const writers = 8
	const readers = 8
	const opsPerWriter = 50

	// Pré-aloca e pré-grava algumas pages para os readers terem algo.
	usable := pf.cipher.UsableBodySize()
	initialIDs := make([]PageID, 20)
	for i := range initialIDs {
		var p Page
		fillBody(&p, byte(i), usable)
		id, _ := pf.AllocatePage()
		initialIDs[i] = id
		_ = pf.WritePage(id, &p)
	}

	var wg sync.WaitGroup

	// Writers: alocam + gravam pages novas
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				var p Page
				fillBody(&p, byte(seed+i), usable)
				id, err := pf.AllocatePage()
				if err != nil {
					t.Error(err)
					return
				}
				if err := pf.WritePage(id, &p); err != nil {
					t.Error(err)
					return
				}
			}
		}(w * 1000)
	}

	// Readers: lêem o set inicial em loop
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				id := initialIDs[i%len(initialIDs)]
				if _, err := pf.ReadPage(id); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Verifica que numPages está consistente com nextID
	if pf.NumPages() == 0 {
		t.Fatal("numPages should be > 0 after writes")
	}
}

func TestOpenInvalidFileSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	// Escreve 100 bytes (not múltiplo de 8192)
	os.WriteFile(path, make([]byte, 100), 0644)

	_, err := NewPageFile(path, nil)
	if err == nil {
		t.Fatal("expected error opening abrir arquivo com tamanho not-múltiplo de PageSize")
	}
}

// binaryPutU32 escreve little-endian sem importar encoding/binary
// only in this test file (mantém os imports do test file curtos).
func binaryPutU32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
