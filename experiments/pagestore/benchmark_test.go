package pagestore

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

// Os benchmarks abaixo geram os números que alimentam a ADR 001.
// Todos usam B.SetBytes(PageSize) para medir MB/s efetivo.

func benchWrite(b *testing.B, cipher crypto.Cipher) {
	path := filepath.Join(b.TempDir(), "bench.db")
	pf, err := OpenPageFile(path, cipher)
	if err != nil {
		b.Fatal(err)
	}
	defer pf.Close()

	var p Page
	usable := UsableBodySize(cipher)
	fillBody(&p, 0xAA, usable)

	b.SetBytes(int64(PageSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := pf.Allocate()
		if err := pf.WritePage(id, &p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite_NoCipher(b *testing.B) { benchWrite(b, nil) }
func BenchmarkWrite_AESGCM(b *testing.B)   { benchWrite(b, newCipher(b)) }

func benchRead(b *testing.B, cipher crypto.Cipher, pagesToPopulate int) {
	path := filepath.Join(b.TempDir(), "bench.db")
	pf, err := OpenPageFile(path, cipher)
	if err != nil {
		b.Fatal(err)
	}
	defer pf.Close()

	var p Page
	usable := UsableBodySize(cipher)
	fillBody(&p, 0x55, usable)

	for i := 0; i < pagesToPopulate; i++ {
		id := pf.Allocate()
		if err := pf.WritePage(id, &p); err != nil {
			b.Fatal(err)
		}
	}
	if err := pf.Sync(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(PageSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pageID := uint64(i % pagesToPopulate)
		if _, err := pf.ReadPage(pageID); err != nil {
			b.Fatal(err)
		}
	}
}

// Sequencial: lê IDs em ordem — SO faz read-ahead, serve do page cache.
func BenchmarkRead_Sequential_NoCipher(b *testing.B) { benchRead(b, nil, 1000) }
func BenchmarkRead_Sequential_AESGCM(b *testing.B)   { benchRead(b, newCipher(b), 1000) }

// Cifra pura: mede só Encrypt/Decrypt, isolando overhead do I/O.
func BenchmarkCipher_Encrypt_PageBody(b *testing.B) {
	c := newCipher(b)
	plaintext := make([]byte, UsableBodySize(c))
	aad := make([]byte, 8)

	b.SetBytes(int64(PageSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Encrypt(plaintext, aad); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCipher_Decrypt_PageBody(b *testing.B) {
	c := newCipher(b)
	plaintext := make([]byte, UsableBodySize(c))
	aad := make([]byte, 8)
	ct, _ := c.Encrypt(plaintext, aad)

	b.SetBytes(int64(PageSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Decrypt(ct, aad); err != nil {
			b.Fatal(err)
		}
	}
}
