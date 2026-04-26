package pagestore

import (
	"encoding/binary"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

// PageCipher envolve um crypto.Cipher com a convenção específica de
// cifragem de pages: AAD = PageID em little-endian. Amarra cada
// ciphertext ao seu pageID e impede ataques de swap entre pages.
//
// Quando o crypto.Cipher subjacente é NoOp, PageCipher também é no-op
// (sem alocações no caminho quente, sem expansão do body).
type PageCipher struct {
	inner   crypto.Cipher
	noop    bool
	usable  int // BodySize - overhead
}

// NewPageCipher cria um PageCipher. Passe nil para `c` para obter um
// PageCipher no-op (comportamento sem TDE, sem overhead).
func NewPageCipher(c crypto.Cipher) *PageCipher {
	if c == nil {
		c = crypto.NoOpCipher{}
	}
	_, noop := c.(crypto.NoOpCipher)
	return &PageCipher{
		inner:  c,
		noop:   noop,
		usable: BodySize - c.Overhead(),
	}
}

// IsNoOp indica se a cifragem é passthrough. Permite a chamadores
// evitar cópias desnecessárias no caminho quente.
func (pc *PageCipher) IsNoOp() bool { return pc.noop }

// UsableBodySize devolve quantos bytes de plaintext cabem em BodySize
// depois de descontado o overhead da cifra (0 para NoOp).
func (pc *PageCipher) UsableBodySize() int { return pc.usable }

// EncryptBody cifra `plaintext` (<= UsableBodySize bytes) com AAD = pageID.
// Retorna slice de exatamente BodySize bytes pronto para gravar em disco.
// Quando NoOp, devolve `plaintext` inalterado.
func (pc *PageCipher) EncryptBody(plaintext []byte, pageID PageID) ([]byte, error) {
	if pc.noop {
		return plaintext, nil
	}
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], uint64(pageID))
	return pc.inner.Encrypt(plaintext, aad[:])
}

// DecryptBody decifra um body lido do disco (exatamente BodySize bytes),
// validando AAD = pageID. Retorna o plaintext (UsableBodySize bytes).
// Quando NoOp, devolve `diskBody` inalterado.
func (pc *PageCipher) DecryptBody(diskBody []byte, pageID PageID) ([]byte, error) {
	if pc.noop {
		return diskBody, nil
	}
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], uint64(pageID))
	return pc.inner.Decrypt(diskBody, aad[:])
}
