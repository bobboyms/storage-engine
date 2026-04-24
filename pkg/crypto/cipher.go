// Package crypto fornece primitivas de criptografia em repouso (TDE)
// para o storage engine. A interface Cipher é o ponto de extensão:
// qualquer componente que escreve em disco (heap, WAL, índices) recebe
// um Cipher opcional e cifra/decifra payloads sem conhecer o algoritmo.
package crypto

// Cipher é a abstração de cifra autenticada (AEAD).
// O parâmetro `aad` (Additional Authenticated Data) NÃO é cifrado,
// mas é validado na decifragem — usado para amarrar o ciphertext
// ao seu contexto (LSN, offset, table id) e impedir record-swap attacks.
type Cipher interface {
	Encrypt(plaintext, aad []byte) ([]byte, error)
	Decrypt(ciphertext, aad []byte) ([]byte, error)
	Overhead() int
}

// NoOpCipher é a implementação default quando TDE está desligado.
// Permite que o código de chamada não tenha branches `if cipher != nil`.
type NoOpCipher struct{}

func (NoOpCipher) Encrypt(p, _ []byte) ([]byte, error) { return p, nil }
func (NoOpCipher) Decrypt(c, _ []byte) ([]byte, error) { return c, nil }
func (NoOpCipher) Overhead() int                       { return 0 }
