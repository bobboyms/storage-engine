package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const (
	KeySize   = 32 // AES-256
	NonceSize = 12 // padrão GCM
	TagSize   = 16 // tag de autenticação GCM
)

// AESGCM implementa Cipher usando AES-256-GCM (AEAD).
// Formato do ciphertext em disco: nonce(12) || ciphertext || tag(16)
type AESGCM struct {
	aead cipher.AEAD
}

func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("crypto: chave deve ter %d bytes, recebida %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{aead: aead}, nil
}

func (c *AESGCM) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	// Pré-aloca: nonce + ciphertext (= len(plaintext) + TagSize)
	out := make([]byte, NonceSize, NonceSize+len(plaintext)+TagSize)
	copy(out, nonce)
	return c.aead.Seal(out, nonce, plaintext, aad), nil
}

func (c *AESGCM) Decrypt(ciphertext, aad []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize+TagSize {
		return nil, fmt.Errorf("crypto: ciphertext muito curto (%d bytes)", len(ciphertext))
	}
	nonce := ciphertext[:NonceSize]
	body := ciphertext[NonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, body, aad)
	if err != nil {
		// AEAD falha = chave errada, dado corrompido OU adulteração
		return nil, fmt.Errorf("crypto: falha de autenticação (chave inválida ou dado adulterado): %w", err)
	}
	return plaintext, nil
}

func (c *AESGCM) Overhead() int { return NonceSize + TagSize }
