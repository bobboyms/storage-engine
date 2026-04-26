package crypto

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// KeyStore implementa a hierarquia de keys de dois níveis:
//
//	Master Key (KEK) — fornecida via env / KMS, NUNCA persistida em disco
//	     ↓ cifra
//	Data Encryption Keys (DEKs) — uma por recurso (heap, wal, ...)
//	     ↓ cifram
//	Páginas e records em disco
//
// As DEKs são persistidas EM DISCO já cifradas (wrapped) com a master key.
// Ao subir o engine, o operador fornece a master key, que decifra as DEKs
// e as carrega em memória. Rotacionar a master key = re-wrap das DEKs
// (rápido, sem reescrever dados).
type KeyStore struct {
	masterKey []byte
	path      string
	wrapped   map[string][]byte // nome lógico -> DEK cifrada
}

// keyFile é o formato persistido em disco (apenas DEKs cifradas).
type keyFile struct {
	WrappedDEKs map[string][]byte `json:"wrapped_deks"`
}

// NewKeyStore abre (ou cria) o keystore em `path`.
// `masterKey` must vir de fora do processo: env var, KMS, HSM, secret manager.
func NewKeyStore(path string, masterKey []byte) (*KeyStore, error) {
	if len(masterKey) != KeySize {
		return nil, fmt.Errorf("crypto: master key must ter %d bytes", KeySize)
	}
	ks := &KeyStore{
		masterKey: masterKey,
		path:      path,
		wrapped:   make(map[string][]byte),
	}
	if err := ks.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return ks, nil
}

func (ks *KeyStore) load() error {
	data, err := os.ReadFile(ks.path)
	if err != nil {
		return err
	}
	var kf keyFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return err
	}
	if kf.WrappedDEKs != nil {
		ks.wrapped = kf.WrappedDEKs
	}
	return nil
}

func (ks *KeyStore) save() error {
	data, err := json.MarshalIndent(keyFile{WrappedDEKs: ks.wrapped}, "", "  ")
	if err != nil {
		return err
	}
	// 0600: somente o owner lê/escreve
	return os.WriteFile(ks.path, data, 0600)
}

// GetOrCreateDEK devolve um Cipher pronto pra usar para o recurso `name`.
// Se a DEK ainda does not exist, gera, cifra com a master key e persiste.
func (ks *KeyStore) GetOrCreateDEK(name string) (Cipher, error) {
	kek, err := NewAESGCM(ks.masterKey)
	if err != nil {
		return nil, err
	}

	if w, ok := ks.wrapped[name]; ok {
		dek, err := kek.Decrypt(w, []byte(name))
		if err != nil {
			return nil, fmt.Errorf("crypto: failed to decrypt DEK %q (wrong master key?): %w", name, err)
		}
		return NewAESGCM(dek)
	}

	dek := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}
	wrapped, err := kek.Encrypt(dek, []byte(name))
	if err != nil {
		return nil, err
	}
	ks.wrapped[name] = wrapped
	if err := ks.save(); err != nil {
		return nil, err
	}
	return NewAESGCM(dek)
}

// RotateMasterKey re-wrappa todas as DEKs com a nova master key.
// Operação barata: NOT reescreve os dados cifrados pelas DEKs.
func (ks *KeyStore) RotateMasterKey(newMasterKey []byte) error {
	if len(newMasterKey) != KeySize {
		return fmt.Errorf("crypto: nova master key must ter %d bytes", KeySize)
	}
	oldKEK, err := NewAESGCM(ks.masterKey)
	if err != nil {
		return err
	}
	newKEK, err := NewAESGCM(newMasterKey)
	if err != nil {
		return err
	}

	rewrapped := make(map[string][]byte, len(ks.wrapped))
	for name, w := range ks.wrapped {
		dek, err := oldKEK.Decrypt(w, []byte(name))
		if err != nil {
			return fmt.Errorf("crypto: failed to decrypt DEK %q during rotation: %w", name, err)
		}
		nw, err := newKEK.Encrypt(dek, []byte(name))
		if err != nil {
			return err
		}
		rewrapped[name] = nw
	}
	ks.wrapped = rewrapped
	ks.masterKey = newMasterKey
	return ks.save()
}
