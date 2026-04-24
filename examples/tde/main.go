// EXEMPLO: TDE (Transparent Data Encryption)
//
// Este exemplo mostra como abrir o storage engine com dados criptografados
// em repouso:
//   - heap criptografado
//   - indice B+Tree automatico criptografado
//   - WAL criptografado
//   - DEKs persistidas no keystore, cifradas pela master key
//
// Em producao, a master key deve vir de KMS/HSM/secret manager. Para rodar
// este exemplo localmente, voce pode deixar DB_MASTER_KEY_HEX vazio; o exemplo
// gera uma chave temporaria e remove os arquivos ao final.
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	enginecrypto "github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

const (
	heapPath     = "tde_accounts.heap"
	walPath      = "tde_accounts.wal"
	keyStorePath = "tde_keys.json"
	tableName    = "accounts"
	indexName    = "email"

	// Chave fixa apenas para exemplo local: 32 bytes em hex.
	// Nunca use uma chave hardcoded como esta em producao.
	testMasterKeyHex = "00112233445566778899aabbccddeeff102132435465768798a9babbdcddedef"
)

func main() {
	cleanup()
	defer cleanup()

	masterKey, generated, err := loadMasterKey()
	if err != nil {
		fmt.Printf("erro carregando master key: %v\n", err)
		return
	}
	if generated {
		fmt.Println("DB_MASTER_KEY_HEX nao definida; usando master key de teste para demo.")
	} else {
		fmt.Println("Master key carregada de DB_MASTER_KEY_HEX.")
	}

	se, indexPath, err := openEncryptedEngine(masterKey)
	if err != nil {
		fmt.Printf("erro abrindo engine com TDE: %v\n", err)
		return
	}

	secretEmail := "tde-secret-card-4111@example.com"
	secretDoc := `{"email":"tde-secret-card-4111@example.com","balance":2500,"note":"tde-secret-card-4111"}`

	if err := se.Put(tableName, indexName, types.VarcharKey(secretEmail), secretDoc); err != nil {
		_ = se.Close()
		fmt.Printf("erro gravando documento: %v\n", err)
		return
	}

	doc, found, err := se.Get(tableName, indexName, types.VarcharKey(secretEmail))
	if err != nil || !found {
		_ = se.Close()
		fmt.Printf("erro lendo documento antes do reopen: found=%v err=%v\n", found, err)
		return
	}
	fmt.Printf("Documento lido antes do reopen: %s\n", doc)

	if err := se.Close(); err != nil {
		fmt.Printf("erro fechando engine: %v\n", err)
		return
	}

	plaintext := []byte("tde-secret-card-4111")
	if err := assertNoPlaintext(heapPath, plaintext); err != nil {
		fmt.Printf("falha TDE no heap: %v\n", err)
		return
	}
	if err := assertNoPlaintext(indexPath, []byte(secretEmail)); err != nil {
		fmt.Printf("falha TDE no indice: %v\n", err)
		return
	}
	if err := assertNoPlaintext(walPath, plaintext); err != nil {
		fmt.Printf("falha TDE no WAL: %v\n", err)
		return
	}
	fmt.Println("OK: heap, indice e WAL nao contem o segredo em plaintext.")

	reopened, _, err := openEncryptedEngine(masterKey)
	if err != nil {
		fmt.Printf("erro reabrindo engine com a mesma chave: %v\n", err)
		return
	}
	defer reopened.Close()

	recovered, found, err := reopened.Get(tableName, indexName, types.VarcharKey(secretEmail))
	if err != nil || !found {
		fmt.Printf("erro lendo apos reopen: found=%v err=%v\n", found, err)
		return
	}
	fmt.Printf("Documento lido apos reopen: %s\n", recovered)
	fmt.Println("OK: TDE ativo e recovery com WAL criptografado funcionando.")
}

func openEncryptedEngine(masterKey []byte) (*storage.StorageEngine, string, error) {
	ks, err := enginecrypto.NewKeyStore(keyStorePath, masterKey)
	if err != nil {
		return nil, "", err
	}

	heapCipher, err := ks.GetOrCreateDEK("heap:accounts")
	if err != nil {
		return nil, "", err
	}
	indexCipher, err := ks.GetOrCreateDEK("btree:accounts:email")
	if err != nil {
		return nil, "", err
	}
	walCipher, err := ks.GetOrCreateDEK("wal:accounts")
	if err != nil {
		return nil, "", err
	}

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, heapCipher)
	if err != nil {
		return nil, "", err
	}

	tableMgr := storage.NewEncryptedTableMenager(indexCipher)
	if err := tableMgr.NewTable(tableName, []storage.Index{
		{Name: indexName, Primary: true, Type: storage.TypeVarchar},
	}, 0, hm); err != nil {
		return nil, "", err
	}

	walOpts := wal.DefaultOptions()
	walOpts.Cipher = walCipher
	walWriter, err := wal.NewWALWriter(walPath, walOpts)
	if err != nil {
		return nil, "", err
	}

	se, err := storage.NewProductionStorageEngine(tableMgr, walWriter)
	if err != nil {
		_ = walWriter.Close()
		return nil, "", err
	}

	indexPath := filepath.Join(filepath.Dir(heapPath), filepath.Base(heapPath)+"."+tableName+"."+indexName+".btree.v2")
	return se, indexPath, nil
}

func loadMasterKey() ([]byte, bool, error) {
	raw := os.Getenv("DB_MASTER_KEY_HEX")
	generated := false
	if raw == "" {
		raw = testMasterKeyHex
		generated = true
	}

	key, err := hex.DecodeString(raw)
	if err != nil {
		return nil, false, err
	}
	if len(key) != enginecrypto.KeySize {
		return nil, false, fmt.Errorf("DB_MASTER_KEY_HEX deve ter %d bytes (%d hex chars)", enginecrypto.KeySize, enginecrypto.KeySize*2)
	}
	return key, generated, nil
}

func assertNoPlaintext(path string, plaintext []byte) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if bytes.Contains(raw, plaintext) {
		return fmt.Errorf("%s contem plaintext %q", path, plaintext)
	}
	return nil
}

func cleanup() {
	os.Remove(heapPath)
	os.Remove(walPath)
	os.Remove(keyStorePath)
	os.Remove(filepath.Base(heapPath) + "." + tableName + "." + indexName + ".btree.v2")
}
