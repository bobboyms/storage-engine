// EXEMPLO: Backup/restore online
//
// Este exemplo mostra:
//   - como abrir o engine em modo de produção com WAL
//   - como criar um backup online com o engine aberto
//   - como validar a integridade do backup pelo manifest
//   - como restaurar em um diretório vazio
//   - como reabrir o engine restaurado e ler os dados
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

const (
	tableName = "accounts"
	indexName = "id"
)

func main() {
	baseDir := "backup_restore_demo"
	sourceDir := filepath.Join(baseDir, "source_db")
	backupDir := filepath.Join(baseDir, "backup")
	restoreDir := filepath.Join(baseDir, "restored_db")

	cleanup(baseDir)
	defer cleanup(baseDir)

	source, err := openEngine(sourceDir)
	if err != nil {
		fmt.Printf("erro abrindo engine origem: %v\n", err)
		return
	}

	for i := int64(1); i <= 3; i++ {
		doc := fmt.Sprintf(`{"id":%d,"email":"user-%d@example.com","balance":%d}`, i, i, i*100)
		if err := source.Put(tableName, indexName, types.IntKey(i), doc); err != nil {
			_ = source.Close()
			fmt.Printf("erro gravando documento %d: %v\n", i, err)
			return
		}
	}
	fmt.Println("Dados gravados no banco origem.")

	manifest, err := source.BackupOnline(backupDir)
	if err != nil {
		_ = source.Close()
		fmt.Printf("erro criando backup online: %v\n", err)
		return
	}
	fmt.Printf("Backup criado em %s com %d arquivos e checkpoint LSN %d.\n",
		backupDir, len(manifest.Files), manifest.CheckpointLSN)

	if _, err := storage.VerifyBackup(backupDir); err != nil {
		_ = source.Close()
		fmt.Printf("backup falhou na verificacao: %v\n", err)
		return
	}
	fmt.Println("Backup verificado com sucesso.")

	// Esta escrita acontece depois do backup. Ela continua existindo no banco
	// origem, mas nao deve aparecer no restore feito a partir do snapshot.
	if err := source.Put(tableName, indexName, types.IntKey(99),
		`{"id":99,"email":"after-backup@example.com","balance":9900}`); err != nil {
		_ = source.Close()
		fmt.Printf("erro gravando documento pos-backup: %v\n", err)
		return
	}

	if _, err := storage.RestoreBackup(backupDir, restoreDir); err != nil {
		_ = source.Close()
		fmt.Printf("erro restaurando backup: %v\n", err)
		return
	}
	fmt.Printf("Backup restaurado em %s.\n", restoreDir)

	restored, err := openEngine(restoreDir)
	if err != nil {
		_ = source.Close()
		fmt.Printf("erro abrindo engine restaurado: %v\n", err)
		return
	}
	defer restored.Close()
	defer source.Close()

	for i := int64(1); i <= 3; i++ {
		doc, found, err := restored.Get(tableName, indexName, types.IntKey(i))
		if err != nil {
			fmt.Printf("erro lendo documento restaurado %d: %v\n", i, err)
			return
		}
		if !found {
			fmt.Printf("documento restaurado %d nao encontrado\n", i)
			return
		}
		fmt.Printf("Documento restaurado %d: %s\n", i, doc)
	}

	_, found, err := restored.Get(tableName, indexName, types.IntKey(99))
	if err != nil {
		fmt.Printf("erro lendo documento pos-backup no restore: %v\n", err)
		return
	}
	if found {
		fmt.Println("ERRO: restore incluiu escrita posterior ao backup.")
		return
	}

	fmt.Println("OK: backup online, verificacao e restore funcionando.")
}

func openEngine(dir string) (*storage.StorageEngine, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	heapPath := filepath.Join(dir, "accounts.heap")
	walPath := filepath.Join(dir, "accounts.wal")

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		return nil, err
	}

	tableMgr := storage.NewTableMenager()
	if err := tableMgr.NewTable(tableName, []storage.Index{
		{Name: indexName, Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
	}, 0, hm); err != nil {
		_ = hm.Close()
		return nil, err
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		_ = hm.Close()
		return nil, err
	}

	engine, err := storage.NewProductionStorageEngine(tableMgr, walWriter)
	if err != nil {
		_ = walWriter.Close()
		_ = hm.Close()
		return nil, err
	}
	return engine, nil
}

func cleanup(path string) {
	_ = os.RemoveAll(path)
}
