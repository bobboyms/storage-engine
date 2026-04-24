package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// durableWriteFile escreve `data` em `path` com garantias fortes de
// durabilidade. Substitui `os.WriteFile`, que:
//   - NÃO fsync o arquivo (conteúdo fica em page cache do SO)
//   - NÃO fsync o diretório (entrada no inode table pode sumir após crash)
//
// Contrato de durability após retorno bem-sucedido:
//   - Conteúdo do arquivo está no disco (fsync do file)
//   - Entrada do arquivo no diretório está no disco (fsync do parent dir)
//   - Arquivo antigo, se existia no mesmo path, foi substituído atomicamente
//
// Padrão: write temp → fsync temp → rename → fsync dir.
func durableWriteFile(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".tmp"

	// 1. Grava no arquivo temporário
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return fmt.Errorf("durableWriteFile: open temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: write: %w", err)
	}

	// 2. fsync do arquivo temp — garante que os bytes estão no disco
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: fsync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: close temp: %w", err)
	}

	// 3. Rename atômico
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: rename: %w", err)
	}

	// 4. fsync do diretório — garante que a entry do nome está no disco
	// Sem isso, após crash o rename pode "sumir" (filesystem não persistiu o dir).
	return fsyncDir(filepath.Dir(path))
}

// fsyncDir abre o diretório e faz fsync. Crítico em POSIX pra garantir
// que operações no nível do dir (create, rename) sobrevivem crash.
// No Windows, open de diretório não funciona como esperado — retornamos
// nil por convenção (Windows tem comportamento diferente de durabilidade).
func fsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		// Em alguns FSs/OSes o diretório não pode ser aberto pra write;
		// usamos apenas Sync read-only. Se falhar, propaga o erro.
		return fmt.Errorf("fsyncDir: open %s: %w", dirPath, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsyncDir: sync %s: %w", dirPath, err)
	}
	return nil
}
