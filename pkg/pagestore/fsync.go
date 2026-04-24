package pagestore

import (
	"fmt"
	"os"
)

// fsyncDir abre o diretório e chama Sync — garantia POSIX de que
// criações/renames dentro dele persistem além de um crash.
//
// Em Windows o comportamento é diferente (abrir diretório pra read
// geralmente não funciona como em POSIX). A função propaga o erro e
// deixa o caller decidir — no nosso caso, NewPageFile trata como
// erro fatal.
func fsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("pagestore: open dir %s: %w", dirPath, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("pagestore: fsync dir %s: %w", dirPath, err)
	}
	return nil
}
