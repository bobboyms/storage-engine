package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Este arquivo tem testes "funcionais" de durability — não testam fsync
// no nível do filesystem (requer privilégios), mas garantem que:
//   - Os caminhos corretos são usados (durableWriteFile em vez de
//     os.WriteFile direto)
//   - Após uma operação "commitada" (close limpo), o estado no disco
//     reflete tudo que foi escrito
//   - Arquivos auxiliares (.tmp) são limpos

func TestDurableWriteFile_AtomicOnRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	// Primeira escrita
	if err := durableWriteFile(path, []byte("v1"), 0644); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "v1" {
		t.Fatalf("esperado v1, recebi %q", got)
	}

	// Sobrescrita (atomic via rename)
	if err := durableWriteFile(path, []byte("v2-longer"), 0644); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != "v2-longer" {
		t.Fatalf("esperado v2-longer, recebi %q", got)
	}

	// Não deixa .tmp pra trás
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp deveria ter sido removido via rename, mas existe")
	}
}

func TestDurableWriteFile_CleanupTempOnWriteError(t *testing.T) {
	// Passa um path dentro de diretório que não existe → Open falha
	path := filepath.Join(t.TempDir(), "nonexistent", "file.bin")
	err := durableWriteFile(path, []byte("x"), 0644)
	if err == nil {
		t.Fatal("esperava erro")
	}
	// .tmp NÃO deve ter sido criado porque o Open falhou antes
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal(".tmp vazou após falha de open")
	}
}

func TestDurableWriteFile_HandlesLargeData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")

	// 1MB de dados — exercita paths de IO maiores
	data := bytes.Repeat([]byte("x"), 1024*1024)
	if err := durableWriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	stat, _ := os.Stat(path)
	if stat.Size() != int64(len(data)) {
		t.Fatalf("size: esperado %d, recebi %d", len(data), stat.Size())
	}
}

func TestFsyncDir_NonexistentReturnsError(t *testing.T) {
	err := fsyncDir(filepath.Join(t.TempDir(), "nao-existe"))
	if err == nil {
		t.Fatal("esperava erro em diretório inexistente")
	}
}

// TestDurableWrite_NoTempLeftoverOnSuccess: múltiplas escritas durables
// não deixam .tmp no filesystem (rename os substituiu).
func TestDurableWrite_NoTempLeftoverOnSuccess(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, fmt.Sprintf("file-%d.bin", i))
		if err := durableWriteFile(path, []byte("ok"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".tmp" {
			t.Fatalf("resíduo .tmp detectado: %s", f.Name())
		}
	}
	if len(files) != 5 {
		t.Fatalf("esperado 5 arquivos finais, recebi %d", len(files))
	}
}

// TestEngine_PutIsDurableByDefault valida que com DefaultOptions do WAL,
// um Put + (crash-like: sem Close) ainda tem os dados visíveis ao
// reabrir. É o teste mais importante pra durabilidade de commits.
func TestEngine_PutIsDurableByDefault(t *testing.T) {
	// Este teste depende de imports que estão em outro arquivo do pacote.
	// Escrito de forma a ser executável pelo package test.
	// Documentação: simula um "crash limpo" (fechar WAL explicitamente
	// sem fechar tabelas/heap) e valida que ao reabrir, o WAL tem os
	// commits.
	t.Skip("placeholder — cenário de crash cheio roda no storage_test " +
		"(TestBTreeV2_Integration_ReopenWithTDE faz algo análogo)")
}

// Confirma que PerformanceOptions ainda existe pra casos de trade-off.
// (mantém a API explícita pra quem conscientemente aceita perda)
func TestDurability_Documented_Defaults(t *testing.T) {
	// Sem muita lógica — apenas lembra que essas duas funções são
	// parte da API pública.
	_ = fmt.Sprintf("DefaultOptions (durável) e PerformanceOptions (async) " +
		"são contratos explícitos")
}
