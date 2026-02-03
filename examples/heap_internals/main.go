package main

import (
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/heap"
)

/*
EXEMPLO: HeapManager Internals

Este exemplo demonstra o uso direto do HeapManager, que é a camada
de armazenamento físico do Storage Engine.

O Heap é responsável por:
1. Armazenar documentos como bytes
2. Gerenciar offsets (posição física no arquivo)
3. Controlar versões via LSN (Log Sequence Number)
4. Implementar MVCC (versões múltiplas para concorrência)

Estrutura de um registro no Heap:
┌──────────────────────────────────────────────────────────────┐
│ Header: CreateLSN | DeleteLSN | PrevOffset | DataLen | Data │
└──────────────────────────────────────────────────────────────┘

NOTA: Normalmente você não usa o Heap diretamente.
O StorageEngine abstrai essas operações. Este exemplo é
para fins educacionais e debugging.
*/

func main() {
	heapPath := "internal.heap"
	cleanup(heapPath)
	defer cleanup(heapPath)

	// ========================================
	// 1. CRIAR HEAP MANAGER
	// ========================================
	fmt.Println("=== Criando HeapManager ===")

	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		return
	}
	defer hm.Close()

	fmt.Printf("✓ Heap criado: %s\n", heapPath)

	// ========================================
	// 2. ESCRITA DE DADOS (Write)
	// ========================================
	fmt.Println("\n=== Escrita de Dados ===")

	// Write(data []byte, createLSN uint64, prevOffset int64) (int64, error)
	// - data: bytes do documento
	// - createLSN: número de sequência do log (para MVCC)
	// - prevOffset: offset da versão anterior (-1 se nova)

	// Primeira inserção
	data1 := []byte(`{"id": 1, "name": "Alice", "version": 1}`)
	offset1, err := hm.Write(data1, 100, -1) // LSN=100, sem versão anterior
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
	} else {
		fmt.Printf("Documento 1 escrito:\n")
		fmt.Printf("  Offset: %d\n", offset1)
		fmt.Printf("  LSN: 100\n")
		fmt.Printf("  Data: %s\n", string(data1))
	}

	// Segunda inserção
	data2 := []byte(`{"id": 2, "name": "Bob", "version": 1}`)
	offset2, err := hm.Write(data2, 101, -1)
	if err == nil {
		fmt.Printf("\nDocumento 2 escrito:\n")
		fmt.Printf("  Offset: %d\n", offset2)
		fmt.Printf("  LSN: 101\n")
	}

	// ========================================
	// 3. LEITURA DE DADOS (Read)
	// ========================================
	fmt.Println("\n=== Leitura de Dados ===")

	// Read(offset int64) ([]byte, *RecordHeader, error)
	// Retorna: dados, header com metadados MVCC, erro
	readData, header, err := hm.Read(offset1)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
	} else {
		fmt.Printf("Lido do offset %d:\n", offset1)
		fmt.Printf("  Data: %s\n", string(readData))
		fmt.Printf("  CreateLSN: %d\n", header.CreateLSN)
		fmt.Printf("  DeleteLSN: %d (0 = ativo)\n", header.DeleteLSN)
		fmt.Printf("  PrevOffset: %d (-1 = primeira versão)\n", header.PrevOffset)
	}

	readData2, _, _ := hm.Read(offset2)
	fmt.Printf("\nLido do offset %d: %s\n", offset2, string(readData2))

	// ========================================
	// 4. ATUALIZAÇÃO (Versioning)
	// ========================================
	fmt.Println("\n=== Atualização com Versioning ===")

	// Atualizar Alice - cria nova versão apontando para a anterior
	data1v2 := []byte(`{"id": 1, "name": "Alice Updated", "version": 2}`)
	offset1v2, err := hm.Write(data1v2, 102, offset1) // LSN=102, aponta para offset1
	if err == nil {
		fmt.Printf("Nova versão de Alice:\n")
		fmt.Printf("  Novo offset: %d\n", offset1v2)
		fmt.Printf("  Aponta para offset anterior: %d\n", offset1)
		fmt.Printf("  Data: %s\n", string(data1v2))
	}

	// A versão antiga ainda está acessível (MVCC)
	oldData, _, _ := hm.Read(offset1)
	fmt.Printf("\nVersão antiga ainda acessível no offset %d: %s\n", offset1, string(oldData))

	// ========================================
	// 5. DELEÇÃO (Soft Delete via LSN)
	// ========================================
	fmt.Println("\n=== Deleção (Soft Delete) ===")

	// Delete(offset int64, deleteLSN uint64) error
	// Marca o registro como deletado no LSN especificado
	// O registro NÃO é removido fisicamente (para MVCC)

	err = hm.Delete(offset2, 103) // Deletar Bob no LSN 103
	if err == nil {
		fmt.Printf("✓ Documento no offset %d marcado como deletado (LSN=103)\n", offset2)
	}

	// O dado ainda pode ser lido (para transações que iniciaram antes do delete)
	deletedData, deletedHeader, _ := hm.Read(offset2)
	fmt.Printf("Dados ainda acessíveis: %s\n", string(deletedData))
	fmt.Printf("DeleteLSN agora é: %d\n", deletedHeader.DeleteLSN)
	fmt.Println("(Transações com SnapshotLSN < 103 ainda veem este registro)")

	// ========================================
	// 6. INFORMAÇÕES DO ARQUIVO
	// ========================================
	fmt.Println("\n=== Informações do Arquivo ===")

	fileInfo, _ := os.Stat(heapPath)
	fmt.Printf("Tamanho do arquivo: %d bytes\n", fileInfo.Size())
	fmt.Printf("Caminho: %s\n", hm.Path())

	// ========================================
	// 7. LEITURA COM MVCC (Conceitual)
	// ========================================
	fmt.Println("\n=== Conceito: Leitura MVCC ===")

	fmt.Println(`
No Storage Engine completo, a leitura usa MVCC:

1. Transação inicia com SnapshotLSN (ex: 101)
2. Ao ler um registro, verifica:
   - CreateLSN <= SnapshotLSN? (registro existia quando a tx iniciou)
   - DeleteLSN == 0 ou DeleteLSN > SnapshotLSN? (não deletado)

Exemplo:
  Registro A: CreateLSN=100, DeleteLSN=0
  Registro B: CreateLSN=100, DeleteLSN=105
  
  TX com SnapshotLSN=103 vê A e B (B não deletado no LSN 103)
  TX com SnapshotLSN=110 vê apenas A (B deletado no LSN 105)`)

	// ========================================
	// 8. LAYOUT FÍSICO DO ARQUIVO
	// ========================================
	fmt.Println("\n=== Layout Físico do Arquivo Heap ===")

	fmt.Println(`
Estrutura de cada registro:
┌────────────────────────────────────────────────────────────────┐
│ Length (4 bytes) │ Valid (1 byte) │ CreateLSN (8 bytes)        │
├────────────────────────────────────────────────────────────────┤
│ DeleteLSN (8 bytes) │ PrevOffset (8 bytes) │ Data (variável)   │
└────────────────────────────────────────────────────────────────┘

Campos:
- Length: Tamanho dos dados
- Valid: 1 = ativo, 0 = deletado
- CreateLSN: LSN quando o registro foi criado
- DeleteLSN: LSN quando foi deletado (0 = ativo)
- PrevOffset: Offset da versão anterior (-1 = primeira versão)
- Data: Conteúdo do documento

Isso permite:
- Reconstrução do estado em qualquer ponto no tempo
- Garbage collection de versões antigas
- Rollback de transações`)

	fmt.Println("\n✓ Exemplo concluído!")
}

func cleanup(heapPath string) {
	os.Remove(heapPath)
}
