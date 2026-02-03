package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"github.com/google/uuid"
)

func GenerateKey() string {
	// NewV7 gera um UUID baseado no tempo atual + aleatoriedade segura
	id, err := uuid.NewV7()
	if err != nil {
		panic(err) // Em caso improvável de erro no gerador de entropia
	}
	return id.String()
}

type StorageEngine struct {
	TableMetaData *TableMetaData
	WAL           *wal.WALWriter // WAL persistente
	Heap          *heap.HeapManager
	Checkpoint    *CheckpointManager
	lsnTracker    *LSNTracker
	metaMu        sync.RWMutex // Lock apenas para operações de metadados (ListTables, etc)
	// Nota: Lock por tabela agora está em Table.mu
}

func NewStorageEngine(tableMetaData *TableMetaData, walPath string, heapPath string) (*StorageEngine, error) {
	// Configuração do Heap
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		return nil, fmt.Errorf("falha ao iniciar Heap: %w", err)
	}

	if walPath == "" {
		return &StorageEngine{
			TableMetaData: tableMetaData,
			WAL:           nil,
			Heap:          hm,
			lsnTracker:    NewLSNTracker(0),
		}, nil
	}

	// Configuração padrão segura
	opts := wal.DefaultOptions()
	opts.SyncPolicy = wal.SyncBatch // Alta performance com durabilidade razoável
	opts.SyncBatchBytes = 10 * 1024 // Flush a cada 10KB (exemplo)

	// Se o diretório não existe, falha na abertura do arquivo.
	// Assume que o chamador já gerenciou diretórios (ou podemos criar aqui)

	writer, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		hm.Close() // Fecha o heap se falhar o WAL
		return nil, fmt.Errorf("falha ao iniciar WAL: %w", err)
	}

	// Configuração do Checkpoint Manager
	// Por padrão, salva checkpoints no mesmo diretório do WAL (ou diretório pai do WAL file)
	checkpointDir := filepath.Dir(walPath)
	checkpointMgr := NewCheckpointManager(checkpointDir)

	return &StorageEngine{
		TableMetaData: tableMetaData,
		WAL:           writer,
		Heap:          hm,
		Checkpoint:    checkpointMgr,
		lsnTracker:    NewLSNTracker(0),
	}, nil
}

// Transaction representa um contexto de execução com Snapshot Isolation
type Transaction struct {
	SnapshotLSN uint64
	engine      *StorageEngine
}

// BeginRead inicia uma transação de leitura (Snapshot)
func (se *StorageEngine) BeginRead() *Transaction {
	return &Transaction{
		SnapshotLSN: se.lsnTracker.Current(), // Captura o "agora" linearizável
		engine:      se,
	}
}

// IsVisible verifica se uma versão do registro é visível para esta transação
func (tx *Transaction) IsVisible(createLSN uint64) bool {
	// Regra básica: Eu vejo tudo que foi commitado ANTES do meu snapshot
	return createLSN <= tx.SnapshotLSN
}

func (se *StorageEngine) Close() error {
	var err error
	if se.WAL != nil {
		if wErr := se.WAL.Close(); wErr != nil {
			err = wErr
		}
	}
	if se.Heap != nil {
		if hErr := se.Heap.Close(); hErr != nil {
			if err == nil {
				err = hErr
			} else {
				err = fmt.Errorf("wal close error: %v, heap close error: %v", err, hErr)
			}
		}
	}
	return err
}

func (se *StorageEngine) Cursor(tree *btree.BPlusTree) *Cursor {
	return &Cursor{tree: tree}
}

// Put: Insert ou Update com Durabilidade (WAL)
func (se *StorageEngine) Put(tableName string, indexName string, key types.Comparable, document string) error {
	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}

	// Adquire read lock na tabela específica (Schema Lock)
	table.RLock()
	defer table.RUnlock()

	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return err
	}

	// Try convert json to bson for validation and better storage
	// If it fails, we treat it as a raw string (backward compatibility for tests)
	bsonDoc, err := JsonToBson(document)
	var bsonData []byte
	if err == nil {
		// Verify if the key exists
		exists, keyType := DoesTheKeyExist(bsonDoc, indexName)
		if !exists {
			return &errors.IndexNotFoundError{
				Name: indexName,
			}
		}

		// Verify if the key type is valid
		if keyType != index.Type {
			return &errors.InvalidKeyTypeError{
				Name:     indexName,
				TypeName: keyType.String(),
			}
		}

		//Serialize bson to bytes
		bsonData, _ = MarshalBson(bsonDoc)
	} else {
		// Fallback to raw bytes
		bsonData = []byte(document)
	}

	// LSN Management
	// Geramos o LSN *antes* de escrever no WAL ou Heap para garantir ordem
	currentLSN := se.lsnTracker.Next()

	// 1. Write Ahead Log
	if se.WAL != nil {
		payload, err := SerializeDocumentEntry(tableName, indexName, key, bsonData)
		if err != nil {
			return err
		}

		entry := wal.AcquireEntry()
		entry.Header.Magic = wal.WALMagic
		entry.Header.Version = 1
		entry.Header.EntryType = wal.EntryInsert // Tratamos Update como Insert no WAL log-structured

		entry.Header.LSN = currentLSN

		entry.Header.PayloadLen = uint32(len(payload))
		entry.Header.CRC32 = wal.CalculateCRC32(payload)
		entry.Payload = append(entry.Payload, payload...)

		if err := se.WAL.WriteEntry(entry); err != nil {
			wal.ReleaseEntry(entry)
			return fmt.Errorf("wal write failed: %w", err)
		}
		wal.ReleaseEntry(entry)
	}

	// 2. Verifica se já existe (UPDATE vs INSERT) para Chain
	// Como estamos com RLock, podemos ler a árvore.
	// Se existe, a nova versão deve apontar para a antiga (Chain).
	// Se não existe, PrevOffset = -1.

	// A árvore aponta para o HEAD (versão mais recente).
	// Precisamos pegar o offset atual antes de sobrescrever.

	var prevOffset int64 = -1

	node, found := index.Tree.Search(key)
	if found {
		_, idx := node.FindLeafLowerBound(key)
		if idx < node.N && node.Keys[idx].Compare(key) == 0 {
			// Found existing key
			prevOffset = node.DataPtrs[idx]
		}
	}

	// 3. Escreve no Heap com LSN e PrevOffset (Version Chain)
	offset, err := se.Heap.Write(bsonData, currentLSN, prevOffset)
	if err != nil {
		return fmt.Errorf("heap write failed: %w", err)
	}

	// 4. Modifica Memória (B+ Tree)
	// Agora a árvore aponta para o novo registro (HEAD)
	err = index.Tree.Insert(key, offset)
	if err != nil {
		// Se for erro de chave duplicada (Unique Index), é um UPDATE.
		// Precisamos forçar a atualização do ponteiro na B-Tree.
		if _, ok := err.(*errors.DuplicateKeyError); ok {
			err = index.Tree.Replace(key, offset)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Get executa uma busca no contexto da transação (Snapshot Isolation)
func (tx *Transaction) Get(tableName string, indexName string, key types.Comparable) (string, bool, error) {
	se := tx.engine

	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return "", false, err
	}

	// Em MVCC, leituras concorrem com escritas.
	// O Table Lock atual é global para a tabela.
	// PROVISÓRIO: Mantemos RLock para garantir que a árvore não mude drasticamente (embora BTree tenha latching).
	// No futuro, podemos relaxar isso.
	table.RLock()
	defer table.RUnlock()

	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return "", false, err
	}

	// Busca na árvore
	// O Search retorna o nó folha onde a chave pode estar
	node, found := index.Tree.Search(key)
	if !found {
		return "", false, nil
	}

	// Temos o nó, busco exato
	_, idx := node.FindLeafLowerBound(key)

	if idx < node.N && node.Keys[idx].Compare(key) == 0 {
		// B-Tree points to the HEAD (newest) version of the record chain.
		currentOffset := node.DataPtrs[idx]

		// Version Chain Traversal (Time Travel)
		for currentOffset != -1 {
			docBytes, header, err := se.Heap.Read(currentOffset)
			if err != nil {
				return "", true, fmt.Errorf("failed to read from heap: %w", err)
			}

			// Visibility Check
			if tx.IsVisible(header.CreateLSN) {
				// 1. Se Valid=true, está viva.
				// 2. Se Valid=false (Delete), verificamos SE a deleção aconteceu DEPOIS do snapshot.

				isVisibleVersion := header.Valid || (header.DeleteLSN > tx.SnapshotLSN)

				if isVisibleVersion {
					// Encontramos a versão visível!
					jsonStr, err := BsonToJson(docBytes)
					if err == nil {
						return jsonStr, true, nil
					}
					return string(docBytes), true, nil
				} else {
					// A versão existe e é visível quanto a CRIAÇÃO, mas já estava DELETADA no snapshot.
					// Portanto, para este snapshot, a chave não existe.
					return "", false, nil
				}
			}

			// Se a versão atual é MUITO NOVA (CreateLSN > SnapshotLSN),
			// precisamos olhar a versão anterior na corrente.
			currentOffset = header.PrevOffset
		}

		// Chegamos ao fim da chain sem achar versão visível
		return "", false, nil
	}

	return "", false, nil
}

// Get wrapper para conveniência (Autocommit / Snapshot instantâneo)
func (se *StorageEngine) Get(tableName string, indexName string, key types.Comparable) (string, bool, error) {
	return se.BeginRead().Get(tableName, indexName, key)
}

// Scan executa uma busca por range no contexto da transação
func (tx *Transaction) Scan(tableName string, indexName string, condition *query.ScanCondition) ([]string, error) {
	se := tx.engine

	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}

	// Adquire read lock na tabela específica
	table.RLock()
	defer table.RUnlock()

	results := []string{}
	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return results, err
	}
	c := se.Cursor(index.Tree)
	defer c.Close() // Libera cursor

	// Otimiza scan se possível (=, >, >=, BETWEEN)
	if condition.ShouldSeek() {
		startKey := condition.GetStartKey()
		c.Seek(startKey)

		for c.Valid() {
			key := c.Key()

			// Verifica se ainda devemos continuar o scan
			if !condition.ShouldContinue(key) {
				break
			}

			// Verifica se a chave satisfaz a condição
			if condition.Matches(key) {
				currentOffset := c.Value()

				// Version Chain Traversal
				foundVisible := false
				var visibleVal string

				for currentOffset != -1 {
					docBytes, header, err := se.Heap.Read(currentOffset)
					if err != nil {
						return nil, fmt.Errorf("heap read failed at key %v: %w", key, err)
					}

					if tx.IsVisible(header.CreateLSN) {
						isVisibleVersion := header.Valid || (header.DeleteLSN > tx.SnapshotLSN)
						if isVisibleVersion {
							jsonStr, err := BsonToJson(docBytes)
							if err == nil {
								visibleVal = jsonStr
							} else {
								visibleVal = string(docBytes)
							}
							foundVisible = true
							break // Encontrou versão
						} else {
							// Versão deletada no snapshot
							break // Não existe para este snapshot
						}
					}
					// Muito novo, tenta anterior
					currentOffset = header.PrevOffset
				}

				if foundVisible {
					results = append(results, visibleVal)
				}
			}
			c.Next()
		}
	} else {
		// Full scan para operadores como != e <
		// Inicia do começo da árvore
		leftmost := index.Tree.Root
		for !leftmost.Leaf {
			leftmost = leftmost.Children[0]
		}

		c.currentNode = leftmost
		c.currentIndex = 0

		for c.Valid() {
			key := c.Key()

			// Para < e <=, podemos parar cedo
			if !condition.ShouldContinue(key) {
				break
			}

			if condition.Matches(key) {
				currentOffset := c.Value()

				// Version Chain Traversal
				foundVisible := false
				var visibleVal string

				for currentOffset != -1 {
					docBytes, header, err := se.Heap.Read(currentOffset)
					if err != nil {
						return nil, fmt.Errorf("heap read failed at key %v: %w", key, err)
					}

					if tx.IsVisible(header.CreateLSN) {
						isVisibleVersion := header.Valid || (header.DeleteLSN > tx.SnapshotLSN)
						if isVisibleVersion {
							jsonStr, err := BsonToJson(docBytes)
							if err == nil {
								visibleVal = jsonStr
							} else {
								visibleVal = string(docBytes)
							}
							foundVisible = true
							break // Encontrou versão
						} else {
							break // Deletado no snapshot
						}
					}
					// Muito novo, tenta anterior
					currentOffset = header.PrevOffset
				}

				if foundVisible {
					results = append(results, visibleVal)
				}
			}
			c.Next()
		}
	}

	return results, nil
}

// Scan wrapper para conveniência
func (se *StorageEngine) Scan(tableName string, indexName string, condition *query.ScanCondition) ([]string, error) {
	return se.BeginRead().Scan(tableName, indexName, condition)
}

// RangeScan: Wrapper de conveniência para BETWEEN (mantido para compatibilidade)
func (se *StorageEngine) RangeScan(tableName string, indexName string, start, end types.Comparable) ([]string, error) {
	return se.Scan(tableName, indexName, query.Between(start, end))
}

// Delete: Remove (DELETE FROM WHERE id = x)
func (se *StorageEngine) Del(tableName string, indexName string, key types.Comparable) (bool, error) {
	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return false, err
	}

	// Adquire read lock na tabela específica (Schema Lock)
	table.RLock()
	defer table.RUnlock()

	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return false, err
	}

	// LSN Management
	currentLSN := se.lsnTracker.Next()

	// 1. Write Ahead Log
	if se.WAL != nil {
		// Para delete, apenas precisamos da chave. Documento vazio.
		payload, err := SerializeDocumentEntry(tableName, indexName, key, nil)
		if err != nil {
			return false, err
		}

		entry := wal.AcquireEntry()
		entry.Header.Magic = wal.WALMagic
		entry.Header.Version = 1
		entry.Header.EntryType = wal.EntryDelete

		entry.Header.LSN = currentLSN

		entry.Header.PayloadLen = uint32(len(payload))
		entry.Header.CRC32 = wal.CalculateCRC32(payload)
		entry.Payload = append(entry.Payload, payload...)

		if err := se.WAL.WriteEntry(entry); err != nil {
			wal.ReleaseEntry(entry)
			return false, fmt.Errorf("wal write failed: %w", err)
		}
		wal.ReleaseEntry(entry)
	}

	// 2. Modifica Memória e Heap
	// Primeiro precisamos pegar o offset para deletar do heap
	node, _ := index.Tree.Search(key)
	if node != nil {
		_, idx := node.FindLeafLowerBound(key)
		if idx < node.N && node.Keys[idx].Compare(key) == 0 {
			offset := node.DataPtrs[idx]

			// Marca como deletado no heap, com o DeletedLSN atual.
			// Em MVCC puro, deveríamos talvez escrever um registro Tombstone como Head da chain.
			// Mas marcar isValid=false + DeleteLSN no head também funciona para parar a chain.
			// A diferença e que overwrite inplace é destructive.
			// Como o Heap é append-only para novos writes, aqui estamos violando isso alterando o passado?
			// Sim, delete altera o registro existente.
			// TODO Padrão: Write New Version (Tombstone) -> Point Tree to Tombstone.
			// Para Phase 2 simplificado: Update in-place Head com DeleteLSN.
			if err := se.Heap.Delete(offset, currentLSN); err != nil {
				return false, fmt.Errorf("heap delete failed: %w", err)
			}
		} else {
			// Chave não encontrada
			return false, nil
		}
	} else {
		return false, nil
	}

	// MVCC Phase 2: Do NOT remove from B-Tree.
	// We need to keep the key pointing to the "Deleted" record (Tombstone)
	// so that older transactions can check visibility (DeleteLSN) and potential previous versions.
	// Garbage Collection (Vacuum) will eventually remove these when safe.
	// removed := index.Tree.Root.Remove(key)
	// if index.Tree.Root.N == 0 && !index.Tree.Root.Leaf {
	// 	index.Tree.Root = index.Tree.Root.Children[0]
	// }

	return true, nil
}

// CreateCheckpoint força a criação de checkpoints para todas as tabelas
// Adquire read lock em cada tabela individualmente durante o checkpoint
func (se *StorageEngine) CreateCheckpoint() error {
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		// Adquire write lock na tabela durante o checkpoint para garantir consistência (snapshot)
		table.Lock()

		// Captura o LSN *após* adquirir o lock e drenar as escritas em andamento.
		// Isso garante que o LSN reflete com precisão o estado congelado da tabela.
		currentLSN := se.lsnTracker.Current()

		// Usa GetIndices() que não adquire lock (já temos o lock)
		for _, idx := range table.GetIndices() {
			if err := se.Checkpoint.CreateCheckpoint(tableName, idx.Name, idx.Tree, currentLSN); err != nil {
				table.Unlock()
				return err
			}
		}
		table.Unlock()
	}
	return nil
}

// Recover: Reconstrói o estado da memória lendo Checkpoint + WAL
// NOTA: Deve ser chamado ANTES de qualquer operação concorrente no engine.
// Durante o recovery, assume acesso exclusivo (startup).
func (se *StorageEngine) Recover(walPath string) error {
	var maxLSN uint64 // Rastreador do maior LSN visto (Checkpoint ou WAL)

	// 1. Tenta carregar Checkpoints
	// Iteramos sobre todas as tabelas conhecidas e tentamos restaurar
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		// Durante startup, podemos acessar diretamente
		for _, idx := range table.GetIndices() {
			tree, lastLSN, err := se.Checkpoint.LoadLatestCheckpoint(tableName, idx.Name)
			if err == nil {
				// Sucesso no load, substitui a árvore em memória
				idx.Tree = tree
				fmt.Printf("Recovered table '%s' index '%s' from Checkpoint (LSN %d)\n", tableName, idx.Name, lastLSN)

				if lastLSN > maxLSN {
					maxLSN = lastLSN
				}
			} else if !os.IsNotExist(err) {
				// Erro real (não apenas arquivo faltando)
				return fmt.Errorf("failed to load checkpoint for %s.%s: %w", tableName, idx.Name, err)
			}
		}
	}

	checkpointLSN := maxLSN // Define o ponto de corte: tudo <= a isso já está no checkpoint

	// 2. Tenta ler WAL para aplicar o delta
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		// Sem WAL, apenas atualiza o tracker com o maxLSN do checkpoint
		se.lsnTracker.Set(maxLSN)
		return nil
	}

	reader, err := wal.NewWALReader(walPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	count := 0
	skipped := 0

	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Em produção: Talvez ignorar últimas entradas corrompidas ou truncar arquivo
			return fmt.Errorf("recovery error at entry %d: %w", count, err)
		}

		// Se essa entrada já está no checkpoint, pula
		if entry.Header.LSN <= checkpointLSN {
			skipped++
			wal.ReleaseEntry(entry)
			continue
		}

		// Replay
		tableName, indexName, key, docBytes, err := DeserializeDocumentEntry(entry.Payload)
		if err != nil {
			wal.ReleaseEntry(entry)
			return fmt.Errorf("deserialize failed at entry %d: %w", count, err)
		}

		// Encontra tabela e índice (durante startup, acesso direto)
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			wal.ReleaseEntry(entry)
			continue
		}
		index, err := table.GetIndex(indexName)
		if err != nil {
			wal.ReleaseEntry(entry)
			continue
		}

		switch entry.Header.EntryType {
		case wal.EntryInsert, wal.EntryUpdate:
			// No recovery com Checkpoint, o HeapFile ainda pode ter dados duplicados
			// se reiniciarmos e inserirmos coisas que o WAL tem mas o Heap nao tinha?
			// Se o dado JÁ EXISTE no HeapFile, nós vamos escrever de novo e pegar um novo offset.
			// É ineficiente mas correto.
			// No Recovery, a chain pode ficar quebrada se não reconstruirmos os links?
			// O WAL contém a sequencia de updates.
			// Se Insert K (LSN 1) -> Update K (LSN 2).
			// Replay LSN 1: Write Heap, Insert Tree (Offset A).
			// Replay LSN 2: Search Tree (Get Offset A), Write Heap (Prev=A), Update Tree (Offset B).
			// Sim, a lógica do Put normal (Search -> Write -> Insert) deve funcionar aqui também
			// desde que o índice esteja sendo atualizado passo a passo.

			var prevOffset int64 = -1
			// Check tree for previous version
			node, found := index.Tree.Search(key)
			if found {
				_, idx := node.FindLeafLowerBound(key)
				if idx < node.N && node.Keys[idx].Compare(key) == 0 {
					prevOffset = node.DataPtrs[idx]
				}
			}

			offset, err := se.Heap.Write(docBytes, entry.Header.LSN, prevOffset) // Pass LSN from WAL
			if err != nil {
				return err
			}
			index.Tree.Insert(key, offset)

		case wal.EntryDelete:
			// Remove from tree
			leaf, idx := index.Tree.FindLeafLowerBound(key)
			if leaf != nil && idx < leaf.N && leaf.Keys[idx].Compare(key) == 0 {
				offset := leaf.DataPtrs[idx]
				// Precisamos saber o LSN do delete. O WAL Entry Header tem.
				se.Heap.Delete(offset, entry.Header.LSN)
			}
			index.Tree.Root.Remove(key)
			if index.Tree.Root.N == 0 && !index.Tree.Root.Leaf {
				index.Tree.Root = index.Tree.Root.Children[0]
			}
		}

		// Atualiza maxLSN (para o tracker)
		if entry.Header.LSN > maxLSN {
			maxLSN = entry.Header.LSN
		}

		wal.ReleaseEntry(entry)
		count++
	}

	// Atualiza o tracker global para o próximo LSN disponível
	se.lsnTracker.Set(maxLSN)

	fmt.Printf("Recovered: %d entries from WAL applied, %d skipped (already in checkpoint). Current LSN: %d\n", count, skipped, maxLSN)
	return nil
}
