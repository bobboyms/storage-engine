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
	"go.mongodb.org/mongo-driver/v2/bson"
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
	Checkpoint    *CheckpointManager
	lsnTracker    *LSNTracker
	TxRegistry    *TransactionRegistry
	metaMu        sync.RWMutex // Lock apenas para operações de metadados (ListTables, etc)
	// Nota: Lock por tabela agora está em Table.mu
}

func NewStorageEngine(tableMetaData *TableMetaData, walWriter *wal.WALWriter) (*StorageEngine, error) {
	// Configuração do Checkpoint Manager
	// Por padrão, salva checkpoints no mesmo diretório do WAL (se existir) ou no diretório atual
	var checkpointDir string
	if walWriter != nil {
		checkpointDir = filepath.Dir(walWriter.Path())
	} else {
		checkpointDir = "." // Fallback for memory-only mode
	}

	checkpointMgr := NewCheckpointManager(checkpointDir)

	return &StorageEngine{
		TableMetaData: tableMetaData,
		WAL:           walWriter,
		Checkpoint:    checkpointMgr,
		lsnTracker:    NewLSNTracker(0),
		TxRegistry:    NewTransactionRegistry(),
	}, nil
}

// IsolationLevel define o nível de isolamento da transação
type IsolationLevel int

const (
	ReadCommitted  IsolationLevel = iota // Leituras veem dados commitados recentemente
	RepeatableRead                       // Snapshot Isolation (Padrão)
)

// Transaction representa um contexto de execução com Snapshot Isolation
type Transaction struct {
	SnapshotLSN uint64
	Level       IsolationLevel
	engine      *StorageEngine
}

// BeginTransaction inicia uma transação com o nível de isolamento especificado
func (se *StorageEngine) BeginTransaction(level IsolationLevel) *Transaction {
	tx := &Transaction{
		SnapshotLSN: se.lsnTracker.Current(), // Captura o "agora" linearizável
		Level:       level,
		engine:      se,
	}
	se.TxRegistry.Register(tx)
	return tx
}

// Close marks the transaction as finished and unregisters it
func (tx *Transaction) Close() {
	tx.engine.TxRegistry.Unregister(tx)
}

// BeginRead inicia uma transação de leitura (Snapshot) com o padrão Repeatable Read
func (se *StorageEngine) BeginRead() *Transaction {
	return se.BeginTransaction(RepeatableRead)
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
	// TODO: Clean up TxRegistry? Not strictly needed as Engine is closing.

	// Fecha heaps de todas as tabelas
	closedHeaps := make(map[*heap.HeapManager]bool)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, _ := se.TableMetaData.GetTableByName(tableName)
		if table != nil && table.Heap != nil && !closedHeaps[table.Heap] {
			if hErr := table.Heap.Close(); hErr != nil {
				if err == nil {
					err = hErr
				} else {
					err = fmt.Errorf("%v; heap close error: %v", err, hErr)
				}
			}
			closedHeaps[table.Heap] = true
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

	// Não precisamos travessar a tabela inteira (Table RLock removido em favor de concurrency granular)
	// se.TableMetaData já proteje o acesso ao mapa de tabelas.

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

	// 2 ~ 4. Atomic Upsert (Write Heap -> Update Tree)
	// Usamos Upsert para garantir atomocidade no acesso à versão anterior e atualização do ponteiro HEAD.
	err = index.Tree.Upsert(key, func(oldOffset int64, exists bool) (int64, error) {
		var prevOffset int64 = -1
		if exists {
			prevOffset = oldOffset
		}

		// Write to Heap (dentro do Lock da folha - safe mas aumenta latência do lock)
		// TODO: Otimização futura - Se heap write for lento, refatorar.
		// Mas como é append-only bufio, deve ser rápido.
		offset, err := table.Heap.Write(bsonData, currentLSN, prevOffset)
		if err != nil {
			return 0, fmt.Errorf("heap write failed: %w", err)
		}

		return offset, nil
	})

	if err != nil {
		return err
	}

	return nil
}

// Get executa uma busca no contexto da transação (Snapshot Isolation)
func (tx *Transaction) Get(tableName string, indexName string, key types.Comparable) (string, bool, error) {
	// Se Read Committed, atualiza o snapshot antes de começar
	tx.refreshSnapshot()

	se := tx.engine

	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return "", false, err
	}

	// Lock-Free Read: Não travamos a tabela. Usamos latching interno da árvore.

	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return "", false, err
	}

	// Busca na árvore thread-safe
	currentOffset, found := index.Tree.Get(key)
	if !found {
		return "", false, nil
	}

	// Version Chain Traversal (Time Travel)
	for currentOffset != -1 {
		docBytes, header, err := table.Heap.Read(currentOffset)
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

// Get wrapper para conveniência (Autocommit / Snapshot instantâneo)
func (se *StorageEngine) Get(tableName string, indexName string, key types.Comparable) (string, bool, error) {
	tx := se.BeginRead()
	defer tx.Close() // Autocommit: Release transaction registration
	return tx.Get(tableName, indexName, key)
}

// Scan executa uma busca por range no contexto da transação
func (tx *Transaction) Scan(tableName string, indexName string, condition *query.ScanCondition) ([]string, error) {
	// Se Read Committed, atualiza snapshot
	tx.refreshSnapshot()

	se := tx.engine

	// Obtém a tabela primeiro (sem lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}

	// Lock-Free Scan: Cursor thread-safe cuida dos locks de folha

	results := []string{}
	// Obtém o índice (já temos o lock da tabela)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return results, err
	}
	c := se.Cursor(index.Tree)
	defer c.Close() // Libera cursor

	// Otimiza scan se possível (=, >, >=, BETWEEN)
	if condition != nil && condition.ShouldSeek() {
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
					docBytes, header, err := table.Heap.Read(currentOffset)
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
		c.Seek(nil)

		for c.Valid() {
			key := c.Key()

			// Para < e <=, podemos parar cedo
			if condition != nil && !condition.ShouldContinue(key) {
				break
			}

			if condition == nil || condition.Matches(key) {
				currentOffset := c.Value()

				// Version Chain Traversal
				foundVisible := false
				var visibleVal string

				for currentOffset != -1 {
					docBytes, header, err := table.Heap.Read(currentOffset)
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

// InsertRow: Insere um documento e atualiza múltiplos índices atomicamente (evita duplicação no heap)
func (se *StorageEngine) InsertRow(tableName string, doc string, keys map[string]types.Comparable) error {
	// 1. Validação básica de metadados
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}

	// Try convert json to bson for validation
	bsonDoc, err := JsonToBson(doc)
	var bsonData []byte
	if err == nil {
		// Validar cada chave em seu respectivo índice
		for indexName := range keys {
			index, err := table.GetIndex(indexName)
			if err != nil {
				return err
			}
			exists, keyType := DoesTheKeyExist(bsonDoc, indexName)
			if !exists {
				return &errors.IndexNotFoundError{Name: indexName}
			}
			if keyType != index.Type {
				return &errors.InvalidKeyTypeError{
					Name:     indexName,
					TypeName: keyType.String(),
				}
			}
		}
		bsonData, _ = MarshalBson(bsonDoc)
	} else {
		bsonData = []byte(doc)
	}

	// 1.5 Constraint Check: Primary keys must be unique
	for indexName, key := range keys {
		index, err := table.GetIndex(indexName)
		if err == nil && index.Primary {
			if _, found := index.Tree.Get(key); found {
				return fmt.Errorf("duplicate key error: key %v already exists in index %s", key, indexName)
			}
		}
	}

	currentLSN := se.lsnTracker.Next()

	// 2. Write Ahead Log (UMA entrada para todos os índices)
	if se.WAL != nil {
		payload, err := SerializeMultiIndexEntry(tableName, keys, bsonData)
		if err != nil {
			return err
		}

		entry := wal.AcquireEntry()
		entry.Header.Magic = wal.WALMagic
		entry.Header.Version = 1
		entry.Header.EntryType = wal.EntryMultiInsert
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

	// 3. Write to Heap (UMA VEZ)
	offset, err := table.Heap.Write(bsonData, currentLSN, -1) // Novas linhas começam com PrevOffset -1
	if err != nil {
		return fmt.Errorf("heap write failed: %w", err)
	}

	// 4. Update Trees
	for indexName, key := range keys {
		index, _ := table.GetIndex(indexName)
		// No caso de InsertRow, tratamos como um Replace se já existir,
		// ou Insert normal se não existir. B+Tree.Replace já faz isso de forma safe.
		if err := index.Tree.Replace(key, offset); err != nil {
			return fmt.Errorf("failed to update index %s: %w", indexName, err)
		}
	}

	return nil
}

// Scan wrapper para conveniência
func (se *StorageEngine) Scan(tableName string, indexName string, condition *query.ScanCondition) ([]string, error) {
	tx := se.BeginRead()
	defer tx.Close()
	return tx.Scan(tableName, indexName, condition)
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

	// Sem Table Lock. Upsert cuida disso.

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
	// Usa Upsert para remover logicamente (ou manter apontando para Tombstone)
	// Precisamos escrever o Tombstone no Heap e atualizar a árvore para apontar para ele.
	// O Delete atual apenas marca no Heap, e NÃO remove da árvore (conforme comentários comentados abaixo).
	// Mas precisamos atualizar o ponteiro na árvore para o novo registro no Heap (que diz "Deleted").

	var wasFound bool
	err = index.Tree.Upsert(key, func(oldOffset int64, exists bool) (int64, error) {
		if !exists {
			return 0, nil // Key not found, nothing to delete
		}
		wasFound = true

		// Escreve registro de Delete no Heap (Tombstone)
		// Delete no Heap requer o offset antigo? O método Heap.Delete atual pede offset.
		// Wait, Heap.Delete(offset) marca o registro OLD como deletado?
		// Engine.go original:
		// offset := node.DataPtrs[idx]
		// se.Heap.Delete(offset, currentLSN) -> Modifica in-place o header do registro antigo?
		// Se Heap.Delete modifica in-place, então não criamos nova versão?
		// Isso viola imutabilidade do WAL/AppendOnly.
		// O comentário dizia: "Para Phase 2 simplificado: Update in-place Head com DeleteLSN."
		// Se for in-place, não precisamos atualizar a árvore (ela aponta pro mesmo offset).
		// ENTRETANTO,		// Para concurrency correta, precisamos lockar o nó enquanto lemos o offset e chamamos heap.Delete.

		if err := table.Heap.Delete(oldOffset, currentLSN); err != nil {
			return 0, fmt.Errorf("heap delete failed: %w", err)
		}

		// Retorna o MESMO offset, pois a árvore não muda (aponta pro mesmo lugar, que agora está marcado deletado)
		return oldOffset, nil
	})

	if err != nil {
		return false, err
	}

	// MVCC Phase 2: Do NOT remove from B-Tree.
	// We need to keep the key pointing to the "Deleted" record (Tombstone)
	// so that older transactions can check visibility (DeleteLSN) and potential previous versions.
	// Garbage Collection (Vacuum) will eventually remove these when safe.
	// removed := index.Tree.Root.Remove(key)
	// if index.Tree.Root.N == 0 && !index.Tree.Root.Leaf {
	// 	index.Tree.Root = index.Tree.Root.Children[0]
	// }

	return wasFound, nil
}

// CreateCheckpoint força a criação de checkpoints para todas as tabelas
// Otimizado: Adquire lock apenas para capturar o LSN consistente,
// realizando a serialização e I/O de forma concorrente com as escritas.
func (se *StorageEngine) CreateCheckpoint() error {
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		// Barrier rápida: Captura o LSN atual.
		// Como o LSNTracker é atômico, poderíamos até fazer sem lock da tabela,
		// mas o lock garante que não estamos no meio de uma alteração de esquema (Indices).
		table.RLock()
		currentLSN := se.lsnTracker.Current()
		indices := table.GetIndicesUnsafe() // Usa versão sem lock pois já temos lock
		table.RUnlock()

		for _, idx := range indices {
			// A serialização em disco agora corre em paralelo com novos Puts.
			// O SerializeBPlusTree usa RLock nos nós (Latch Crabbing) para garantir
			// que a estrutura do arquivo seja consistente, mesmo que "fuzzy" (possa conter dados de LSNs posteriores).
			if err := se.Checkpoint.CreateCheckpoint(tableName, idx.Name, idx.Tree, currentLSN); err != nil {
				return err
			}
		}
	}
	return nil
}

// Helper to refresh snapshot for ReadCommitted
func (tx *Transaction) refreshSnapshot() {
	if tx.Level == ReadCommitted {
		tx.SnapshotLSN = tx.engine.lsnTracker.Current()
	}
}

// Recover: Reconstrói o estado da memória lendo Checkpoint + WAL
// NOTA: Deve ser chamado ANTES de qualquer operação concorrente no engine.
// Durante o recovery, assume acesso exclusivo (startup).
func (se *StorageEngine) Recover(walPath string) error {
	var maxLSN uint64                     // Rastreador do maior LSN visto globalmente
	loadedLSNs := make(map[string]uint64) // Rastreia LSN por índice: "table.index" -> LSN

	// 1. Tenta carregar Checkpoints
	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		for _, idx := range table.GetIndices() {
			tree, lastLSN, err := se.Checkpoint.LoadLatestCheckpoint(tableName, idx.Name)
			key := fmt.Sprintf("%s.%s", tableName, idx.Name)
			if err == nil {
				// Sucesso no load, substitui a árvore em memória
				idx.Tree = tree
				loadedLSNs[key] = lastLSN
				fmt.Printf("Recovered table '%s' index '%s' from Checkpoint (LSN %d)\n", tableName, idx.Name, lastLSN)

				if lastLSN > maxLSN {
					maxLSN = lastLSN
				}
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("failed to load checkpoint for %s.%s: %w", tableName, idx.Name, err)
			} else {
				loadedLSNs[key] = 0 // No checkpoint
			}
		}
	}

	// 2. Tenta ler WAL para aplicar o delta
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
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
			return fmt.Errorf("recovery error at entry %d: %w", count, err)
		}

		// Atualiza maxLSN visto
		if entry.Header.LSN > maxLSN {
			maxLSN = entry.Header.LSN
		}

		switch entry.Header.EntryType {
		case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete:
			// Replay de operação em índice único
			tableName, indexName, key, docBytes, err := DeserializeDocumentEntry(entry.Payload)
			if err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("deserialize failed at entry %d: %w", count, err)
			}

			// Verifica se já aplicamos este LSN neste índice
			lookupKey := fmt.Sprintf("%s.%s", tableName, indexName)
			if loadedLSNs[lookupKey] >= entry.Header.LSN {
				skipped++
				wal.ReleaseEntry(entry)
				continue
			}

			table, err := se.TableMetaData.GetTableByName(tableName)
			if err != nil {
				wal.ReleaseEntry(entry)
				continue // Table mismatch/deleted?
			}
			index, err := table.GetIndex(indexName)
			if err != nil {
				wal.ReleaseEntry(entry)
				continue
			}

			if entry.Header.EntryType == wal.EntryDelete {
				// Delete Logical
				leaf, idx := index.Tree.FindLeafLowerBound(key)
				if leaf != nil && idx < leaf.N && leaf.Keys[idx].Compare(key) == 0 {
					offset := leaf.DataPtrs[idx]
					table.Heap.Delete(offset, entry.Header.LSN)
				}
			} else {
				// Insert/Update
				var prevOffset int64 = -1
				node, found := index.Tree.Search(key)
				if found {
					_, idx := node.FindLeafLowerBound(key)
					if idx < node.N && node.Keys[idx].Compare(key) == 0 {
						prevOffset = node.DataPtrs[idx]
					}
				}

				offset, err := table.Heap.Write(docBytes, entry.Header.LSN, prevOffset)
				if err != nil {
					return fmt.Errorf("heap write failed: %w", err)
				}
				if err := index.Tree.Replace(key, offset); err != nil {
					return fmt.Errorf("failed to update tree during recovery: %w", err)
				}
			}

		case wal.EntryMultiInsert:
			tableName, keys, docBytes, err := DeserializeMultiIndexEntry(entry.Payload)
			if err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("deserialize multi-key failed: %w", err)
			}

			table, err := se.TableMetaData.GetTableByName(tableName)
			if err != nil {
				wal.ReleaseEntry(entry)
				continue
			}

			// Verifica se ALGUM índice precisa de update
			needsUpdate := false
			for indexName := range keys {
				lookupKey := fmt.Sprintf("%s.%s", tableName, indexName)
				if loadedLSNs[lookupKey] < entry.Header.LSN {
					needsUpdate = true
					break
				}
			}

			if !needsUpdate {
				skipped++
				wal.ReleaseEntry(entry)
				continue
			}

			// Escreve no heap (pode duplicar dados se alguns índices já tinham checkpoint, mas necessário para consistência dos novos)
			offset, err := table.Heap.Write(docBytes, entry.Header.LSN, -1)
			if err != nil {
				return fmt.Errorf("heap write failed: %w", err)
			}

			// Atualiza apenas os índices que precisam
			for indexName, key := range keys {
				lookupKey := fmt.Sprintf("%s.%s", tableName, indexName)
				// Se LSN do índice é MENOR que da entrada, ele precisa ser atualizado
				if loadedLSNs[lookupKey] < entry.Header.LSN {
					index, err := table.GetIndex(indexName)
					if err != nil {
						continue
					}
					if err := index.Tree.Replace(key, offset); err != nil {
						return fmt.Errorf("failed to update index %s during recovery: %w", indexName, err)
					}
				}
			}
		}

		wal.ReleaseEntry(entry)
		count++
	}

	se.lsnTracker.Set(maxLSN)
	fmt.Printf("Recovered: %d entries from WAL applied, %d skipped. Current LSN: %d\n", count, skipped, maxLSN)
	return nil
}

// Vacuum performs Garbage Collection on the specified table.
// It removes dead Tombstones (deleted records visible to no active transaction)
// and compacts the Heap file, reclaiming space.
func (se *StorageEngine) Vacuum(tableName string) error {
	// 1. Acquire Table Lock (Exclusive)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	table.Lock()
	defer table.Unlock()

	// 2. Determine Minimum Visible LSN
	// Any Tombstone with DeleteLSN < minLSN is safe to remove.
	minLSN := se.TxRegistry.GetMinActiveLSN()

	fmt.Printf("Starting Vacuum for table %s. MinLSN: %d\n", tableName, minLSN)

	// 3. Create New Heap (Temporary)
	oldHeap := table.Heap
	newHeapPath := oldHeap.Path() + "_vacuum"
	// Ensure cleanup of previous failed runs
	os.Remove(newHeapPath + "_001.data") // Simple cleanup for first segment

	newHeap, err := heap.NewHeapManager(newHeapPath)
	if err != nil {
		return fmt.Errorf("failed to create temp heap: %w", err)
	}

	// 4. Scan and Compact
	offsetMap := make(map[int64]int64) // Old -> New
	type treeUpdate struct {
		Index     string
		Key       types.Comparable
		NewOffset int64
	}
	var updates []treeUpdate

	iter, err := oldHeap.NewIterator()
	if err != nil {
		newHeap.Close()
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for {
		doc, header, oldOffset, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			newHeap.Close()
			return fmt.Errorf("heap iteration failed: %w", err)
		}

		// Decision Logic
		keep := true
		if !header.Valid {
			// It is a Tombstone.
			if header.DeleteLSN < minLSN {
				keep = false // Dead!
			} else {
				// Keep! Still visible to some transaction.
			}
		}

		// Extract Keys for Tree operations
		var bsonDoc bson.D
		parseErr := func() error {
			// Try BSON first
			d, err := UnmarshalBson(doc)
			if err == nil {
				bsonDoc = d
				return nil
			}
			// Try JSON
			d, err = JsonToBson(string(doc))
			if err == nil {
				bsonDoc = d
				return nil
			}
			return fmt.Errorf("failed to parse doc")
		}()

		if !keep {
			// Dead Tombstone: Remove from Tree
			if parseErr == nil {
				for _, idx := range table.GetIndicesUnsafe() {
					keyVal, err := GetValueFromBson(bsonDoc, idx.Name)
					if err == nil {
						idx.Tree.Remove(keyVal)
					}
				}
			}
			continue
		}

		// Keep: Copy to New Heap
		newPrev := int64(-1)
		if header.PrevOffset != -1 {
			if mapped, ok := offsetMap[header.PrevOffset]; ok {
				newPrev = mapped
			}
		}

		newOffset, err := newHeap.Write(doc, header.CreateLSN, newPrev)
		if err != nil {
			newHeap.Close()
			return fmt.Errorf("failed to write to new heap: %w", err)
		}

		// Restore Delete status if it was a kept Tombstone
		if !header.Valid {
			if err := newHeap.Delete(newOffset, header.DeleteLSN); err != nil {
				newHeap.Close()
				return fmt.Errorf("failed to mark deleted in new heap: %w", err)
			}
		}

		offsetMap[oldOffset] = newOffset

		// Collect Tree Update
		if parseErr == nil {
			for _, idx := range table.GetIndicesUnsafe() {
				keyVal, err := GetValueFromBson(bsonDoc, idx.Name)
				if err == nil {
					updates = append(updates, treeUpdate{
						Index:     idx.Name,
						Key:       keyVal,
						NewOffset: newOffset,
					})
				}
			}
		}
	}

	// 5. Update Trees (Batch)
	iter.Close() // Release file handles before swapping files
	for _, up := range updates {
		if idx, ok := table.Indices[up.Index]; ok {
			idx.Tree.Upsert(up.Key, func(current int64, exists bool) (int64, error) {
				return up.NewOffset, nil
			})
		}
	}

	// 6. Swap Heaps
	oldHeap.Close()
	newHeap.Close()

	oldPath := oldHeap.Path()
	// Use strict pattern to avoid matching _vacuum files (since _vacuum starts with _)
	files, _ := filepath.Glob(oldPath + "_[0-9][0-9][0-9].data")
	for _, f := range files {
		os.Remove(f)
	}

	newFiles, _ := filepath.Glob(newHeapPath + "_[0-9][0-9][0-9].data")
	for _, f := range newFiles {
		// New files: name_vacuum_XXX.data
		// Target: name_XXX.data
		// Need to strip "_vacuum" from base path part
		// newHeapPath matches oldPath + "_vacuum"
		// so f starts with oldPath + "_vacuum"
		suffix := f[len(newHeapPath):] // "_001.data"
		dest := oldPath + suffix
		if err := os.Rename(f, dest); err != nil {
			return fmt.Errorf("failed to rename vacuum file: %w", err)
		}
	}

	// Re-open
	finalHeap, err := heap.NewHeapManager(oldPath)
	if err != nil {
		return fmt.Errorf("failed to reopen heap: %w", err)
	}
	table.Heap = finalHeap

	return nil
}
