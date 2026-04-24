package storage

import (
	goerrors "errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"github.com/google/uuid"
)

// isChainEndErr retorna true se err indica que o slot/record foi
// reclamado por vacuum — caminhando a chain, devemos tratar como fim
// (não como erro real de I/O).
func isChainEndErr(err error) bool {
	return goerrors.Is(err, v2.ErrVacuumed)
}

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
	lsnTracker    *LSNTracker
	txIDCounter   uint64
	appliedLSN    *AppliedLSNTracker
	TxRegistry    *TransactionRegistry
	metaMu        sync.RWMutex // Lock apenas para operações de metadados (ListTables, etc)
	// Nota: Lock por tabela agora está em Table.mu
}

// NewProductionStorageEngine é o construtor recomendado pra uso em produção.
//
// Comportamento:
//  1. Exige walWriter != nil (sem WAL não há durabilidade).
//  2. Faz auto-recovery: replay idempotente do WAL sincronizando tree+heap
//     com o estado commitado antes de devolver o engine. Transações que
//     o Put retornou como bem-sucedido são visíveis após crash.
//  3. Avança lsnTracker pro max LSN do WAL automaticamente.
//
// Custo: abrir o engine em produção pode levar O(N) no tamanho do WAL
// pra replay. Pra bases grandes, Fase 8 (fuzzy checkpoint) reduz isso.
//
// Pra testes/memory-only (WAL=nil), use NewStorageEngine diretamente.
func NewProductionStorageEngine(tableMetaData *TableMetaData, walWriter *wal.WALWriter) (*StorageEngine, error) {
	if walWriter == nil {
		return nil, fmt.Errorf("storage: NewProductionStorageEngine exige walWriter não-nil (sem WAL não há durabilidade)")
	}

	se, err := NewStorageEngine(tableMetaData, walWriter)
	if err != nil {
		return nil, err
	}

	// Replay idempotente. Se o WAL está vazio (setup inicial), é no-op.
	if err := se.Recover(walWriter.Path()); err != nil {
		return nil, fmt.Errorf("storage: recovery falhou: %w", err)
	}
	return se, nil
}

func NewStorageEngine(tableMetaData *TableMetaData, walWriter *wal.WALWriter) (*StorageEngine, error) {
	// Ao abrir o engine com um WAL já populado (reopen), precisamos
	// avançar o lsnTracker para o maior LSN registrado. Sem isso,
	// transações novas começam com SnapshotLSN=0 e não enxergam registros
	// persistidos (CreateLSN >= 1) — o record path finge que "sumiu".
	//
	// Só fazemos o SCAN do WAL aqui (leve, O(entries), sem replay).
	// O rebuild efetivo continua em Recover().
	initialLSN := uint64(0)
	if walWriter != nil {
		maxLSN, err := scanMaxWALLSN(walWriter.Path(), walWriter.Cipher())
		if err != nil {
			return nil, fmt.Errorf("storage: falha ao sincronizar LSN do WAL: %w", err)
		}
		initialLSN = maxLSN
	}

	return &StorageEngine{
		TableMetaData: tableMetaData,
		WAL:           walWriter,
		lsnTracker:    NewLSNTracker(initialLSN),
		txIDCounter:   initialLSN,
		appliedLSN:    NewAppliedLSNTracker(),
		TxRegistry:    NewTransactionRegistry(),
	}, nil
}

func (se *StorageEngine) nextTxID() uint64 {
	return atomic.AddUint64(&se.txIDCounter, 1)
}

// scanMaxWALLSN lê o WAL em `path` procurando o maior LSN. Leve e
// independente de Recover (que faz replay completo). Arquivo inexistente
// ou vazio → retorna 0 sem erro.
func scanMaxWALLSN(path string, cipher crypto.Cipher) (uint64, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	reader, err := wal.NewWALReaderWithCipher(path, cipher)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var maxLSN uint64
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			// WAL truncado no fim (crash mid-write) é esperado —
			// paramos sem erro, tendo lido até onde foi possível.
			break
		}
		if entry.Header.LSN > maxLSN {
			maxLSN = entry.Header.LSN
		}
		wal.ReleaseEntry(entry)
	}
	return maxLSN, nil
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

	// Fecha as trees do runtime page-based.
	closedTrees := make(map[btree.Tree]bool)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, _ := se.TableMetaData.GetTableByName(tableName)
		if table == nil {
			continue
		}
		for _, idx := range table.GetIndices() {
			if idx.Tree != nil && !closedTrees[idx.Tree] {
				if tErr := idx.Tree.Close(); tErr != nil {
					if err == nil {
						err = tErr
					} else {
						err = fmt.Errorf("%v; tree close error: %v", err, tErr)
					}
				}
				closedTrees[idx.Tree] = true
			}
		}
	}

	// Fecha heaps de todas as tabelas
	closedHeaps := make(map[heap.Heap]bool)
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

func (se *StorageEngine) readVisibleValue(tx *Transaction, table *Table, key types.Comparable, currentOffset int64) (string, bool, error) {
	for currentOffset != -1 {
		docBytes, header, err := table.Heap.Read(currentOffset)
		if isChainEndErr(err) {
			return "", false, nil
		}
		if err != nil {
			return "", false, fmt.Errorf("heap read failed at key %v: %w", key, err)
		}

		if tx.IsVisible(header.CreateLSN) {
			isVisibleVersion := header.Valid || (header.DeleteLSN > tx.SnapshotLSN)
			if !isVisibleVersion {
				return "", false, nil
			}

			jsonStr, err := BsonToJson(docBytes)
			if err == nil {
				return jsonStr, true, nil
			}
			return string(docBytes), true, nil
		}
		currentOffset = header.PrevRecordID
	}

	return "", false, nil
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

	se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)

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
	currentOffset, found, err := index.Tree.Get(key)
	if err != nil {
		return "", false, fmt.Errorf("tree get: %w", err)
	}
	if !found {
		return "", false, nil
	}

	// Version Chain Traversal (Time Travel)
	for currentOffset != -1 {
		docBytes, header, err := table.Heap.Read(currentOffset)
		if isChainEndErr(err) {
			// Record foi vacuumado — chain termina aqui. Não é erro.
			return "", false, nil
		}
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
		currentOffset = header.PrevRecordID
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
	if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
		var scanErr error
		visit := func(key types.Comparable, currentOffset int64) error {
			if condition != nil && !condition.Matches(key) {
				return nil
			}

			visibleVal, foundVisible, err := se.readVisibleValue(tx, table, key, currentOffset)
			if err != nil {
				return err
			}
			if foundVisible {
				results = append(results, visibleVal)
			}
			return nil
		}

		if condition != nil {
			switch condition.Operator {
			case query.OpEqual:
				scanErr = treeV2.Scan(condition.Value, condition.Value, visit)
			case query.OpBetween:
				scanErr = treeV2.Scan(condition.Value, condition.ValueEnd, visit)
			default:
				scanErr = treeV2.ScanAll(visit)
			}
		} else {
			scanErr = treeV2.ScanAll(visit)
		}
		return results, scanErr
	}

	return results, fmt.Errorf("Scan: índice %s usa tipo não suportado %T", indexName, index.Tree)
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
			if _, found, _ := index.Tree.Get(key); found {
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
		se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)
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
			if isChainEndErr(err) {
				return oldOffset, nil
			}
			return 0, fmt.Errorf("heap delete failed: %w", err)
		}
		wasFound = true

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

	if wasFound {
		se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)
	}

	return wasFound, nil
}

// CreateCheckpoint agora faz flush durável do estado page-based.
// O formato `.chk` legado não é mais usado pelo runtime do engine.
func (se *StorageEngine) CreateCheckpoint() error {
	if se.WAL != nil {
		if err := se.WAL.Sync(); err != nil {
			return err
		}
	}

	syncedTrees := make(map[btree.Tree]bool)
	syncedHeaps := make(map[heap.Heap]bool)

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		for _, idx := range table.GetIndices() {
			if idx.Tree != nil && !syncedTrees[idx.Tree] {
				if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
					if err := treeV2.Sync(); err != nil {
						return err
					}
				}
				syncedTrees[idx.Tree] = true
			}
		}

		if table.Heap != nil && !syncedHeaps[table.Heap] {
			if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
				if err := heapV2.Sync(); err != nil {
					return err
				}
			}
			syncedHeaps[table.Heap] = true
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

// Recover: reconstrói o estado a partir do WAL.
// NOTA: Deve ser chamado ANTES de qualquer operação concorrente no engine.
// Durante o recovery, assume acesso exclusivo (startup).
func (se *StorageEngine) Recover(walPath string) error {
	return se.RecoverWithCipher(walPath, se.walCipher())
}

// RecoverWithCipher reconstrói o estado a partir de um WAL cifrado ou em claro.
// Use diretamente apenas quando o WALWriter do engine não está disponível.
func (se *StorageEngine) RecoverWithCipher(walPath string, cipher crypto.Cipher) error {
	var maxLSN uint64
	loadedLSNs := make(map[string]uint64)

	analysis, err := se.analyzeRecoveryWithCipher(walPath, cipher)
	if err != nil {
		return err
	}
	if analysis.MaxLSN > maxLSN {
		maxLSN = analysis.MaxLSN
	}

	// 1. Redo scan-only: relê o WAL inteiro, mas reaplica apenas
	// operações autocommit ou pertencentes a transações commitadas.
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		se.lsnTracker.Set(maxLSN)
		return nil
	}

	reader, err := wal.NewWALReaderWithCipher(walPath, cipher)
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
			if isExpectedWALTail(err) {
				break
			}
			return fmt.Errorf("recovery error at entry %d: %w", count, err)
		}

		// Atualiza maxLSN visto
		if entry.Header.LSN > maxLSN {
			maxLSN = entry.Header.LSN
		}

		payload, shouldRedo, err := analysis.shouldRedo(entry)
		if err != nil {
			wal.ReleaseEntry(entry)
			return fmt.Errorf("redo classification failed at entry %d: %w", count, err)
		}
		if !shouldRedo {
			skipped++
			wal.ReleaseEntry(entry)
			count++
			continue
		}

		switch entry.Header.EntryType {
		case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete:
			if err := se.redoDocumentEntry(entry, payload, loadedLSNs); err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("redo document failed at entry %d: %w", count, err)
			}
		case wal.EntryMultiInsert:
			if err := se.redoMultiInsertEntry(entry, payload, loadedLSNs); err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("redo multi-insert failed at entry %d: %w", count, err)
			}
		default:
			skipped++
		}

		wal.ReleaseEntry(entry)
		count++
	}

	// 2. Undo-lite: loser txs nunca chegaram ao estado visível porque o
	// write path só aplica heap/tree após COMMIT durável.
	se.undoLoserTransactions(analysis)

	se.lsnTracker.Set(maxLSN)
	atomic.StoreUint64(&se.txIDCounter, maxLSN)
	if analysis.CheckpointLSN > 0 {
		fmt.Printf("Recovered: %d entries applied, %d skipped (checkpoint LSN=%d → redo start). Current LSN: %d\n",
			count, skipped, analysis.CheckpointLSN, maxLSN)
	} else {
		fmt.Printf("Recovered: %d entries from WAL applied, %d skipped. Current LSN: %d\n", count, skipped, maxLSN)
	}
	return nil
}

func (se *StorageEngine) walCipher() crypto.Cipher {
	if se == nil || se.WAL == nil {
		return nil
	}
	return se.WAL.Cipher()
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

	// 3. Dispatch para a implementação atual: compactação in-place,
	// sem reescrever o B+ tree. Slots vacuumados viram length=0;
	// leituras caem em ErrVacuumed (tratado como fim de chain no
	// engine.Get).
	if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
		n, err := heapV2.Vacuum(minLSN)
		if err != nil {
			return fmt.Errorf("Vacuum v2 failed for table %s: %w", tableName, err)
		}
		fmt.Printf("Vacuum v2 completed for table %s: %d records reclaimed\n", tableName, n)
		return nil
	}

	return fmt.Errorf("Vacuum: heap legado removido; tabela %s deve usar HeapV2", tableName)
}
