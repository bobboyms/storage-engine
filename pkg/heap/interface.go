package heap

// Heap é a abstração do heap page-based usado pelo engine.
//
// Os RecordIDs (int64) retornados por Write são opacos. O sentinela -1
// significa "sem versão anterior".
type Heap interface {
	// Write grava um documento e devolve seu RecordID estável.
	// `prevRecordID` é -1 para a primeira versão ou o RecordID da
	// versão anterior numa cadeia MVCC.
	Write(doc []byte, createLSN uint64, prevRecordID int64) (int64, error)

	// Read devolve o documento e o header do record identificado
	// por `recordID`. O header é retornado mesmo se Valid=false —
	// transações antigas precisam ler versões deleted.
	Read(recordID int64) ([]byte, *RecordHeader, error)

	// Delete marca o record como invalid (lazy delete do MVCC).
	// Bytes do doc e CreateLSN/PrevRecordID são preservados.
	Delete(recordID int64, deleteLSN uint64) error

	// Close libera recursos.
	Close() error

	// Path devolve o caminho do arquivo subjacente (útil pra diagnóstico).
	Path() string
}
