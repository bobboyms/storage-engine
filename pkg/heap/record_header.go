package heap

// RecordHeader é o metadata por registro (MVCC), compartilhado pela
// implementação page-based do heap.
//
// PrevRecordID é um int64 opaco apontando para a versão anterior.
// O sentinela -1 significa "sem versão anterior".
type RecordHeader struct {
	Valid        bool
	CreateLSN    uint64
	DeleteLSN    uint64
	PrevRecordID int64
}
