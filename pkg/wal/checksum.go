package wal

import "hash/crc32"

// Tabela CRC32 Castagnoli (mais eficiente em hardware moderno)
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// CalculateCRC32 calcula o checksum dos dados
func CalculateCRC32(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// ValidateCRC32 verifica se os dados correspondem ao checksum esperado
func ValidateCRC32(data []byte, expected uint32) bool {
	return CalculateCRC32(data) == expected
}
