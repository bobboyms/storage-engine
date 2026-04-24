package pagestore

import "hash/crc32"

func crc32Sum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}
