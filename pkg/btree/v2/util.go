package v2

import "encoding/binary"

// Helpers pra encoding little-endian — reduzem ruído visual nos
// encode/decode de headers.

func binEncU16(buf []byte, v uint16) { binary.LittleEndian.PutUint16(buf, v) }
func binEncU32(buf []byte, v uint32) { binary.LittleEndian.PutUint32(buf, v) }
func binEncU64(buf []byte, v uint64) { binary.LittleEndian.PutUint64(buf, v) }
func binDecU16(buf []byte) uint16    { return binary.LittleEndian.Uint16(buf) }
func binDecU32(buf []byte) uint32    { return binary.LittleEndian.Uint32(buf) }
func binDecU64(buf []byte) uint64    { return binary.LittleEndian.Uint64(buf) }
