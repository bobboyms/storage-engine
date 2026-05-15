package storage

import (
	"fmt"
	"os"
)

const ariesTestPageSize = 8192

func readPageFile(path string, pageID uint64, buf []byte) error {
	if len(buf) != ariesTestPageSize {
		return fmt.Errorf("buf must be %d bytes", ariesTestPageSize)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	off := int64(pageID) * int64(ariesTestPageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		return err
	}
	return nil
}

func writePageFile(path string, pageID uint64, data []byte) error {
	if len(data) != ariesTestPageSize {
		return fmt.Errorf("data must be %d bytes", ariesTestPageSize)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	off := int64(pageID) * int64(ariesTestPageSize)
	if _, err := f.WriteAt(data, off); err != nil {
		return err
	}
	return f.Sync()
}
