package main

import (
	"bytes"
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	storagesql "github.com/bobboyms/storage-engine/pkg/sql"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func noEnv(string) string { return "" }

func buildDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := storagesql.OpenDatabaseWithOptions(ctx, dir, storagesql.OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES ('u-1', 'n')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

func TestRun_CleanDatabaseExitsZero(t *testing.T) {
	dir := buildDatabase(t)
	var out bytes.Buffer
	if code := run(context.Background(), []string{dir}, noEnv, &out); code != 0 {
		t.Fatalf("run = %d, want 0; output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "no issues found") {
		t.Fatalf("output does not report a clean database:\n%s", out.String())
	}
}

// poisonWAL appends one sentinel-LSN page-redo entry (well-formed payload) to
// a closed database's WAL.
func poisonWAL(t *testing.T, dir string) {
	t.Helper()
	writer, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = math.MaxUint64
	payload := make([]byte, 2+8+pagestore.PageSize)
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // bounded test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := writer.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned entry: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}

func TestRun_PoisonedWALExitsOne(t *testing.T) {
	dir := buildDatabase(t)
	poisonWAL(t, dir)

	var out bytes.Buffer
	if code := run(context.Background(), []string{dir}, noEnv, &out); code != 1 {
		t.Fatalf("run = %d, want 1; output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "wal_poisoned_lsn") {
		t.Fatalf("output does not mention the poisoned WAL finding:\n%s", out.String())
	}
}

func TestRun_RepairFixesPoisonedWAL(t *testing.T) {
	dir := buildDatabase(t)
	poisonWAL(t, dir)

	var out bytes.Buffer
	if code := run(context.Background(), []string{dir}, noEnv, &out); code != 1 {
		t.Fatalf("pre-repair check = %d, want 1; output:\n%s", code, out.String())
	}

	out.Reset()
	if code := run(context.Background(), []string{"-repair", dir}, noEnv, &out); code != 0 {
		t.Fatalf("repair run = %d, want 0; output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "removed 1 poisoned WAL entries") {
		t.Fatalf("repair output does not log the WAL sanitation:\n%s", out.String())
	}

	out.Reset()
	if code := run(context.Background(), []string{dir}, noEnv, &out); code != 0 {
		t.Fatalf("post-repair check = %d, want 0; output:\n%s", code, out.String())
	}
}

func TestRun_RepairOfCleanDatabaseIsNoop(t *testing.T) {
	dir := buildDatabase(t)
	var out bytes.Buffer
	if code := run(context.Background(), []string{"-repair", dir}, noEnv, &out); code != 0 {
		t.Fatalf("repair of clean db = %d, want 0; output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "nothing to repair") {
		t.Fatalf("output does not report the no-op:\n%s", out.String())
	}
}

func TestRun_BadUsageExitsTwo(t *testing.T) {
	var out bytes.Buffer
	if code := run(context.Background(), nil, noEnv, &out); code != 2 {
		t.Fatalf("run with no args = %d, want 2", code)
	}
	if code := run(context.Background(), []string{t.TempDir(), "extra"}, noEnv, &out); code != 2 {
		t.Fatalf("run with extra args = %d, want 2", code)
	}
}

func TestRun_BadMasterKeyExitsTwo(t *testing.T) {
	dir := buildDatabase(t)
	env := func(key string) string {
		if key == "DB_MASTER_KEY_HEX" {
			return "zz-not-hex"
		}
		return ""
	}
	var out bytes.Buffer
	if code := run(context.Background(), []string{dir}, env, &out); code != 2 {
		t.Fatalf("run with invalid master key = %d, want 2; output:\n%s", code, out.String())
	}
}
