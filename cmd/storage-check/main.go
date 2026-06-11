// storage-check runs the read-only integrity verifier ("scrub") over a SQL
// database directory and reports every invariant violation it finds.
//
// Usage:
//
//	storage-check <database-dir>
//
// For a TDE-encrypted database, export the 32-byte master key as hex in
// DB_MASTER_KEY_HEX.
//
// The database must not be in use: the verifier takes the same exclusive
// directory lock as a live database. Run it against a stopped instance, a
// backup, or a copy.
//
// Exit codes: 0 = no errors (warnings allowed), 1 = integrity errors found,
// 2 = could not run (usage, lock held, unreadable directory).
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	storagesql "github.com/bobboyms/storage-engine/pkg/sql"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Getenv, os.Stdout))
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprintln(out, "usage: storage-check <database-dir>  (set DB_MASTER_KEY_HEX for TDE databases)")
		return 2
	}
	dir := args[0]

	opts := storagesql.VerifyOptions{}
	if keyHex := getenv("DB_MASTER_KEY_HEX"); keyHex != "" {
		key, err := hex.DecodeString(keyHex)
		if err != nil {
			_, _ = fmt.Fprintf(out, "storage-check: DB_MASTER_KEY_HEX is not valid hex: %v\n", err)
			return 2
		}
		opts.Encryption = &storagesql.EncryptionOptions{MasterKey: key}
	}

	report, err := storagesql.VerifyDir(ctx, dir, opts)
	if err != nil {
		_, _ = fmt.Fprintf(out, "storage-check: %v\n", err)
		return 2
	}

	for _, finding := range report.Findings {
		_, _ = fmt.Fprintln(out, finding.String())
	}
	_, _ = fmt.Fprintf(out, "checked %d tables, %d indexes, %d live rows: ",
		report.Tables, report.Indexes, report.LiveRows)
	switch {
	case report.HasErrors():
		_, _ = fmt.Fprintf(out, "%d findings (with errors)\n", len(report.Findings))
		return 1
	case len(report.Findings) > 0:
		_, _ = fmt.Fprintf(out, "%d warnings\n", len(report.Findings))
		return 0
	default:
		_, _ = fmt.Fprintln(out, "no issues found")
		return 0
	}
}
