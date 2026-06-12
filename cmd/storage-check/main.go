// storage-check runs the read-only integrity verifier ("scrub") over a SQL
// database directory and reports every invariant violation it finds. With
// -repair it additionally applies the offline repairs that have an
// unambiguous source of truth (fsck): poisoned WAL entries are removed,
// stamped page LSNs healed, and broken or missing indexes rebuilt from the
// heap. Take a backup before repairing — files are rewritten in place.
//
// Usage:
//
//	storage-check [-repair] <database-dir>
//
// For a TDE-encrypted database, export the 32-byte master key as hex in
// DB_MASTER_KEY_HEX.
//
// The database must not be in use: the verifier takes the same exclusive
// directory lock as a live database. Run it against a stopped instance, a
// backup, or a copy.
//
// Exit codes: 0 = no errors (warnings allowed; after repair when -repair),
// 1 = integrity errors found (or remaining after repair), 2 = could not run
// (usage, lock held, unreadable directory).
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
	repair := false
	if len(args) > 0 && args[0] == "-repair" {
		repair = true
		args = args[1:]
	}
	if len(args) != 1 {
		_, _ = fmt.Fprintln(out, "usage: storage-check [-repair] <database-dir>  (set DB_MASTER_KEY_HEX for TDE databases)")
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

	if repair {
		return runRepair(ctx, dir, opts, out)
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

// runRepair drives the offline repair (fsck) and reports what changed.
func runRepair(ctx context.Context, dir string, opts storagesql.VerifyOptions, out io.Writer) int {
	report, err := storagesql.RepairDir(ctx, dir, opts)
	if err != nil {
		_, _ = fmt.Fprintf(out, "storage-check: %v\n", err)
		return 2
	}

	if !report.Before.HasErrors() {
		_, _ = fmt.Fprintln(out, "nothing to repair: no integrity errors found")
		return 0
	}
	for _, finding := range report.Before.Findings {
		_, _ = fmt.Fprintf(out, "before: %s\n", finding)
	}
	for _, action := range report.Actions {
		_, _ = fmt.Fprintf(out, "repair: %s\n", action)
	}
	for _, finding := range report.After.Findings {
		_, _ = fmt.Fprintf(out, "after: %s\n", finding)
	}
	if report.After.HasErrors() {
		_, _ = fmt.Fprintln(out, "repair incomplete: errors remain (see findings above)")
		return 1
	}
	_, _ = fmt.Fprintln(out, "repair complete: database passes verification")
	return 0
}
