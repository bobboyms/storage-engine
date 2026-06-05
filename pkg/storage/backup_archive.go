package storage

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ArchiveBackup packs a backup directory (the manifest plus its files) into a
// single gzip-compressed tar archive at archivePath, making a backup portable
// as one file. The directory layout is preserved, so ExtractBackup reproduces a
// directory that VerifyBackup and RestoreBackup accept unchanged.
func ArchiveBackup(backupDir, archivePath string) error {
	if _, err := os.Stat(filepath.Join(backupDir, backupManifestName)); err != nil {
		return fmt.Errorf("backup: %q is not a backup directory: %w", backupDir, err)
	}

	// Open backupDir as a root so file reads are scoped to it: a symlink inside
	// the directory cannot redirect a read outside it (avoids TOCTOU traversal).
	root, err := os.OpenRoot(backupDir)
	if err != nil {
		return fmt.Errorf("backup: open backup dir: %w", err)
	}
	defer func() { _ = root.Close() }()

	out, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("backup: create archive: %w", err)
	}
	defer func() { _ = out.Close() }()

	gz := gzip.NewWriter(out)
	tw := tar.NewWriter(gz)

	walkErr := filepath.Walk(backupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(backupDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := root.Open(rel)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.Copy(tw, f)
		return err
	})
	if walkErr != nil {
		return fmt.Errorf("backup: archive backup: %w", walkErr)
	}

	if err := tw.Close(); err != nil {
		return fmt.Errorf("backup: finalize tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("backup: finalize gzip: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("backup: sync archive: %w", err)
	}
	return nil
}

// ExtractBackup unpacks an archive produced by ArchiveBackup into destDir,
// reproducing the backup directory. Entry paths are validated so a crafted
// archive cannot write outside destDir (path traversal).
func ExtractBackup(archivePath, destDir string) error {
	in, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("backup: open archive: %w", err)
	}
	defer func() { _ = in.Close() }()

	gz, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("backup: open gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	if err := os.MkdirAll(destDir, 0o700); err != nil {
		return fmt.Errorf("backup: create dest: %w", err)
	}
	cleanDest := filepath.Clean(destDir)

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("backup: read tar: %w", err)
		}

		target, err := safeJoin(cleanDest, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			if err := writeExtractedFile(target, tr); err != nil {
				return err
			}
		default:
			return fmt.Errorf("backup: unsupported archive entry %q (type %d)", hdr.Name, hdr.Typeflag)
		}
	}
	return nil
}

// safeJoin joins a cleaned destination with an archive entry name, rejecting any
// name that escapes the destination directory.
func safeJoin(cleanDest, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("backup: archive entry has absolute path %q", name)
	}
	target := filepath.Join(cleanDest, filepath.FromSlash(name))
	if target != cleanDest && !strings.HasPrefix(target, cleanDest+string(os.PathSeparator)) {
		return "", fmt.Errorf("backup: archive entry %q escapes destination", name)
	}
	return target, nil
}

func writeExtractedFile(target string, r io.Reader) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, r); err != nil { //nolint:gosec // size bounded by trusted backups; traversal already blocked
		return err
	}
	return nil
}
