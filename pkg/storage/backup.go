package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bobboyms/storage-engine/pkg/wal"
)

const (
	backupManifestName = "manifest.json"
	backupFilesDirName = "files"
	backupManifestVer  = 1
)

type BackupManifest struct {
	Version       int          `json:"version"`
	CreatedAtUTC  time.Time    `json:"created_at_utc"`
	SourceRoot    string       `json:"source_root"`
	CheckpointLSN uint64       `json:"checkpoint_lsn"`
	Files         []BackupFile `json:"files"`
}

type BackupFile struct {
	Role   string `json:"role"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type backupSourceFile struct {
	role string
	path string
}

type pathProvider interface {
	Path() string
}

// BackupOnline cria um snapshot consistente do engine com ele aberto.
// Escritas ficam pausadas durante o checkpoint/cópia; leituras continuam.
func (se *StorageEngine) BackupOnline(backupDir string) (*BackupManifest, error) {
	if backupDir == "" {
		return nil, fmt.Errorf("backup: backupDir vazio")
	}

	if err := prepareEmptyBackupDir(backupDir); err != nil {
		return nil, err
	}

	se.opMu.Lock()
	defer se.opMu.Unlock()

	if se.WAL != nil {
		if err := se.fuzzyCheckpointLocked(); err != nil {
			return nil, fmt.Errorf("backup: checkpoint: %w", err)
		}
	} else if err := se.flushAllDirtyPages(); err != nil {
		return nil, fmt.Errorf("backup: flush pages: %w", err)
	}

	sources, err := se.backupSourceFiles()
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("backup: nenhum arquivo de storage encontrado")
	}

	root, err := commonPathRoot(sources)
	if err != nil {
		return nil, err
	}

	filesDir := filepath.Join(backupDir, backupFilesDirName)
	if err := os.MkdirAll(filesDir, 0700); err != nil {
		return nil, err
	}

	manifest := &BackupManifest{
		Version:       backupManifestVer,
		CreatedAtUTC:  time.Now().UTC(),
		SourceRoot:    root,
		CheckpointLSN: se.lsnTracker.Current(),
		Files:         make([]BackupFile, 0, len(sources)),
	}

	for _, src := range sources {
		abs, err := filepath.Abs(src.path)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, abs)
		if err != nil {
			return nil, err
		}
		if err := validateBackupRelPath(rel); err != nil {
			return nil, err
		}

		dst := filepath.Join(filesDir, rel)
		size, sum, err := copyFileWithHash(src.path, dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY)
		if err != nil {
			return nil, fmt.Errorf("backup: copiar %s: %w", src.path, err)
		}
		manifest.Files = append(manifest.Files, BackupFile{
			Role:   src.role,
			Path:   filepath.ToSlash(rel),
			Size:   size,
			SHA256: sum,
		})
	}

	sort.Slice(manifest.Files, func(i, j int) bool {
		if manifest.Files[i].Path == manifest.Files[j].Path {
			return manifest.Files[i].Role < manifest.Files[j].Role
		}
		return manifest.Files[i].Path < manifest.Files[j].Path
	})

	if err := writeBackupManifest(backupDir, manifest); err != nil {
		return nil, err
	}
	if err := syncDirectory(backupDir); err != nil {
		return nil, err
	}

	return manifest, nil
}

// VerifyBackup valida manifest, tamanho e SHA-256 de cada arquivo copiado.
func VerifyBackup(backupDir string) (*BackupManifest, error) {
	manifest, err := readBackupManifest(backupDir)
	if err != nil {
		return nil, err
	}
	if manifest.Version != backupManifestVer {
		return nil, fmt.Errorf("backup: versão de manifest não suportada: %d", manifest.Version)
	}
	if len(manifest.Files) == 0 {
		return nil, fmt.Errorf("backup: manifest sem arquivos")
	}

	filesDir := filepath.Join(backupDir, backupFilesDirName)
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		rel := filepath.FromSlash(file.Path)
		if err := validateBackupRelPath(rel); err != nil {
			return nil, err
		}
		if _, ok := seen[file.Path]; ok {
			return nil, fmt.Errorf("backup: arquivo duplicado no manifest: %s", file.Path)
		}
		seen[file.Path] = struct{}{}

		size, sum, err := hashExistingFile(filepath.Join(filesDir, rel))
		if err != nil {
			return nil, fmt.Errorf("backup: verificar %s: %w", file.Path, err)
		}
		if size != file.Size {
			return nil, fmt.Errorf("backup: tamanho inválido em %s: got %d want %d", file.Path, size, file.Size)
		}
		if sum != file.SHA256 {
			return nil, fmt.Errorf("backup: sha256 inválido em %s", file.Path)
		}
	}
	return manifest, nil
}

// RestoreBackup verifica um backup e restaura seus arquivos em targetDir.
// Arquivos existentes não são sobrescritos.
func RestoreBackup(backupDir, targetDir string) (*BackupManifest, error) {
	if targetDir == "" {
		return nil, fmt.Errorf("restore: targetDir vazio")
	}
	manifest, err := VerifyBackup(backupDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(targetDir, 0700); err != nil {
		return nil, err
	}

	filesDir := filepath.Join(backupDir, backupFilesDirName)
	for _, file := range manifest.Files {
		rel := filepath.FromSlash(file.Path)
		if err := validateBackupRelPath(rel); err != nil {
			return nil, err
		}
		dst := filepath.Join(targetDir, rel)
		if _, err := os.Stat(dst); err == nil {
			return nil, fmt.Errorf("restore: arquivo já existe: %s", dst)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}

	for _, file := range manifest.Files {
		rel := filepath.FromSlash(file.Path)
		src := filepath.Join(filesDir, rel)
		dst := filepath.Join(targetDir, rel)
		size, sum, err := copyFileWithHash(src, dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY)
		if err != nil {
			return nil, fmt.Errorf("restore: copiar %s: %w", file.Path, err)
		}
		if size != file.Size || sum != file.SHA256 {
			return nil, fmt.Errorf("restore: verificação pós-cópia falhou em %s", file.Path)
		}
	}
	if err := syncDirectory(targetDir); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (se *StorageEngine) backupSourceFiles() ([]backupSourceFile, error) {
	var out []backupSourceFile
	add := func(role, path string) error {
		if path == "" {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fmt.Errorf("backup: %s aponta para diretório: %s", role, path)
		}
		out = append(out, backupSourceFile{role: role, path: path})
		return nil
	}

	tableNames := se.TableMetaData.ListTables()
	sort.Strings(tableNames)
	for _, tableName := range tableNames {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil || table == nil {
			continue
		}
		if table.Heap != nil {
			if err := add("heap:"+tableName, table.Heap.Path()); err != nil {
				return nil, err
			}
		}
		indices := table.GetIndices()
		sort.Slice(indices, func(i, j int) bool { return indices[i].Name < indices[j].Name })
		for _, idx := range indices {
			if idx.Tree == nil {
				continue
			}
			pathed, ok := idx.Tree.(pathProvider)
			if !ok {
				return nil, fmt.Errorf("backup: índice %s.%s não expõe Path()", tableName, idx.Name)
			}
			if err := add("index:"+tableName+"."+idx.Name, pathed.Path()); err != nil {
				return nil, err
			}
		}
	}

	if se.WAL != nil {
		paths, err := wal.SegmentPaths(se.WAL.Path())
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			if err := add("wal", path); err != nil {
				return nil, err
			}
		}
	}

	seen := make(map[string]backupSourceFile, len(out))
	for _, src := range out {
		abs, err := filepath.Abs(src.path)
		if err != nil {
			return nil, err
		}
		if existing, ok := seen[abs]; ok {
			existing.role += "," + src.role
			seen[abs] = existing
			continue
		}
		src.path = abs
		seen[abs] = src
	}

	out = out[:0]
	for _, src := range seen {
		out = append(out, src)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

func prepareEmptyBackupDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("backup: diretório de backup deve estar vazio: %s", path)
	}
	return nil
}

func commonPathRoot(files []backupSourceFile) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("backup: lista de arquivos vazia")
	}
	partsFor := func(path string) ([]string, error) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		abs = filepath.Clean(abs)
		volume := filepath.VolumeName(abs)
		trimmed := strings.TrimPrefix(abs, volume)
		return append([]string{volume}, strings.Split(strings.Trim(trimmed, string(os.PathSeparator)), string(os.PathSeparator))...), nil
	}

	common, err := partsFor(files[0].path)
	if err != nil {
		return "", err
	}
	common = common[:len(common)-1]
	for _, file := range files[1:] {
		parts, err := partsFor(file.path)
		if err != nil {
			return "", err
		}
		parts = parts[:len(parts)-1]
		n := 0
		for n < len(common) && n < len(parts) && common[n] == parts[n] {
			n++
		}
		common = common[:n]
	}
	if len(common) == 0 {
		return "", fmt.Errorf("backup: arquivos em volumes diferentes")
	}
	volume := common[0]
	pathParts := common[1:]
	if len(pathParts) == 0 {
		if volume != "" {
			return volume + string(os.PathSeparator), nil
		}
		return string(os.PathSeparator), nil
	}
	return filepath.Join(append([]string{volume + string(os.PathSeparator)}, pathParts...)...), nil
}

func validateBackupRelPath(rel string) error {
	clean := filepath.Clean(rel)
	if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || clean == ".." {
		return fmt.Errorf("backup: caminho inválido no manifest: %s", rel)
	}
	return nil
}

func copyFileWithHash(src, dst string, flag int) (int64, string, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, "", err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", fmt.Errorf("não é arquivo regular")
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return 0, "", err
	}

	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return 0, "", err
	}

	h := sha256.New()
	size, copyErr := copyAndHash(out, in, h)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return 0, "", copyErr
	}
	if syncErr != nil {
		_ = os.Remove(tmp)
		return 0, "", syncErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return 0, "", closeErr
	}

	if flag&os.O_EXCL != 0 {
		if _, err := os.Stat(dst); err == nil {
			_ = os.Remove(tmp)
			return 0, "", os.ErrExist
		} else if !os.IsNotExist(err) {
			_ = os.Remove(tmp)
			return 0, "", err
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return 0, "", err
	}
	if err := syncDirectory(filepath.Dir(dst)); err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

func copyAndHash(dst io.Writer, src io.Reader, h hash.Hash) (int64, error) {
	return io.Copy(io.MultiWriter(dst, h), src)
}

func hashExistingFile(path string) (int64, string, error) {
	in, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", fmt.Errorf("não é arquivo regular")
	}
	h := sha256.New()
	size, err := io.Copy(h, in)
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

func writeBackupManifest(backupDir string, manifest *BackupManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(backupDir, backupManifestName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0600); err != nil {
		return err
	}
	file, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

func readBackupManifest(backupDir string) (*BackupManifest, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, backupManifestName))
	if err != nil {
		return nil, err
	}
	var manifest BackupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
