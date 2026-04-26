package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

var segmentSuffixRE = regexp.MustCompile(`\.(\d{20})$`)

func segmentPath(base string, seq uint64) string {
	return fmt.Sprintf("%s.%020d", base, seq)
}

func parseSegmentSeq(base, path string) (uint64, bool) {
	if !strings.HasPrefix(path, base+".") {
		return 0, false
	}
	m := segmentSuffixRE.FindStringSubmatch(path)
	if len(m) != 2 {
		return 0, false
	}
	seq, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// SegmentPaths retorna os segmentos arquivados locais em ordem e o WAL ativo
// por último. O arquivo ativo pode estar ausente quando se está restaurando
// apenas a partir de segmentos.
func SegmentPaths(base string) ([]string, error) {
	dir := filepath.Dir(base)
	name := filepath.Base(base)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type seg struct {
		path string
		seq  uint64
	}
	segments := make([]seg, 0)
	prefix := name + "."
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		seq, ok := parseSegmentSeq(base, path)
		if !ok {
			continue
		}
		segments = append(segments, seg{path: path, seq: seq})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })

	paths := make([]string, 0, len(segments)+1)
	for _, segment := range segments {
		paths = append(paths, segment.path)
	}
	if _, err := os.Stat(base); err == nil {
		paths = append(paths, base)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return paths, nil
}

func nextSegmentPath(base string) (string, error) {
	paths, err := SegmentPaths(base)
	if err != nil {
		return "", err
	}
	var maxSeq uint64
	for _, path := range paths {
		if seq, ok := parseSegmentSeq(base, path); ok && seq > maxSeq {
			maxSeq = seq
		}
	}
	return segmentPath(base, maxSeq+1), nil
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type segmentRange struct {
	path   string
	maxLSN uint64
	hasLSN bool
}

func scanSegmentRange(path string, cipher crypto.Cipher) (segmentRange, error) {
	reader, err := newSinglePathReader(path, cipher)
	if err != nil {
		return segmentRange{}, err
	}
	defer reader.Close()

	result := segmentRange{path: path}
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return result, err
		}
		if entry.Header.LSN > result.maxLSN {
			result.maxLSN = entry.Header.LSN
		}
		result.hasLSN = true
		ReleaseEntry(entry)
	}
	return result, nil
}

// ArchiveAndTruncate remove segmentos locais cujo max LSN já está coberto por
// checkpointLSN. Se archiveDir estiver configurado, copia cada segmento para lá
// antes de remover o arquivo ativo local.
func ArchiveAndTruncate(base string, cipher crypto.Cipher, archiveDir string, checkpointLSN uint64, retentionSegments int) error {
	paths, err := SegmentPaths(base)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}

	candidates := make([]string, 0)
	for _, path := range paths {
		if path == base {
			continue
		}
		rng, err := scanSegmentRange(path, cipher)
		if err != nil {
			return fmt.Errorf("wal: scan segment %s: %w", path, err)
		}
		if rng.hasLSN && rng.maxLSN < checkpointLSN {
			candidates = append(candidates, path)
		}
	}

	if retentionSegments > 0 && len(candidates) > retentionSegments {
		candidates = candidates[:len(candidates)-retentionSegments]
	} else if retentionSegments > 0 {
		candidates = nil
	}

	for _, path := range candidates {
		if archiveDir != "" {
			if err := archiveSegment(path, archiveDir); err != nil {
				return err
			}
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if len(candidates) > 0 {
		return fsyncDir(filepath.Dir(base))
	}
	return nil
}

func archiveSegment(path, archiveDir string) error {
	if err := os.MkdirAll(archiveDir, 0700); err != nil {
		return err
	}
	dst := filepath.Join(archiveDir, filepath.Base(path))
	if _, err := os.Stat(dst); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return err
	}

	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, src); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return fsyncDir(archiveDir)
}

// RestoreArchivedSegments copia segmentos do archiveDir de volta para o
// diretório ativo do WAL. Arquivos existsntes are not sobrescritos.
func RestoreArchivedSegments(base, archiveDir string) error {
	if archiveDir == "" {
		return nil
	}
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	activeDir := filepath.Dir(base)
	activeBase := filepath.Base(base)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), activeBase+".") {
			continue
		}
		if _, ok := parseSegmentSeq(filepath.Join(activeDir, activeBase), filepath.Join(activeDir, entry.Name())); !ok {
			continue
		}
		src := filepath.Join(archiveDir, entry.Name())
		dst := filepath.Join(activeDir, entry.Name())
		if _, err := os.Stat(dst); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	return fsyncDir(activeDir)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
