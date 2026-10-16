package persist

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
)

// Manifest file names inside a data directory.
const (
	// ManifestName is the manifest, the commit point of every checkpoint.
	ManifestName = "MANIFEST"
	// ManifestTmpName is where a new manifest is written before it is renamed
	// over ManifestName. A leftover one is garbage from a crash.
	ManifestTmpName = "MANIFEST.tmp"
	// ManifestVersion is the manifest format version.
	ManifestVersion = 1

	manifestMagic = "SYLPHY-MANIFEST"
)

// BaseRef identifies the base snapshot. The zero value means "no base".
type BaseRef struct {
	// Name is the file name, from BaseName.
	Name string
	// Size is the file size in bytes.
	Size int64
	// CRC is the snapshot's whole-file CRC-32C (its trailer's checksum).
	CRC uint32
}

// Manifest lists what makes up the database on disk: an optional base snapshot
// followed by segments in replay order. A manifest with no segments is valid;
// snapshot-only mode never has any.
type Manifest struct {
	// Generation increases with every committed checkpoint.
	Generation uint64
	// Base is the base snapshot, if any.
	Base BaseRef
	// Segments are the AOF segment file names, oldest first; their numeric
	// ids must be strictly increasing.
	Segments []string
}

// BaseName returns the file name of the base snapshot with the given id.
func BaseName(id uint64) string { return fmt.Sprintf("base-%06d.snap", id) }

// SegmentName returns the file name of the AOF segment with the given id.
func SegmentName(id uint64) string { return fmt.Sprintf("seg-%06d.aof", id) }

// ParseBaseName returns the id in a canonical base file name.
func ParseBaseName(name string) (uint64, bool) { return parseName(name, "base-", ".snap") }

// ParseSegmentName returns the id in a canonical segment file name.
func ParseSegmentName(name string) (uint64, bool) { return parseName(name, "seg-", ".aof") }

// parseName accepts only the exact spelling BaseName/SegmentName produce, so a
// manifest can never name a path outside the data directory.
func parseName(name, prefix, suffix string) (uint64, bool) {
	if len(name) < len(prefix)+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	id, err := strconv.ParseUint(name[len(prefix):len(name)-len(suffix)], 10, 64)
	if err != nil || fmt.Sprintf("%s%06d%s", prefix, id, suffix) != name {
		return 0, false
	}
	return id, true
}

// Validate checks the manifest's internal consistency.
func (m Manifest) Validate() error {
	if m.Base.Name != "" {
		if _, ok := ParseBaseName(m.Base.Name); !ok {
			return fmt.Errorf("%w: bad base file name %q", ErrCorrupt, m.Base.Name)
		}
		if m.Base.Size < 0 {
			return fmt.Errorf("%w: negative base size", ErrCorrupt)
		}
	} else if m.Base.Size != 0 || m.Base.CRC != 0 {
		return fmt.Errorf("%w: base size or checksum without a base", ErrCorrupt)
	}
	var prev uint64
	for i, s := range m.Segments {
		id, ok := ParseSegmentName(s)
		if !ok {
			return fmt.Errorf("%w: bad segment file name %q", ErrCorrupt, s)
		}
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: segment ids must be strictly increasing (%q)", ErrCorrupt, s)
		}
		prev = id
	}
	return nil
}

// EncodeManifest renders m in the manifest text format:
//
//	SYLPHY-MANIFEST 1
//	generation 7
//	base base-000003.snap 123456 0a1b2c3d     (optional: name, size, CRC-32C hex)
//	segment seg-000004.aof                    (zero or more, in replay order)
//	crc 89abcdef                              (CRC-32C of every preceding byte)
//
// Every number is canonical decimal (or 8 lower-case hex digits), so a
// manifest that decodes re-encodes to the same bytes.
func EncodeManifest(m Manifest) []byte {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %d\n", manifestMagic, ManifestVersion)
	fmt.Fprintf(&sb, "generation %d\n", m.Generation)
	if m.Base.Name != "" {
		fmt.Fprintf(&sb, "base %s %d %08x\n", m.Base.Name, m.Base.Size, m.Base.CRC)
	}
	for _, s := range m.Segments {
		fmt.Fprintf(&sb, "segment %s\n", s)
	}
	body := sb.String()
	return []byte(body + fmt.Sprintf("crc %08x\n", crc32.Checksum([]byte(body), castagnoli)))
}

func parseCanonicalUint(s string, bitSize int) (uint64, bool) {
	v, err := strconv.ParseUint(s, 10, bitSize)
	if err != nil || strconv.FormatUint(v, 10) != s {
		return 0, false
	}
	return v, true
}

func parseCanonicalHex32(s string) (uint32, bool) {
	if len(s) != 8 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil || fmt.Sprintf("%08x", v) != s {
		return 0, false
	}
	return uint32(v), true
}

// DecodeManifest parses and verifies a manifest. The checksum is verified
// before anything else is interpreted, so a damaged file is reported as
// ErrChecksum and only an intact file of a newer format as ErrVersion.
func DecodeManifest(b []byte) (Manifest, error) {
	s := string(b)
	if !strings.HasSuffix(s, "\n") {
		return Manifest{}, fmt.Errorf("%w: manifest is not newline-terminated", ErrTruncated)
	}
	trimmed := s[:len(s)-1]
	cut := strings.LastIndexByte(trimmed, '\n') + 1
	body, last := s[:cut], trimmed[cut:]
	cf := strings.Split(last, " ")
	if len(cf) != 2 || cf[0] != "crc" {
		return Manifest{}, fmt.Errorf("%w: manifest has no checksum line", ErrTruncated)
	}
	want, ok := parseCanonicalHex32(cf[1])
	if !ok {
		return Manifest{}, fmt.Errorf("%w: malformed manifest checksum", ErrCorrupt)
	}
	if crc32.Checksum([]byte(body), castagnoli) != want {
		return Manifest{}, fmt.Errorf("%w: manifest", ErrChecksum)
	}

	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	if len(lines) < 2 {
		return Manifest{}, fmt.Errorf("%w: manifest is too short", ErrCorrupt)
	}
	vf := strings.Split(lines[0], " ")
	if len(vf) != 2 || vf[0] != manifestMagic {
		return Manifest{}, fmt.Errorf("%w: bad manifest magic", ErrCorrupt)
	}
	ver, ok := parseCanonicalUint(vf[1], 32)
	if !ok {
		return Manifest{}, fmt.Errorf("%w: bad manifest version", ErrCorrupt)
	}
	if ver != ManifestVersion {
		return Manifest{}, fmt.Errorf("%w: manifest version %d", ErrVersion, ver)
	}
	gf := strings.Split(lines[1], " ")
	if len(gf) != 2 || gf[0] != "generation" {
		return Manifest{}, fmt.Errorf("%w: manifest has no generation", ErrCorrupt)
	}
	gen, ok := parseCanonicalUint(gf[1], 64)
	if !ok {
		return Manifest{}, fmt.Errorf("%w: bad manifest generation", ErrCorrupt)
	}
	m := Manifest{Generation: gen}
	for i, line := range lines[2:] {
		f := strings.Split(line, " ")
		switch {
		case f[0] == "base" && len(f) == 4 && i == 0: // the base always comes first
			size, ok1 := parseCanonicalUint(f[2], 63)
			crc, ok2 := parseCanonicalHex32(f[3])
			if !ok1 || !ok2 {
				return Manifest{}, fmt.Errorf("%w: bad base line %q", ErrCorrupt, line)
			}
			m.Base = BaseRef{Name: f[1], Size: int64(size), CRC: crc}
		case f[0] == "segment" && len(f) == 2:
			m.Segments = append(m.Segments, f[1])
		default:
			return Manifest{}, fmt.Errorf("%w: unexpected manifest line %q", ErrCorrupt, line)
		}
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// LoadManifest reads dir's manifest. found is false, with a nil error, when
// there is none: a directory that has never been checkpointed.
func LoadManifest(fsys FS, dir string) (m Manifest, found bool, err error) {
	name := filepath.Join(dir, ManifestName)
	b, err := ReadFile(fsys, name)
	if errors.Is(err, fs.ErrNotExist) {
		return Manifest{}, false, nil
	}
	if err != nil {
		return Manifest{}, false, fmt.Errorf("persist: reading %s: %w", name, err)
	}
	m, err = DecodeManifest(b)
	if err != nil {
		return Manifest{}, false, fmt.Errorf("persist: %s: %w", name, err)
	}
	return m, true, nil
}

// CommitManifest atomically installs m: write MANIFEST.tmp, fsync it, rename it
// over MANIFEST, fsync the directory. The rename is the commit point.
//
// If it returns an error the outcome is uncertain only when the failure was in
// the final directory sync: the new manifest may or may not survive a crash.
// A crash at any other step leaves the previous manifest in place.
func CommitManifest(fsys FS, dir string, m Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	tmp := filepath.Join(dir, ManifestTmpName)
	if err := writeFileSync(fsys, tmp, EncodeManifest(m)); err != nil {
		_ = fsys.Remove(tmp)
		return fmt.Errorf("persist: writing manifest: %w", err)
	}
	if err := fsys.Rename(tmp, filepath.Join(dir, ManifestName)); err != nil {
		_ = fsys.Remove(tmp)
		return fmt.Errorf("persist: installing manifest: %w", err)
	}
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("persist: syncing %s after manifest rename: %w", dir, err)
	}
	return nil
}
