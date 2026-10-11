package persist

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
)

// ErrCrashed is returned by every operation on a MemFS after a simulated crash
// (CrashAt or Halt).
var ErrCrashed = errors.New("persist: simulated crash")

// OpKind identifies a mutating file-system operation for fault injection.
type OpKind uint8

// Mutating operation kinds. MemFS counts these (and only these) operations;
// reads, Close, Size, ReadDir and read-only opens are free.
const (
	// OpOpen is OpenFile with write intent (write mode, create, truncate or append).
	OpOpen OpKind = iota + 1
	// OpWrite is File.Write.
	OpWrite
	// OpSync is File.Sync.
	OpSync
	// OpTruncate is File.Truncate.
	OpTruncate
	// OpRename is FS.Rename.
	OpRename
	// OpRemove is FS.Remove.
	OpRemove
	// OpSyncDir is FS.SyncDir.
	OpSyncDir
	// OpMkdir is FS.MkdirAll.
	OpMkdir
)

var opNames = [...]string{
	OpOpen: "open", OpWrite: "write", OpSync: "sync", OpTruncate: "truncate",
	OpRename: "rename", OpRemove: "remove", OpSyncDir: "syncdir", OpMkdir: "mkdir",
}

// String returns the operation name.
func (k OpKind) String() string {
	if int(k) < len(opNames) && opNames[k] != "" {
		return opNames[k]
	}
	return "unknown"
}

// Op describes one mutating operation about to run.
type Op struct {
	// N is the 1-based sequence number of this operation in the MemFS.
	N int
	// Kind is the operation kind.
	Kind OpKind
	// Name is the path (the old path for a rename).
	Name string
}

// inode is a file's content. data is what readers see; durable is what would
// survive a crash. clean means data extends durable (every write since the
// last Sync appended), which lets Sync copy only the new tail.
type inode struct {
	data    []byte
	durable []byte
	clean   bool
}

// MemFS is an in-memory FS for tests. It models the failure behavior of a real
// disk adversarially: file bytes are durable only after Sync, and directory
// changes (create, rename, remove) are durable only after SyncDir. It can fail
// or "crash" at the Nth mutating operation, and Crash returns the disk image a
// power loss at that moment could leave behind.
//
// MemFS is safe for concurrent use. Hooks run with its lock held and must not
// call back into it.
type MemFS struct {
	mu      sync.Mutex
	files   map[string]*inode // the namespace processes see
	durable map[string]*inode // the namespace as of each directory's last SyncDir
	dirs    map[string]bool
	ops     int
	crashed bool
	hook    func(Op) error
}

var _ FS = (*MemFS)(nil)

// NewMemFS returns an empty MemFS whose only directory is ".".
func NewMemFS() *MemFS {
	return &MemFS{
		files:   make(map[string]*inode),
		durable: make(map[string]*inode),
		dirs:    map[string]bool{".": true},
	}
}

func norm(name string) string { return filepath.ToSlash(filepath.Clean(name)) }

func pathErr(op, name string, err error) error { return &fs.PathError{Op: op, Path: name, Err: err} }

// begin accounts one mutating operation. The caller holds m.mu.
func (m *MemFS) begin(kind OpKind, name string) error {
	if m.crashed {
		return ErrCrashed
	}
	m.ops++
	if m.hook == nil {
		return nil
	}
	err := m.hook(Op{N: m.ops, Kind: kind, Name: name})
	if errors.Is(err, ErrCrashed) {
		m.crashed = true
	}
	return err
}

// SetHook installs h, which runs before every mutating operation. Returning
// nil lets the operation proceed; returning an error makes it fail without any
// effect; returning ErrCrashed also halts the MemFS permanently.
func (m *MemFS) SetHook(h func(Op) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hook = h
}

// FailAt makes the n-th mutating operation (counting from the creation of the
// MemFS; see Ops) fail once with err and no effect.
func (m *MemFS) FailAt(n int, err error) {
	m.SetHook(func(op Op) error {
		if op.N == n {
			return err
		}
		return nil
	})
}

// FailFrom makes every mutating operation from the n-th on fail with err.
func (m *MemFS) FailFrom(n int, err error) {
	m.SetHook(func(op Op) error {
		if op.N >= n {
			return err
		}
		return nil
	})
}

// CrashAt simulates a crash at the n-th mutating operation: that operation has
// no effect and it and every later one return ErrCrashed. Take the post-crash
// disk with Crash.
func (m *MemFS) CrashAt(n int) { m.FailAt(n, ErrCrashed) }

// Halt makes every later operation return ErrCrashed, like a dead process.
func (m *MemFS) Halt() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.crashed = true
}

// Ops returns how many mutating operations have run.
func (m *MemFS) Ops() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ops
}

// Contents returns a copy of the current content of name.
func (m *MemFS) Contents(name string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ino, ok := m.files[norm(name)]
	if !ok {
		return nil, false
	}
	return slices.Clone(ino.data), true
}

// Names returns every file name, sorted.
func (m *MemFS) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.files))
	for n := range m.files {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// CrashOptions selects how pessimistic a simulated power loss is.
type CrashOptions struct {
	// DropDirOps reverts directory changes (create, rename, remove) that no
	// SyncDir has covered. Without it every directory change survives.
	DropDirOps bool
	// KeepUnsynced, if set, says how many bytes of a file's unsynced appended
	// tail survive (clamped to [0, unsynced]); a torn write. Nil keeps none.
	// Files that were overwritten in place keep only their synced content.
	KeepUnsynced func(name string, unsynced int) int
}

// Crash returns the disk image a power loss right now could leave: a new,
// fully durable MemFS. It does not change m, so one run can be examined under
// several CrashOptions.
func (m *MemFS) Crash(o CrashOptions) *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := m.files
	if o.DropDirOps {
		ns = m.durable
	}
	out := NewMemFS()
	for d := range m.dirs {
		out.dirs[d] = true
	}
	for name, ino := range ns {
		data := slices.Clone(ino.durable)
		if o.KeepUnsynced != nil && ino.clean && len(ino.data) > len(ino.durable) {
			unsynced := len(ino.data) - len(ino.durable)
			keep := min(max(o.KeepUnsynced(name, unsynced), 0), unsynced)
			data = append(data, ino.data[len(ino.durable):len(ino.durable)+keep]...)
		}
		n := &inode{data: data, durable: slices.Clone(data), clean: true}
		out.files[name] = n
		out.durable[name] = n
	}
	return out
}

// OpenFile implements FS.
func (m *MemFS) OpenFile(name string, flag int, _ os.FileMode) (File, error) {
	name = norm(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	mode := flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	intent := mode != os.O_RDONLY || flag&(os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0
	if intent {
		if err := m.begin(OpOpen, name); err != nil {
			return nil, pathErr("open", name, err)
		}
	} else if m.crashed {
		return nil, pathErr("open", name, ErrCrashed)
	}
	ino, ok := m.files[name]
	switch {
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, pathErr("open", name, fs.ErrExist)
	case !ok && flag&os.O_CREATE == 0:
		return nil, pathErr("open", name, fs.ErrNotExist)
	case !ok:
		if !m.dirs[path.Dir(name)] {
			return nil, pathErr("open", name, fs.ErrNotExist)
		}
		ino = &inode{clean: true}
		m.files[name] = ino
	}
	if flag&os.O_TRUNC != 0 && len(ino.data) > 0 {
		ino.data = nil
		ino.clean = false
	}
	return &memFile{
		m: m, name: name, ino: ino,
		read:       mode == os.O_RDONLY || mode == os.O_RDWR,
		write:      mode == os.O_WRONLY || mode == os.O_RDWR,
		appendMode: flag&os.O_APPEND != 0,
	}, nil
}

// Rename implements FS.
func (m *MemFS) Rename(oldName, newName string) error {
	oldName, newName = norm(oldName), norm(newName)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(OpRename, oldName); err != nil {
		return &os.LinkError{Op: "rename", Old: oldName, New: newName, Err: err}
	}
	ino, ok := m.files[oldName]
	if !ok || !m.dirs[path.Dir(newName)] {
		return &os.LinkError{Op: "rename", Old: oldName, New: newName, Err: fs.ErrNotExist}
	}
	m.files[newName] = ino
	if oldName != newName {
		delete(m.files, oldName)
	}
	return nil
}

// Remove implements FS.
func (m *MemFS) Remove(name string) error {
	name = norm(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(OpRemove, name); err != nil {
		return pathErr("remove", name, err)
	}
	if _, ok := m.files[name]; !ok {
		return pathErr("remove", name, fs.ErrNotExist)
	}
	delete(m.files, name)
	return nil
}

// ReadDir implements FS.
func (m *MemFS) ReadDir(dir string) ([]string, error) {
	dir = norm(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashed {
		return nil, pathErr("readdir", dir, ErrCrashed)
	}
	if !m.dirs[dir] {
		return nil, pathErr("readdir", dir, fs.ErrNotExist)
	}
	var names []string
	for n := range m.files {
		if path.Dir(n) == dir {
			names = append(names, path.Base(n))
		}
	}
	slices.Sort(names)
	return names, nil
}

// MkdirAll implements FS. Directories are durable immediately.
func (m *MemFS) MkdirAll(dir string, _ os.FileMode) error {
	dir = norm(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(OpMkdir, dir); err != nil {
		return pathErr("mkdir", dir, err)
	}
	for d := dir; d != "." && d != "/"; d = path.Dir(d) {
		m.dirs[d] = true
	}
	return nil
}

// SyncDir implements FS: it makes every directory change under dir durable.
func (m *MemFS) SyncDir(dir string) error {
	dir = norm(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(OpSyncDir, dir); err != nil {
		return pathErr("syncdir", dir, err)
	}
	if !m.dirs[dir] {
		return pathErr("syncdir", dir, fs.ErrNotExist)
	}
	for n := range m.durable {
		if path.Dir(n) == dir {
			if _, ok := m.files[n]; !ok {
				delete(m.durable, n)
			}
		}
	}
	for n, ino := range m.files {
		if path.Dir(n) == dir {
			m.durable[n] = ino
		}
	}
	return nil
}

// memFile is an open handle. Like a POSIX descriptor it keeps working after
// its name is renamed or removed.
type memFile struct {
	m          *MemFS
	name       string
	ino        *inode
	pos        int64
	read       bool
	write      bool
	appendMode bool
	closed     bool
}

var _ File = (*memFile)(nil)

func (f *memFile) Read(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	switch {
	case f.closed:
		return 0, fs.ErrClosed
	case f.m.crashed:
		return 0, ErrCrashed
	case !f.read:
		return 0, pathErr("read", f.name, errors.New("file not open for reading"))
	case f.pos >= int64(len(f.ino.data)):
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	switch {
	case f.closed:
		return 0, fs.ErrClosed
	case !f.write:
		return 0, pathErr("write", f.name, errors.New("file not open for writing"))
	}
	if err := f.m.begin(OpWrite, f.name); err != nil {
		return 0, pathErr("write", f.name, err)
	}
	ino := f.ino
	pos := f.pos
	if f.appendMode {
		pos = int64(len(ino.data))
	}
	n := int64(len(ino.data))
	switch {
	case pos > n:
		ino.data = append(ino.data, make([]byte, pos-n)...)
		ino.data = append(ino.data, p...)
	case pos == n:
		ino.data = append(ino.data, p...)
	default:
		overlap := copy(ino.data[pos:], p)
		ino.data = append(ino.data, p[overlap:]...)
		ino.clean = false
	}
	f.pos = pos + int64(len(p))
	return len(p), nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	if err := f.m.begin(OpSync, f.name); err != nil {
		return pathErr("sync", f.name, err)
	}
	ino := f.ino
	if ino.clean && len(ino.durable) <= len(ino.data) {
		ino.durable = append(ino.durable, ino.data[len(ino.durable):]...)
	} else {
		ino.durable = slices.Clone(ino.data)
	}
	ino.clean = true
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	if !f.write {
		return pathErr("truncate", f.name, errors.New("file not open for writing"))
	}
	if size < 0 {
		return pathErr("truncate", f.name, errors.New("negative size"))
	}
	if err := f.m.begin(OpTruncate, f.name); err != nil {
		return pathErr("truncate", f.name, err)
	}
	ino := f.ino
	n := int64(len(ino.data))
	switch {
	case size < n:
		ino.data = ino.data[:size]
		ino.clean = false
	case size > n:
		ino.data = append(ino.data, make([]byte, size-n)...)
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	return int64(len(f.ino.data)), nil
}

func (f *memFile) Close() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return nil
}
