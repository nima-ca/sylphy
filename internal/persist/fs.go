package persist

import (
	"io"
	"os"
	"runtime"
)

// File is the subset of *os.File the persistence layer uses.
type File interface {
	io.ReadWriteCloser
	// Sync flushes the file's data to stable storage.
	Sync() error
	// Truncate changes the file size.
	Truncate(size int64) error
	// Size returns the current file size in bytes.
	Size() (int64, error)
}

// FS is the file-system seam. Production code uses OSFS; tests use MemFS to
// inject failures and simulate crashes deterministically.
//
// Durability contract (what the real implementation gives, and what MemFS
// models adversarially): file data is durable after File.Sync; a created,
// renamed or removed directory entry is durable only after SyncDir of its
// directory.
type FS interface {
	// OpenFile opens name with os.O_* flags, like os.OpenFile.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// Rename atomically replaces newName with oldName.
	Rename(oldName, newName string) error
	// Remove deletes a file.
	Remove(name string) error
	// ReadDir lists the names in dir, sorted.
	ReadDir(dir string) ([]string, error)
	// MkdirAll creates dir and any missing parents.
	MkdirAll(dir string, perm os.FileMode) error
	// SyncDir flushes dir's directory entries to stable storage. It is a
	// no-op on Windows, which cannot open directories for syncing.
	SyncDir(dir string) error
}

type osFS struct{}

var _ FS = osFS{}

// OSFS returns the FS backed by the operating system.
func OSFS() FS { return osFS{} }

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Rename(oldName, newName string) error { return os.Rename(oldName, newName) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) ReadDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir) // sorted by file name
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, nil
}

func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }

func (osFS) SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// ReadFile reads the whole named file through fsys.
func ReadFile(fsys FS, name string) ([]byte, error) {
	f, err := fsys.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return data, err
}

// writeFileSync creates or truncates name, writes data and fsyncs it.
func writeFileSync(fsys FS, name string, data []byte) (err error) {
	f, err := fsys.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}
