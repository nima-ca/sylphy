package persist

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// TmpSuffix marks a file that is still being written. Recovery deletes every
// leftover one: a file with this suffix is never part of a committed state.
const TmpSuffix = ".tmp"

// BaseWriter writes a new base snapshot. It is the file half of a checkpoint;
// the store half (collecting shard blocks) belongs to the orchestrator.
//
// The commit order is: write base-NNNNNN.snap.tmp, fsync it, rename it to its
// final name, fsync the directory. The directory sync happens before any
// manifest refers to the file: a manifest rename that reached disk ahead of the
// base's rename would otherwise name a file that does not exist after a crash.
type BaseWriter struct {
	fsys   FS
	dir    string
	id     uint64
	tmp    string
	final  string
	f      File
	sw     *SnapshotWriter
	closed bool
}

// CreateBase starts the base snapshot with the given id in dir.
func CreateBase(fsys FS, dir string, id uint64) (*BaseWriter, error) {
	final := filepath.Join(dir, BaseName(id))
	tmp := final + TmpSuffix
	f, err := fsys.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("persist: creating %s: %w", tmp, err)
	}
	sw, err := NewSnapshotWriter(f)
	if err != nil {
		_ = f.Close()
		_ = fsys.Remove(tmp)
		return nil, fmt.Errorf("persist: writing %s: %w", tmp, err)
	}
	return &BaseWriter{fsys: fsys, dir: dir, id: id, tmp: tmp, final: final, f: f, sw: sw}, nil
}

// WriteFramed appends blocks produced by BlockBuffer.Finish. After an error the
// caller must call Abort.
func (b *BaseWriter) WriteFramed(framed []byte, entries, blocks int) error {
	if b.closed {
		return errors.New("persist: base writer is closed")
	}
	return b.sw.WriteFramed(framed, entries, blocks)
}

// Commit finishes the file and makes it durable under its final name. The
// returned BaseRef is ready for a Manifest. On error nothing is left behind
// that a later recovery would not delete, and the caller must not use the file.
func (b *BaseWriter) Commit() (BaseRef, error) {
	if b.closed {
		return BaseRef{}, errors.New("persist: base writer is closed")
	}
	b.closed = true
	ref, err := b.sw.Close()
	if err == nil {
		err = b.f.Sync()
	}
	if cerr := b.f.Close(); err == nil {
		err = cerr
	}
	renamed := false
	if err == nil {
		if err = b.fsys.Rename(b.tmp, b.final); err == nil {
			renamed = true
			err = b.fsys.SyncDir(b.dir)
		}
	}
	if err != nil {
		_ = b.fsys.Remove(b.tmp)
		if renamed {
			_ = b.fsys.Remove(b.final)
		}
		return BaseRef{}, fmt.Errorf("persist: finishing %s: %w", BaseName(b.id), err)
	}
	ref.Name = BaseName(b.id)
	return ref, nil
}

// Abort discards an unfinished base. It is safe after Commit and idempotent.
func (b *BaseWriter) Abort() {
	if b.closed {
		return
	}
	b.closed = true
	_ = b.f.Close()
	_ = b.fsys.Remove(b.tmp)
}

// RetireFiles deletes files a committed checkpoint no longer needs, then syncs
// the directory. Missing files are not an error. A failure here is harmless to
// correctness (recovery deletes unlisted files), so callers log it.
func RetireFiles(fsys FS, dir string, names ...string) error {
	var errs []error
	for _, n := range names {
		if n == "" {
			continue
		}
		if err := fsys.Remove(filepath.Join(dir, n)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := fsys.SyncDir(dir); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
