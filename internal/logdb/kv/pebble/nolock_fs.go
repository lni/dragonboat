// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pebble

import (
	"io"
	"os"

	pvfs "github.com/cockroachdb/pebble/vfs"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/vfs"
)

// noLockFS is a filesystem wrapper that bypasses file locking for non-blocking
// read-only access. It implements the pebble/vfs.FS interface but returns
// a no-op closer for Lock operations, allowing read-only Pebble instances to
// open databases that are actively locked by running NodeHost instances.
type NoLockFS struct {
	fs pvfs.FS
}

var _ pvfs.FS = (*NoLockFS)(nil)

// NewNoLockFS creates a new pebble/vfs.FS instance that bypasses locking.
func NewNoLockFS(fs vfs.IFS) pvfs.FS {
	return &NoLockFS{fs: vfs.NewPebbleFS(fs)}
}

// Create creates the named file for writing, truncating it if it already exists.
func (n *NoLockFS) Create(name string) (pvfs.File, error) {
	return n.fs.Create(name)
}

// Link creates newname as a hard link to the oldname file.
func (n *NoLockFS) Link(oldname, newname string) error {
	return n.fs.Link(oldname, newname)
}

// Open opens the named file for reading.
func (n *NoLockFS) Open(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	return n.fs.Open(name, opts...)
}

// OpenDir opens the named directory for syncing.
func (n *NoLockFS) OpenDir(name string) (pvfs.File, error) {
	return n.fs.OpenDir(name)
}

// Remove removes the named file or directory.
func (n *NoLockFS) Remove(name string) error {
	return n.fs.Remove(name)
}

// RemoveAll removes the named file or directory and any children it contains.
func (n *NoLockFS) RemoveAll(name string) error {
	return n.fs.RemoveAll(name)
}

// Rename renames a file.
func (n *NoLockFS) Rename(oldname, newname string) error {
	return n.fs.Rename(oldname, newname)
}

// ReuseForWrite attempts to reuse the file with oldname.
func (n *NoLockFS) ReuseForWrite(oldname, newname string) (pvfs.File, error) {
	return n.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll creates a directory and all necessary parents.
func (n *NoLockFS) MkdirAll(dir string, perm os.FileMode) error {
	return n.fs.MkdirAll(dir, perm)
}

// Lock returns a no-op closer, bypassing file locking for read-only access.
// This allows opening databases that are actively locked by running instances.
func (n *NoLockFS) Lock(name string) (io.Closer, error) {
	// Return a no-op closer that does nothing when closed
	return &noOpCloser{}, nil
}

// List returns a listing of the given directory.
func (n *NoLockFS) List(dir string) ([]string, error) {
	return n.fs.List(dir)
}

// Stat returns an os.FileInfo describing the named file.
func (n *NoLockFS) Stat(name string) (os.FileInfo, error) {
	return n.fs.Stat(name)
}

// PathBase returns the last element of path.
func (n *NoLockFS) PathBase(path string) string {
	return n.fs.PathBase(path)
}

// PathJoin joins any number of path elements into a single path.
func (n *NoLockFS) PathJoin(elem ...string) string {
	return n.fs.PathJoin(elem...)
}

// PathDir returns all but the last element of path.
func (n *NoLockFS) PathDir(path string) string {
	return n.fs.PathDir(path)
}

// GetFreeSpace returns the amount of free disk space for the filesystem.
func (n *NoLockFS) GetFreeSpace(path string) (uint64, error) {
	return n.fs.GetFreeSpace(path)
}

// noOpCloser is a Closer that does nothing when Close() is called.
type noOpCloser struct{}

func (n *noOpCloser) Close() error {
	return nil
}

// PebbleReadOnlyNoLockKVStore creates a read-only pebble-based IKVStore instance.
// Implementation of logdb.kvFactory.
func PebbleReadOnlyNoLockKVStore(config config.LogDBConfig,
	callback kv.LogDBCallback,
	dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	if fs != vfs.DefaultFS {
		_, isErrorFS := fs.(*vfs.ErrorFS)
		_, isMemFS := fs.(*vfs.MemFS)
		if !isErrorFS && !isMemFS {
			panic("invalid fs")
		}
	}
	return NewReadOnlyKVStore(config, dir, wal, NewNoLockFS(fs))
}
