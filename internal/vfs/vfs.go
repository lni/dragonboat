// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
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

package vfs

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors/oserror"
	pvfs "github.com/cockroachdb/pebble/vfs"

	gvfs "github.com/lni/vfs"
)

// IFS is the vfs interface used by dragonboat.
type IFS = gvfs.FS

// MemFS is a memory backed file system for testing purposes.
type MemFS = gvfs.MemFS

// DefaultFS is a vfs instance using underlying OS fs.
var DefaultFS IFS = gvfs.Default

// MemStrictFS is a vfs instance using memfs.
var MemStrictFS IFS = gvfs.NewStrictMem()

// File is the file interface returned by IFS.
type File = gvfs.File

// NewMemFS creates a in-memory fs.
func NewMemFS() IFS {
	return gvfs.NewStrictMem()
}

// PebbleFS is a wrapper struct that implements the pebble/vfs.FS interface.
type PebbleFS struct {
	fs IFS
}

var _ pvfs.FS = (*PebbleFS)(nil)

// NewPebbleFS creates a new pebble/vfs.FS instance.
func NewPebbleFS(fs IFS) pvfs.FS {
	return &PebbleFS{fs}
}

// GetDiskUsage ...
func (p *PebbleFS) GetDiskUsage(path string) (pvfs.DiskUsage, error) {
	du, err := p.fs.GetDiskUsage(path)
	return pvfs.DiskUsage{
		AvailBytes: du.AvailBytes,
		TotalBytes: du.TotalBytes,
		UsedBytes:  du.UsedBytes,
	}, err
}

// Create ...
func (p *PebbleFS) Create(name string) (pvfs.File, error) {
	return p.fs.Create(name)
}

// Link ...
func (p *PebbleFS) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

// Open ...
func (p *PebbleFS) Open(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenDir ...
func (p *PebbleFS) OpenDir(name string) (pvfs.File, error) {
	return p.fs.OpenDir(name)
}

// Remove ...
func (p *PebbleFS) Remove(name string) error {
	return p.fs.Remove(name)
}

// RemoveAll ...
func (p *PebbleFS) RemoveAll(name string) error {
	return p.fs.RemoveAll(name)
}

// Rename ...
func (p *PebbleFS) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// ReuseForWrite ...
func (p *PebbleFS) ReuseForWrite(oldname, newname string) (pvfs.File, error) {
	return p.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll ...
func (p *PebbleFS) MkdirAll(dir string, perm os.FileMode) error {
	return p.fs.MkdirAll(dir, perm)
}

// Lock ...
func (p *PebbleFS) Lock(name string) (io.Closer, error) {
	return p.fs.Lock(name)
}

// List ...
func (p *PebbleFS) List(dir string) ([]string, error) {
	return p.fs.List(dir)
}

// Stat ...
func (p *PebbleFS) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

// PathBase ...
func (p *PebbleFS) PathBase(path string) string {
	return p.fs.PathBase(path)
}

// PathJoin ...
func (p *PebbleFS) PathJoin(elem ...string) string {
	return p.fs.PathJoin(elem...)
}

// PathDir ...
func (p *PebbleFS) PathDir(path string) string {
	return p.fs.PathDir(path)
}

// IsNotExist returns a boolean value indicating whether the specified error is
// to indicate that a file or directory does not exist.
func IsNotExist(err error) bool {
	return oserror.IsNotExist(err)
}

// IsExist returns a boolean value indicating whether the specified error is to
// indicate that a file or directory already exists.
func IsExist(err error) bool {
	return oserror.IsExist(err)
}

// TempDir returns the directory use for storing temporary files.
func TempDir() string {
	return os.TempDir()
}

// Clean is a wrapper for filepath.Clean.
func Clean(dir string) string {
	return filepath.Clean(dir)
}

// ReportLeakedFD reports leaked file fds.
func ReportLeakedFD(fs IFS, t *testing.T) {
	gvfs.ReportLeakedFD(fs, t)
}
