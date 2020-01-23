// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

	pvfs "github.com/cockroachdb/pebble/vfs"

	gvfs "github.com/lni/goutils/vfs"
)

type IFS = gvfs.FS

var DefaultFS IFS = gvfs.Default
var MemFS IFS = gvfs.NewMem()
var MemStrictFS IFS = gvfs.NewStrictMem()

type File = gvfs.File

type PebbleFS struct {
	fs IFS
}

func NewPebbleFS(fs IFS) pvfs.FS {
	return &PebbleFS{fs}
}

func (p *PebbleFS) Create(name string) (pvfs.File, error) {
	return p.fs.Create(name)
}

func (p *PebbleFS) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

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

func (p *PebbleFS) OpenDir(name string) (pvfs.File, error) {
	return p.fs.OpenDir(name)
}

func (p *PebbleFS) Remove(name string) error {
	return p.fs.Remove(name)
}

func (p *PebbleFS) RemoveAll(name string) error {
	return p.fs.RemoveAll(name)
}

func (p *PebbleFS) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

func (p *PebbleFS) ReuseForWrite(oldname, newname string) (pvfs.File, error) {
	return p.fs.ReuseForWrite(oldname, newname)
}

func (p *PebbleFS) MkdirAll(dir string, perm os.FileMode) error {
	return p.fs.MkdirAll(dir, perm)
}

func (p *PebbleFS) Lock(name string) (io.Closer, error) {
	return p.fs.Lock(name)
}

func (p *PebbleFS) List(dir string) ([]string, error) {
	return p.fs.List(dir)
}

func (p *PebbleFS) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

func (p *PebbleFS) PathBase(path string) string {
	return p.fs.PathBase(path)
}

func (p *PebbleFS) PathJoin(elem ...string) string {
	return p.fs.PathJoin(elem...)
}

func (p *PebbleFS) PathDir(path string) string {
	return p.fs.PathDir(path)
}
