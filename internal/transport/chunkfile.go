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

package transport

import (
	"io"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/vfs"
)

// ChunkFile is the snapshot chunk file being transferred.
type ChunkFile struct {
	file    vfs.File
	syncDir bool
	dir     string
	fs      vfs.IFS
}

// OpenChunkFileForAppend opens the chunk file at fp for appending.
func OpenChunkFileForAppend(fp string, fs vfs.IFS) (*ChunkFile, error) {
	f, err := fs.OpenForAppend(fp)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f, fs: fs}, nil
}

// OpenChunkFileForRead opens for the chunk file for read-only operation.
func OpenChunkFileForRead(fp string, fs vfs.IFS) (*ChunkFile, error) {
	f, err := fs.Open(fp)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f, fs: fs}, nil
}

// CreateChunkFile creates a new chunk file.
func CreateChunkFile(fp string, fs vfs.IFS) (*ChunkFile, error) {
	f, err := fs.Create(fp)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f, syncDir: true, dir: fs.PathDir(fp), fs: fs}, nil
}

// ReadAt reads from the file.
func (cf *ChunkFile) ReadAt(data []byte, offset int64) (int, error) {
	return cf.file.ReadAt(data, offset)
}

// Read reads from the file.
func (cf *ChunkFile) Read(data []byte) (int, error) {
	return io.ReadFull(cf.file, data)
}

// Write writes the specified data to the chunk file.
func (cf *ChunkFile) Write(data []byte) (int, error) {
	return cf.file.Write(data)
}

// Close closes the chunk file.
func (cf *ChunkFile) Close() error {
	if err := cf.file.Close(); err != nil {
		return err
	}
	if cf.syncDir {
		return fileutil.SyncDir(cf.dir, cf.fs)
	}
	return nil
}

// Sync syncs the chunk file.
func (cf *ChunkFile) Sync() error {
	return cf.file.Sync()
}
