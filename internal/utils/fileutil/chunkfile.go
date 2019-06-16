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

package fileutil

import (
	"io"
	"os"
	"path/filepath"
)

// ChunkFile is the snapshot chunk file being transferred.
type ChunkFile struct {
	file    *os.File
	syncDir bool
	dir     string
}

// OpenChunkFileForAppend opens the chunk file at fp for appending.
func OpenChunkFileForAppend(fp string) (*ChunkFile, error) {
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_APPEND, DefaultFileMode)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f}, nil
}

// OpenChunkFileForRead opens for the chunk file for read-only operation.
func OpenChunkFileForRead(fp string) (*ChunkFile, error) {
	f, err := os.OpenFile(fp, os.O_RDONLY, DefaultFileMode)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f}, nil
}

// CreateChunkFile creates a new chunk file.
func CreateChunkFile(fp string) (*ChunkFile, error) {
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DefaultFileMode)
	if err != nil {
		return nil, err
	}
	return &ChunkFile{file: f, syncDir: true, dir: filepath.Dir(fp)}, nil
}

// SeekFromBeginning seeks the underlying file from the beginning.
func (cf *ChunkFile) SeekFromBeginning(offset int64) (int64, error) {
	return cf.file.Seek(offset, 0)
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
func (cf *ChunkFile) Close() {
	if err := cf.file.Close(); err != nil {
		panic(err)
	}
	if cf.syncDir {
		if err := SyncDir(cf.dir); err != nil {
			panic(err)
		}
	}
}

// Sync syncs the chunk file.
func (cf *ChunkFile) Sync() error {
	return cf.file.Sync()
}
