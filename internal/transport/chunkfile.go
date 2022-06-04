// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/vfs"
)

// chunkFile is the snapshot chunk file being transferred.
type chunkFile struct {
	file    vfs.File
	fs      vfs.IFS
	dir     string
	syncDir bool
}

// openChunkFileForAppend opens the chunk file at fp for appending.
func openChunkFileForAppend(fp string, fs vfs.IFS) (*chunkFile, error) {
	f, err := fs.OpenForAppend(fp)
	if err != nil {
		return nil, err
	}
	return &chunkFile{file: f, fs: fs}, nil
}

// openChunkFileForRead opens for the chunk file for read-only operation.
func openChunkFileForRead(fp string, fs vfs.IFS) (*chunkFile, error) {
	f, err := fs.Open(fp)
	if err != nil {
		return nil, err
	}
	return &chunkFile{file: f, fs: fs}, nil
}

// createChunkFile creates a new chunk file.
func createChunkFile(fp string, fs vfs.IFS) (*chunkFile, error) {
	f, err := fs.Create(fp)
	if err != nil {
		return nil, err
	}
	return &chunkFile{file: f, syncDir: true, dir: fs.PathDir(fp), fs: fs}, nil
}

// readAt reads from the file.
func (cf *chunkFile) readAt(data []byte, offset int64) (int, error) {
	return cf.file.ReadAt(data, offset)
}

// write writes the specified data to the chunk file.
func (cf *chunkFile) write(data []byte) (int, error) {
	return cf.file.Write(data)
}

// close closes the chunk file.
func (cf *chunkFile) close() error {
	if err := cf.file.Close(); err != nil {
		return err
	}
	if cf.syncDir {
		return fileutil.SyncDir(cf.dir, cf.fs)
	}
	return nil
}

// sync syncs the chunk file.
func (cf *chunkFile) sync() error {
	return cf.file.Sync()
}
