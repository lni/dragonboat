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
	"bytes"
	"crypto/md5"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
)

const (
	// SnapshotFlagFilename defines the filename of the snapshot flag file.
	SnapshotFlagFilename = "dragonboat.snapshot.message"
)

func getHash(data []byte) []byte {
	h := md5.New()
	if _, err := h.Write(data); err != nil {
		panic(err)
	}
	s := h.Sum(nil)
	return s[8:]
}

// CreateFlagFile creates a flag file in the specific location. The flag file
// contains the marshaled data of the specified protobuf message.
func CreateFlagFile(dir string, filename string, msg proto.Message) (err error) {
	fp := filepath.Join(dir, filename)
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if cerr := SyncDir(dir); err == nil {
			err = cerr
		}
	}()
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	h := getHash(data)
	n, err := f.Write(h)
	if err != nil {
		return err
	}
	if n != len(h) {
		return io.ErrShortWrite
	}
	n, err = f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return f.Sync()
}

// GetFlagFileContent gets the content of the flag file found in the specified
// location. The data of the flag file will be unmarshaled into the specified
// protobuf message.
func GetFlagFileContent(dir string, filename string, msg proto.Message) error {
	fp := filepath.Join(dir, filename)
	data, err := ioutil.ReadFile(filepath.Clean(fp))
	if err != nil {
		return err
	}
	if len(data) < 8 {
		panic("corrupted flag file")
	}
	h := data[:8]
	buf := data[8:]
	expectedHash := getHash(buf)
	if !bytes.Equal(h, expectedHash) {
		panic("corrupted flag file content")
	}
	return proto.Unmarshal(buf, msg)
}

// HasFlagFile returns a boolean value indicating whether flag file can be
// found in the specified location.
func HasFlagFile(dir string, filename string) bool {
	fp := filepath.Join(dir, filename)
	fi, err := os.Stat(fp)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

// RemoveFlagFile removes the specified flag file.
func RemoveFlagFile(dir string, filename string) error {
	fp := filepath.Join(dir, filename)
	return os.Remove(fp)
}
