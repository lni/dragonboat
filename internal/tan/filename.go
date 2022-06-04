// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
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

package tan

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/lni/vfs"
)

var (
	bsFilenameRe    = regexp.MustCompile(`^BOOTSTRAP-([0-9]+)-([0-9]+)$`)
	tmpBSFilenameRe = regexp.MustCompile(`^BOOTSTRAP-[0-9]+-[0-9]+.tmp$`)
)

type fileNum uint64

// String returns a string representation of the file number.
func (fn fileNum) String() string { return fmt.Sprintf("%06d", fn) }

// fileType enumerates the types of files found in a DB.
type fileType int

// The FileType enumeration.
const (
	fileTypeLog fileType = iota
	fileTypeLogTemp
	fileTypeIndex
	fileTypeIndexTemp
	fileTypeBootstrap
	fileTypeLock
	fileTypeManifest
	fileTypeCurrent
	fileTypeTemp
	fileTypeBootstrapTemp
)

func setCurrentFile(dirname string, fs vfs.FS, fileNum fileNum) (err error) {
	newFilename := makeFilename(fs, dirname, fileTypeCurrent, fileNum)
	oldFilename := makeFilename(fs, dirname, fileTypeTemp, fileNum)
	if err := fs.RemoveAll(oldFilename); err != nil {
		return err
	}
	f, err := fs.Create(oldFilename)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.Sync())
		err = firstError(err, f.Close())
		if err == nil {
			err = fs.Rename(oldFilename, newFilename)
		}
	}()
	v := []byte(fmt.Sprintf("MANIFEST-%s\n", fileNum))
	w := newWriter(f)
	defer func() {
		err = firstError(err, w.close())
	}()
	if _, err = w.writeRecord(v); err != nil {
		return err
	}
	return nil
}

func makeBootstrapFilename(fs vfs.FS,
	dirname string, shardID uint64, replicaID uint64, tmp bool) string {
	pattern := "BOOTSTRAP-%d-%d"
	if tmp {
		pattern = "BOOTSTRAP-%d-%d.tmp"
	}
	return fs.PathJoin(dirname, fmt.Sprintf(pattern, shardID, replicaID))
}

func parseBootstrapFilename(filename string) (uint64, uint64, bool) {
	if parts := bsFilenameRe.FindStringSubmatch(filename); len(parts) == 3 {
		shardID, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		replicaID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return shardID, replicaID, true
	}
	return 0, 0, false
}

// makeFilename builds a filename from components.
func makeFilename(fs vfs.FS, dirname string, fileType fileType, fileNum fileNum) string {
	switch fileType {
	case fileTypeLog:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.log", fileNum))
	case fileTypeLogTemp:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.logtmp", fileNum))
	case fileTypeIndex:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.index", fileNum))
	case fileTypeIndexTemp:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.idxtmp", fileNum))
	case fileTypeBootstrap:
		panic("use makeBootstrapFilename")
	case fileTypeLock:
		return fs.PathJoin(dirname, "LOCK")
	case fileTypeManifest:
		return fs.PathJoin(dirname, fmt.Sprintf("MANIFEST-%s", fileNum))
	case fileTypeCurrent:
		return fs.PathJoin(dirname, "CURRENT")
	case fileTypeTemp:
		return fs.PathJoin(dirname, fmt.Sprintf("CURRENT-%s.dbtmp", fileNum))
	case fileTypeBootstrapTemp:
		panic("use makeBootstrapFilename")
	}
	panic("unreachable")
}

// parseFilename parses the components from a filename.
func parseFilename(fs vfs.FS, filename string) (fileType fileType, fileNum fileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return fileTypeCurrent, 0, true
	case filename == "LOCK":
		return fileTypeLock, 0, true
	case filename == "BOOTSTRAP":
		panic("unexpected BOOTSTRAP file")
	case filename == "BOOTSTRAP.tmp":
		panic("unexpected BOOTSTRAP.tmp")
	case strings.HasPrefix(filename, "MANIFEST-"):
		fileNum, ok = parseFileNum(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return fileTypeManifest, fileNum, true
	case strings.HasPrefix(filename, "CURRENT-") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("CURRENT-"):], ".dbtmp")
		fileNum, ok = parseFileNum(s)
		if !ok {
			break
		}
		return fileTypeTemp, fileNum, ok
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		fileNum, ok = parseFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "log":
			return fileTypeLog, fileNum, true
		case "index":
			return fileTypeIndex, fileNum, true
		case "idxtmp":
			return fileTypeIndexTemp, fileNum, true
		case "logtmp":
			return fileTypeLogTemp, fileNum, true
		}
	}
	return 0, fileNum, false
}

func parseFileNum(s string) (fileNum, bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return fileNum(u), true
}
