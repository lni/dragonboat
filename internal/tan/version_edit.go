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
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

var errCorruptManifest = errors.New("corrupt manifest")

type byteReader interface {
	io.ByteReader
	io.Reader
}

// Tags for the versionEdit disk format.
const (
	tagNextFileNumber = 1
	tagDeletedFile    = 2
	tagNewFile        = 3
)

// deletedFileEntry holds the state for a file deletion. The file itself might
// still be referenced by another level.
type deletedFileEntry struct {
	fileNum fileNum
}

// newFileEntry holds the state for a new file.
type newFileEntry struct {
	meta *fileMetadata
}

// versionEdit holds the state for an edit to a Version along with other
// on-disk state
type versionEdit struct {
	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST and OPTIONS files.
	nextFileNum  fileNum
	deletedFiles map[deletedFileEntry]*fileMetadata
	newFiles     []newFileEntry
}

// decode decodes an edit from the specified reader.
func (v *versionEdit) decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	d := versionEditDecoder{br}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch tag {
		case tagNextFileNumber:
			n, err := d.readfileNum()
			if err != nil {
				return err
			}
			v.nextFileNum = n

		case tagDeletedFile:
			fileNum, err := d.readfileNum()
			if err != nil {
				return err
			}
			if v.deletedFiles == nil {
				v.deletedFiles = make(map[deletedFileEntry]*fileMetadata)
			}
			v.deletedFiles[deletedFileEntry{fileNum}] = nil

		case tagNewFile:
			fileNum, err := d.readfileNum()
			if err != nil {
				return err
			}
			v.newFiles = append(v.newFiles, newFileEntry{
				meta: &fileMetadata{
					fileNum: fileNum,
				},
			})

		default:
			return errCorruptManifest
		}
	}
	return nil
}

// encode encodes an edit to the specified writer.
func (v *versionEdit) encode(w io.Writer) error {
	e := versionEditEncoder{new(bytes.Buffer)}

	if v.nextFileNum != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(uint64(v.nextFileNum))
	}
	for x := range v.deletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.fileNum))
	}
	for _, x := range v.newFiles {
		e.writeUvarint(tagNewFile)
		e.writeUvarint(uint64(x.meta.fileNum))
	}
	_, err := w.Write(e.Bytes())
	return err
}

type versionEditDecoder struct {
	byteReader
}

func (d versionEditDecoder) readfileNum() (fileNum, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	return fileNum(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptManifest
		}
		return 0, err
	}
	return u, nil
}

type versionEditEncoder struct {
	*bytes.Buffer
}

func (e versionEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

// bulkVersionEdit summarizes the files added and deleted from a set of version
// edits.
type bulkVersionEdit struct {
	added   []*fileMetadata
	deleted map[fileNum]*fileMetadata

	// AddedByFileNum maps file number to file metadata for all added files
	// from accumulated version edits. AddedByFileNum is only populated if set
	// to non-nil by a caller. It must be set to non-nil when replaying
	// version edits read from a MANIFEST (as opposed to VersionEdits
	// constructed in-memory).  While replaying a MANIFEST file,
	// VersionEdit.deletedFiles map entries have nil values, because the
	// on-disk deletion record encodes only the file number. Accumulate
	// uses AddedByFileNum to correctly populate the bulkVersionEdit's Deleted
	// field with non-nil *fileMetadata.
	addedByFileNum map[fileNum]*fileMetadata
}

// accumulate adds the file addition and deletions in the specified version
// edit to the bulk edit's internal state.
func (b *bulkVersionEdit) accumulate(ve *versionEdit) error {
	for fn, m := range ve.deletedFiles {
		if b.deleted == nil {
			b.deleted = make(map[fileNum]*fileMetadata)
		}
		if m == nil {
			// m is nil only when replaying a MANIFEST.
			if b.addedByFileNum == nil {
				return errors.Errorf("deleted file %s's metadata is absent and bve.addedByFileNum is nil",
					fn.fileNum)
			}
			m = b.addedByFileNum[fn.fileNum]
			if m == nil {
				return errors.Errorf("file deleted %s before it was inserted", fn.fileNum)
			}
		}
		b.deleted[m.fileNum] = m
	}

	for _, nf := range ve.newFiles {
		if b.deleted != nil {
			// A new file should not have been deleted in this or a preceding
			// VersionEdit at the same level (though files can move across levels).
			if _, ok := b.deleted[nf.meta.fileNum]; ok {
				return errors.Errorf("file deleted %s before it was inserted", nf.meta.fileNum)
			}
		}
		b.added = append(b.added, nf.meta)
		if b.addedByFileNum != nil {
			b.addedByFileNum[nf.meta.fileNum] = nf.meta
		}
	}
	return nil
}

// apply applies the delta b to the current version to produce a new
// version. The new version is consistent with respect to the comparer cmp.
//
// curr may be nil, which is equivalent to a pointer to a zero version.
//
// On success, a map of zombie files containing the file numbers and sizes of
// deleted files is returned. These files are considered zombies because they
// are no longer referenced by the returned Version, but cannot be deleted from
// disk as they are still in use by the incoming Version.
func (b *bulkVersionEdit) apply(curr *version,
) (_ *version, zombies map[fileNum]uint64, _ error) {
	addZombie := func(fn fileNum) {
		if zombies == nil {
			zombies = make(map[fileNum]uint64)
		}
		zombies[fn] = 0
	}
	// The remove zombie function is used to handle tables that are moved from
	// one level to another during a version edit (i.e. a "move" compaction).
	removeZombie := func(fileNum fileNum) {
		if zombies != nil {
			delete(zombies, fileNum)
		}
	}

	v := new(version)
	if curr == nil || curr.files == nil {
		v.files = make(map[fileNum]*fileMetadata)
	} else {
		v.files = curr.clone()
	}

	if len(b.added) == 0 && len(b.deleted) == 0 {
		return v, zombies, nil
	}

	// Some edits on this level.
	addedFiles := b.added
	deletedMap := b.deleted
	if n := len(v.files) + len(addedFiles); n == 0 {
		return nil, nil, errors.New("No current or added files but have deleted files")
	}

	// NB: addedFiles may be empty and it also is not necessarily
	// internally consistent: it does not reflect deletions in deletedMap.

	for _, f := range deletedMap {
		addZombie(f.fileNum)
		f, ok := v.files[f.fileNum]
		if ok {
			// Deleting a file from the B-Tree may decrement its
			// reference count. However, because we cloned the
			// previous level's B-Tree, this should never result in a
			// file's reference count dropping to zero.
			if atomic.AddInt32(&f.refs, -1) == 0 {
				err := errors.Errorf("file %s obsolete during B-Tree removal", f.fileNum)
				return nil, nil, err
			}
			delete(v.files, f.fileNum)
		}
	}

	for _, f := range addedFiles {
		if _, ok := deletedMap[f.fileNum]; ok {
			// Already called addZombie on this file in the preceding
			// loop, so we don't need to do it here.
			continue
		}
		atomic.AddInt32(&f.refs, 1)
		v.files[f.fileNum] = f
		removeZombie(f.fileNum)
	}
	return v, zombies, nil
}
