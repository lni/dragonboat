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
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"
)

// fileMetadata holds the metadata for an on-disk table.
type fileMetadata struct {
	// refs is the reference count for the file: incremented when a file is added
	// to a version and decremented when the version is unreferenced. The file is
	// obsolete when the reference count falls to zero.
	refs int32
	// fileNum is the file number.
	fileNum fileNum
}

// version is a collection of file metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type version struct {
	refcnt int32

	files map[fileNum]*fileMetadata

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	deleted func(obsolete []*fileMetadata)

	// The list the version is linked into.
	list *versionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *version
}

func (v *version) clone() map[fileNum]*fileMetadata {
	fs := make(map[fileNum]*fileMetadata)
	for _, f := range v.files {
		atomic.AddInt32(&f.refs, 1)
		fs[f.fileNum] = f
	}
	return fs
}

// refs returns the number of references to the version.
func (v *version) refs() int32 {
	return atomic.LoadInt32(&v.refcnt)
}

// ref increments the version refcount.
func (v *version) ref() {
	atomic.AddInt32(&v.refcnt, 1)
}

// unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *version) unref() {
	if atomic.AddInt32(&v.refcnt, -1) == 0 {
		obsolete := v.unrefFiles()
		l := v.list
		l.mu.Lock()
		l.remove(v)
		v.deleted(obsolete)
		l.mu.Unlock()
	}
}

// unrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *version) unrefLocked() {
	if atomic.AddInt32(&v.refcnt, -1) == 0 {
		v.list.remove(v)
		v.deleted(v.unrefFiles())
	}
}

func (v *version) unrefFiles() []*fileMetadata {
	var obsolete []*fileMetadata
	for _, f := range v.files {
		if atomic.AddInt32(&f.refs, -1) == 0 {
			obsolete = append(obsolete, f)
		}
	}
	return obsolete
}

// checkConsistency checks that all of the files listed in the version exist
func (v *version) checkConsistency(dirname string, fs vfs.FS) error {
	var buf bytes.Buffer
	var args []interface{}
	for fileNum := range v.files {
		path := makeFilename(fs, dirname, fileTypeLog, fileNum)
		if _, err := fs.Stat(path); err != nil {
			buf.WriteString("%s: %v\n")
			args = append(args, errors.Safe(fileNum), err)
			continue
		}
	}
	if buf.Len() == 0 {
		return nil
	}
	return errors.Errorf(buf.String(), args...)
}

// versionList holds a list of versions. The versions are ordered from oldest
// to newest.
type versionList struct {
	mu   *sync.Mutex
	root version
}

// init initializes the version list.
func (l *versionList) init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// empty returns true if the list is empty, and false otherwise.
func (l *versionList) empty() bool {
	return l.root.next == &l.root
}

// back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *versionList) back() *version {
	return l.root.prev
}

// pushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *versionList) pushBack(v *version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("pebble: version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
}

// remove removes the specified version from the list.
func (l *versionList) remove(v *version) {
	if v == &l.root {
		panic("pebble: cannot remove version list root node")
	}
	if v.list != l {
		panic("pebble: version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}
