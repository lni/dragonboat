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
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"
)

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new version is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the MANIFEST file, which is replayed at startup.
type versionSet struct {
	maxManifestFileSize int64
	// Immutable fields.
	dirname string
	// Set to DB.mu.
	mu *sync.Mutex
	fs vfs.FS
	// Mutable fields.
	versions versionList
	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(obsolete []*fileMetadata)
	obsoleteTables    []*fileMetadata
	obsoleteManifests []fileNum
	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables map[fileNum]uint64 // filenum -> size
	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	nextFileNum fileNum
	// The current manifest file number.
	manifestFileNum fileNum
	manifestFile    vfs.File
	manifest        *writer
	writing         bool
	writerCond      sync.Cond
}

func (vs *versionSet) init(dirname string, opts *Options, mu *sync.Mutex) {
	vs.maxManifestFileSize = opts.MaxManifestFileSize
	vs.dirname = dirname
	vs.mu = mu
	vs.writerCond.L = mu
	vs.fs = opts.FS
	vs.versions.init(mu)
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = make(map[fileNum]uint64)
	vs.nextFileNum = 1
}

// create creates a version set for a fresh DB.
func (vs *versionSet) create(dirname string, opt *Options, dir vfs.File,
	mu *sync.Mutex) error {
	vs.init(dirname, opt, mu)
	newVersion := &version{}
	vs.append(newVersion)

	// Note that a "snapshot" version edit is written to the manifest when it is
	// created.
	vs.manifestFileNum = vs.getNextFileNum()
	if err := vs.createManifest(vs.dirname, vs.manifestFileNum, vs.nextFileNum); err != nil {
		return err
	}
	if err := vs.manifest.flush(); err != nil {
		return err
	}
	if err := vs.manifestFile.Sync(); err != nil {
		return err
	}
	if err := setCurrentFile(vs.dirname, vs.fs, vs.manifestFileNum); err != nil {
		return err
	}
	return dir.Sync()
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string,
	opt *Options, mu *sync.Mutex) (err error) {
	vs.init(dirname, opt, mu)
	// Read the CURRENT file to find the current manifest file.
	current, err := vs.fs.Open(makeFilename(vs.fs, dirname, fileTypeCurrent, 0))
	if err != nil {
		return errors.Wrapf(err, "could not open CURRENT file for DB %q", dirname)
	}
	defer func() {
		err = firstError(err, current.Close())
	}()
	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return errors.Errorf("CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return errors.Errorf("CURRENT file for DB %q is too large", dirname)
	}
	r := newReader(current, fileNum(0))
	cr, err := r.next()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, cr); err != nil {
		return err
	}
	b := buf.Bytes()
	if b[len(b)-1] != '\n' {
		return errors.Errorf("CURRENT file for DB %q is malformed", dirname)
	}
	b = bytes.TrimSpace(b)

	var ok bool
	if _, vs.manifestFileNum, ok = parseFilename(vs.fs, string(b)); !ok {
		return errors.Errorf("MANIFEST name %q is malformed", errors.Safe(b))
	}

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	bve.addedByFileNum = make(map[fileNum]*fileMetadata)
	manifest, err := vs.fs.Open(vs.fs.PathJoin(dirname, string(b)))
	if err != nil {
		return errors.Wrapf(err, "could not open manifest file %q for DB %q",
			errors.Safe(b), dirname)
	}
	defer func() {
		err = firstError(err, manifest.Close())
	}()
	rr := newReader(manifest, 0 /* logNum */)
	for {
		r, err := rr.next()
		if err == io.EOF || IsInvalidRecord(err) {
			// FIXME:
			// not to tolerate actually corrupted manifest
			break
		}
		if err != nil {
			return errors.Wrapf(err, "error when loading manifest file %q",
				errors.Safe(b))
		}
		var ve versionEdit
		err = ve.decode(r)
		if err != nil {
			// Break instead of returning an error if the record is corrupted
			// or invalid.
			if err == io.EOF || IsInvalidRecord(err) {
				break
			}
			return err
		}
		if err := bve.accumulate(&ve); err != nil {
			return err
		}
		if ve.nextFileNum != 0 {
			vs.nextFileNum = ve.nextFileNum
		}
	}

	newVersion, _, err := bve.apply(nil)
	if err != nil {
		return err
	}
	vs.append(newVersion)
	return nil
}

func (vs *versionSet) close() error {
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// logLock locks the manifest for writing. The lock must be released by either
// a call to logUnlock or logAndApply.
//
// DB.mu must be held when calling this method, but the mutex may be dropped and
// re-acquired during the course of this method.
func (vs *versionSet) logLock() {
	// Wait for any existing writing to the manifest to complete, then mark the
	// manifest as busy.
	for vs.writing {
		vs.writerCond.Wait()
	}
	vs.writing = true
}

// logUnlock releases the lock for manifest writing.
//
// DB.mu must be held when calling this method.
func (vs *versionSet) logUnlock() {
	if !vs.writing {
		panic("MANIFEST not locked for writing")
	}
	vs.writing = false
	vs.writerCond.Signal()
}

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version.
//
// DB.mu must be held when calling this method and will be released temporarily
// while performing file I/O. Requires that the manifest is locked for writing
// (see logLock). Will unconditionally release the manifest lock (via
// logUnlock) even if an error occurs.
//
// inProgressCompactions is called while DB.mu is held, to get the list of
// in-progress compactions.
func (vs *versionSet) logAndApply(
	ve *versionEdit,
	dir vfs.File,
) error {
	if !vs.writing {
		panic("MANIFEST not locked for writing")
	}
	defer vs.logUnlock()

	// This is the next manifest filenum, but if the current file is too big we
	// will write this ve to the next file which means what ve encodes is the
	// current filenum and not the next one.
	//
	// TODO(sbhola): figure out why this is correct and update comment.
	ve.nextFileNum = vs.nextFileNum

	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or the current one
	// is too large.
	var newManifestFileNum fileNum
	if vs.manifest == nil || vs.manifest.size() >= vs.maxManifestFileSize {
		newManifestFileNum = vs.getNextFileNum()
	}

	// Grab certain values before releasing vs.mu, in case createManifest() needs
	// to be called.
	nextFileNum := vs.nextFileNum

	var zombies map[fileNum]uint64
	if err := func() error {
		var bve bulkVersionEdit
		if err := bve.accumulate(ve); err != nil {
			return err
		}

		var err error
		newVersion, zombies, err = bve.apply(currentVersion)
		if err != nil {
			return err
		}

		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum, nextFileNum); err != nil {
				return err
			}
		}

		w, err := vs.manifest.next()
		if err != nil {
			return err
		}
		// NB: Any error from this point on is considered fatal as we don't now if
		// the MANIFEST write occurred or not. Trying to determine that is
		// fraught. Instead we rely on the standard recovery mechanism run when a
		// database is open. In particular, that mechanism generates a new MANIFEST
		// and ensures it is synced.
		if err := ve.encode(w); err != nil {
			return err
		}
		if err := vs.manifest.flush(); err != nil {
			return err
		}
		if err := vs.manifestFile.Sync(); err != nil {
			return err
		}
		if newManifestFileNum != 0 {
			if err := setCurrentFile(vs.dirname, vs.fs, newManifestFileNum); err != nil {
				return err
			}
			if err := dir.Sync(); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	// Update the zombie tables set first, as installation of the new version
	// will unref the previous version which could result in addObsoleteLocked
	// being called.
	for fileNum, size := range zombies {
		vs.zombieTables[fileNum] = size
	}
	// Install the new version.
	vs.append(newVersion)
	if newManifestFileNum != 0 {
		if vs.manifestFileNum != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, vs.manifestFileNum)
		}
		vs.manifestFileNum = newManifestFileNum
	}

	return nil
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(
	dirname string, fileNum, nextFileNum fileNum,
) (err error) {
	var (
		filename     = makeFilename(vs.fs, dirname, fileTypeManifest, fileNum)
		manifestFile vfs.File
		manifest     *writer
	)
	defer func() {
		if manifest != nil {
			err = firstError(err, manifest.close())
		}
		if manifestFile != nil {
			err = firstError(err, manifestFile.Close())
		}
		if err != nil {
			err = firstError(err, vs.fs.Remove(filename))
		}
	}()
	manifestFile, err = vs.fs.Create(filename)
	if err != nil {
		return err
	}
	manifest = newWriter(manifestFile)

	snapshot := versionEdit{}
	cv := vs.currentVersion()
	for _, meta := range cv.files {
		snapshot.newFiles = append(snapshot.newFiles, newFileEntry{
			meta: meta,
		})
	}

	// When creating a version snapshot for an existing DB, this snapshot VersionEdit will be
	// immediately followed by another VersionEdit (being written in logAndApply()). That
	// VersionEdit always contains a LastSeqNum, so we don't need to include that in the snapshot.
	// But it does not necessarily include MinUnflushedLogNum, NextFileNum, so we initialize those
	// using the corresponding fields in the versionSet (which came from the latest preceding
	// VersionEdit that had those fields).
	snapshot.nextFileNum = nextFileNum

	w, err1 := manifest.next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.encode(w); err != nil {
		return err
	}

	if vs.manifest != nil {
		if err := vs.manifest.close(); err != nil {
			return err
		}
		vs.manifest = nil
	}
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
		vs.manifestFile = nil
	}

	vs.manifest, manifest = manifest, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (vs *versionSet) getNextFileNum() fileNum {
	x := vs.nextFileNum
	vs.nextFileNum++
	return x
}

func (vs *versionSet) append(v *version) {
	if v.refs() != 0 {
		panic("version should be unreferenced")
	}
	if !vs.versions.empty() {
		vs.versions.back().unrefLocked()
	}
	v.deleted = vs.obsoleteFn
	v.ref()
	vs.versions.pushBack(v)
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.back()
}

func (vs *versionSet) addObsoleteLocked(obsolete []*fileMetadata) {
	for _, fileMeta := range obsolete {
		// Note that the obsolete tables are no longer zombie by the definition of
		// zombie, but we leave them in the zombie tables map until they are
		// deleted from disk.
		if _, ok := vs.zombieTables[fileMeta.fileNum]; !ok {
			panic("MANIFEST obsolete table not marked as zombie")
		}
	}
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
}
