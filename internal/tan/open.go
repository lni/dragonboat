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
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/syncutil"
)

// open opens the tan db located in the folder called dirname.
func open(name string, dirname string, opts *Options) (*db, error) {
	opts = opts.EnsureDefaults()
	d := &db{
		name:                 name,
		closedCh:             make(chan struct{}),
		opts:                 opts,
		dirname:              dirname,
		deleteObsoleteCh:     make(chan struct{}, 1),
		deleteobsoleteWorker: syncutil.NewStopper(),
	}
	d.mu.versions = &versionSet{}
	d.mu.nodeStates = newNodeStates()

	d.mu.Lock()
	defer d.mu.Unlock()

	var err error
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	currentName := makeFilename(opts.FS, dirname, fileTypeCurrent, 0)
	if _, err := opts.FS.Stat(currentName); oserror.IsNotExist(err) {
		// Create the DB if it did not already exist.
		plog.Infof("%s creating a new tan db", d.id())
		if err := d.mu.versions.create(dirname, opts, d.dataDir, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, errors.Wrapf(err, "tan: database %q", dirname)
	} else {
		// Load the version set.
		plog.Infof("%s loading an existing tan db", d.id())
		if err := d.mu.versions.load(dirname, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
		if err := d.mu.versions.currentVersion().checkConsistency(dirname, opts.FS); err != nil {
			return nil, err
		}
	}

	ls, err := opts.FS.List(d.dirname)
	if err != nil {
		return nil, err
	}
	plog.Infof("%s on disk files %v", d.id(), ls)
	type fileNumAndName struct {
		num  fileNum
		name string
	}
	currentVersion := d.mu.versions.currentVersion()
	var indexFiles []fileNumAndName
	logFiles := make(map[fileNum]fileNumAndName)
	lastLogNum := fileNum(0)
	for _, filename := range ls {
		ft, fn, ok := parseFilename(opts.FS, filename)
		if !ok {
			continue
		}
		// Don't reuse any obsolete file numbers
		if d.mu.versions.nextFileNum <= fn {
			d.mu.versions.nextFileNum = fn + 1
		}

		switch ft {
		case fileTypeLog:
			if _, ok := currentVersion.files[fn]; ok {
				if fn > lastLogNum {
					lastLogNum = fn
				}
				logFiles[fn] = fileNumAndName{fn, filename}
			}
		case fileTypeLogTemp:
			fallthrough
		case fileTypeBootstrapTemp:
			fallthrough
		case fileTypeIndexTemp:
			fallthrough
		case fileTypeTemp:
			if err := opts.FS.Remove(opts.FS.PathJoin(dirname, filename)); err != nil {
				return nil, err
			}
		case fileTypeIndex:
			if _, ok := currentVersion.files[fn]; ok {
				indexFiles = append(indexFiles, fileNumAndName{fn, filename})
			}
		}
	}
	sort.Slice(indexFiles, func(i, j int) bool {
		return indexFiles[i].num < indexFiles[j].num
	})
	for _, indexFile := range indexFiles {
		if _, ok := logFiles[indexFile.num]; !ok {
			plog.Panicf("log file %d missing", indexFile.num)
		}
		if err := d.mu.nodeStates.load(dirname, indexFile.num, opts.FS); err != nil {
			// TODO: we can actually regenerate the index when it is corrupted
			return nil, err
		}
		delete(logFiles, indexFile.num)
	}
	plog.Infof("%s logFiles to rebuild: %v", d.id(), logFiles)
	plog.Infof("%s indexFiles: %v", d.id(), indexFiles)
	for _, lf := range logFiles {
		if len(indexFiles) == 0 || lf.num > indexFiles[len(indexFiles)-1].num {
			if lf.num != lastLogNum {
				plog.Panicf("more than one log file have index missing")
			}
			if err := d.rebuildLogAndIndex(lf.num); err != nil {
				return nil, err
			}
		}
	}

	if err := d.createNewLog(); err != nil {
		return nil, err
	}
	d.updateReadStateLocked(nil)

	// indexes are populated when d.mu.state.load() is called above
	for _, index := range d.mu.nodeStates.indexes {
		if index.entries.compactedTo > 0 {
			if err := d.compactionLocked(index); err != nil {
				return nil, err
			}
		}
	}

	d.deleteobsoleteWorker.RunWorker(func() {
		d.deleteObsoleteWorkerMain()
	})
	d.scanObsoleteFiles(ls)
	d.notifyDeleteObsoleteWorker()
	return d, nil
}

func (d *db) id() string {
	return d.name
}

func (d *db) createNewLog() error {
	if d.mu.logFile != nil {
		if err := d.mu.logFile.Close(); err != nil {
			return err
		}
	}
	logNum := d.mu.versions.getNextFileNum()
	logName := makeFilename(d.opts.FS, d.dirname, fileTypeLog, logNum)
	logFile, err := d.opts.FS.Create(logName)
	if err != nil {
		return err
	}
	if err := prealloc(logFile, d.opts.MaxLogFileSize+indexBlockSize); err != nil {
		return err
	}
	if err := d.dataDir.Sync(); err != nil {
		return err
	}
	d.mu.logFile = logFile
	d.mu.logWriter = newWriter(logFile)
	d.mu.logNum = logNum
	d.mu.offset = 0
	ve := versionEdit{
		newFiles: []newFileEntry{{meta: &fileMetadata{fileNum: logNum}}},
	}
	d.mu.versions.logLock()
	return d.mu.versions.logAndApply(&ve, d.dataDir)
}

func (d *db) rebuildLogAndIndex(logNum fileNum) (err error) {
	plog.Infof("%s rebuildLogAndIndex, logNum %d", d.id(), logNum)
	f := func(u pb.Update, offset int64) bool {
		d.updateIndex(u, offset, logNum)
		return true
	}
	if err := d.readLog(indexEntry{fileNum: logNum}, f); err != nil {
		if !IsInvalidRecord(err) {
			return err
		}
		if err := d.rebuildLog(logNum); err != nil {
			return err
		}
	}
	// save to index file
	return d.mu.nodeStates.save(d.dirname, d.dataDir, logNum, d.opts.FS)
}

func (d *db) rebuildLog(logNum fileNum) (err error) {
	// it is possible to have the last log file to contain a corrupted chunk or
	// block on the tail, e.g. power got cut after partially written chunk or
	// block. for those situations, the log file itself is totally fine, we just
	// need to remove the final chunk or block.
	// in theory, we should be able to just truncate the log file to the last
	// reported offset. however, for simplicity, let's just copy the log and skip
	// the last broken chunk or block.
	fn := makeFilename(d.opts.FS, d.dirname, fileTypeLogTemp, logNum)
	ln := makeFilename(d.opts.FS, d.dirname, fileTypeLog, logNum)
	f, err := d.opts.FS.Create(fn)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.Sync())
		err = firstError(err, f.Close())
		err = firstError(err, d.opts.FS.Rename(fn, ln))
		err = firstError(err, d.dataDir.Sync())
	}()
	w := newWriter(f)
	defer func() {
		err = firstError(err, w.close())
	}()
	buf := make([]byte, defaultBufferSize)
	var herr error
	var newOffset int64
	h := func(u pb.Update, offset int64) bool {
		sz := u.SizeUpperLimit()
		if sz > len(buf) {
			buf = make([]byte, sz)
		}
		data := pb.MustMarshalTo(&u, buf)
		updatedOffset, err := w.writeRecord(data)
		if err != nil {
			herr = err
			return false
		}
		if newOffset != offset {
			plog.Panicf("offset changed, %d, %d", offset, newOffset)
		}
		newOffset = updatedOffset
		return true
	}
	if err := d.readLog(indexEntry{fileNum: logNum}, h); err != nil {
		if !IsInvalidRecord(err) {
			return err
		}
	}
	return herr
}

func (d *db) saveIndex() error {
	return d.mu.nodeStates.save(d.dirname, d.dataDir, d.mu.logNum, d.opts.FS)
}

func (d *db) close() error {
	d.deleteobsoleteWorker.Stop()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	d.closed.Store(errors.WithStack(ErrClosed))
	close(d.closedCh)

	var err error
	err = firstError(err, d.mu.logWriter.close())
	err = firstError(err, d.saveIndex())
	// Note that versionSet.close() only closes the MANIFEST. The versions list
	// is still valid for the checks below.
	err = firstError(err, d.mu.versions.close())
	plog.Infof("%s is being closed, logNum %d", d.id(), d.mu.logNum)
	err = firstError(err, d.mu.logFile.Close())
	err = firstError(err, d.dataDir.Close())

	if err == nil {
		d.readState.val.unrefLocked()
	}
	return err
}
