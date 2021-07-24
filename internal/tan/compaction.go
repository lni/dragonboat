// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"sort"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

// when compacting entries, a compaction update is written to the log to record
// the op. the compactedTo field of the index.entries and index.currEntries are
// set.
func (d *db) removeEntries(clusterID uint64, nodeID uint64, index uint64) error {
	return d.remove(clusterID, nodeID, index)
}

func (d *db) remove(clusterID uint64, nodeID uint64, index uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	update := getCompactionUpdate(clusterID, nodeID, index)
	buf := make([]byte, update.SizeUpperLimit())
	data := pb.MustMarshalTo(&update, buf)
	if err := d.doWriteLocked(update, data); err != nil {
		return err
	}
	nodeIndex := d.mu.state.getIndex(clusterID, nodeID)
	nodeIndex.currEntries.setCompactedTo(index)
	nodeIndex.entries.setCompactedTo(index)
	return d.compactionLocked(nodeIndex)
}

func (d *db) compactionLocked(index *nodeIndex) error {
	if obsolete := index.compaction(); len(obsolete) > 0 {
		obsolete = d.mu.state.getObsolete(obsolete)
		if len(obsolete) > 0 {
			ve := versionEdit{
				deletedFiles: make(map[deletedFileEntry]*fileMetadata),
			}
			for _, fn := range obsolete {
				ve.deletedFiles[deletedFileEntry{fn}] = &fileMetadata{fileNum: fn}
			}
			d.mu.versions.logLock()
			if err := d.mu.versions.logAndApply(&ve, d.dataDir); err != nil {
				return err
			}
		}
	}
	d.updateReadStateLocked(nil)
	d.notifyDeleteObsoleteWorker()
	return nil
}

func merge(a, b []fileNum) []fileNum {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i] != a[n-1] {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}

func mergeFileMetas(a, b []*fileMetadata) []*fileMetadata {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i].fileNum < a[j].fileNum
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i].fileNum != a[n-1].fileNum {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}

func (d *db) scanObsoleteFiles(list []string) {
	manifestFileNum := d.mu.versions.manifestFileNum
	liveFiles := d.mu.versions.currentVersion().files

	var obsoleteTables []*fileMetadata
	var obsoleteManifests []fileNum

	for _, filename := range list {
		fileType, fileNum, ok := parseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch fileType {
		case fileTypeManifest:
			if fileNum == manifestFileNum {
				continue
			}
			obsoleteManifests = append(obsoleteManifests, fileNum)
		case fileTypeLog:
			if _, ok := liveFiles[fileNum]; ok {
				continue
			}
			fileMeta := &fileMetadata{
				fileNum: fileNum,
			}
			obsoleteTables = append(obsoleteTables, fileMeta)
		default:
			// Don't delete files we don't know about.
			continue
		}
	}
	d.mu.versions.obsoleteTables = mergeFileMetas(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.obsoleteManifests = merge(d.mu.versions.obsoleteManifests, obsoleteManifests)
}

func (d *db) notifyDeleteObsoleteWorker() {
	select {
	case d.deleteObsoleteCh <- struct{}{}:
	default:
	}
}

func (d *db) deleteObsoleteWorkerMain() {
	for {
		select {
		case <-d.deleteobsoleteWorker.ShouldStop():
			return
		case <-d.deleteObsoleteCh:
			if err := d.deleteObsoleteFiles(); err != nil {
				panicNow(err)
			}
		}
	}
}

func (d *db) deleteObsoleteFiles() error {
	d.mu.Lock()
	obsoleteManifests := d.mu.versions.obsoleteManifests
	d.mu.versions.obsoleteManifests = nil
	obsoleteTables := d.mu.versions.obsoleteTables
	d.mu.versions.obsoleteTables = nil
	d.mu.Unlock()

	for _, fn := range obsoleteManifests {
		filename := makeFilename(d.opts.FS, d.dirname, fileTypeManifest, fn)
		if err := d.opts.FS.RemoveAll(filename); err != nil {
			return err
		}
	}
	for _, meta := range obsoleteTables {
		filename := makeFilename(d.opts.FS, d.dirname, fileTypeLog, meta.fileNum)
		indexFilename := makeFilename(d.opts.FS, d.dirname, fileTypeIndex, meta.fileNum)
		if err := d.opts.FS.RemoveAll(filename); err != nil {
			return err
		}
		if err := d.opts.FS.RemoveAll(indexFilename); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) removeAll(clusterID uint64, nodeID uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.removeAllLocked(clusterID, nodeID, false)
}

func (d *db) importSnapshot(clusterID uint64,
	nodeID uint64, ss pb.Snapshot) error {
	// TODO: need to remove the bootstrap record first
	return d.installSnapshot(clusterID, nodeID, ss)
}

func (d *db) installSnapshot(clusterID uint64,
	nodeID uint64, ss pb.Snapshot) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.removeAllLocked(clusterID, nodeID, true); err != nil {
		return err
	}
	update := pb.Update{
		ClusterID: clusterID,
		NodeID:    nodeID,
		State: pb.State{
			Commit: ss.Index,
			Term:   ss.Term,
		},
		Snapshot: ss,
	}
	buf := make([]byte, update.SizeUpperLimit())
	data := pb.MustMarshalTo(&update, buf)
	return d.doWriteLocked(update, data)
}

func (d *db) removeAllLocked(clusterID uint64, nodeID uint64, newLog bool) error {
	if newLog {
		if err := d.createNewLog(); err != nil {
			return err
		}
	}
	index := d.mu.state.getIndex(clusterID, nodeID)
	index.removeAll()
	v := d.mu.versions.currentVersion()
	ve := versionEdit{
		deletedFiles: make(map[deletedFileEntry]*fileMetadata),
	}
	for fn := range v.files {
		if fn != d.mu.versions.manifestFileNum && fn != d.mu.logNum {
			ve.deletedFiles[deletedFileEntry{fn}] = &fileMetadata{fileNum: fn}
		}
	}
	d.mu.versions.logLock()
	if err := d.mu.versions.logAndApply(&ve, d.dataDir); err != nil {
		return err
	}
	d.updateReadStateLocked(nil)
	d.notifyDeleteObsoleteWorker()
	return nil
}

// when compacting entries, a compaction update is written to the log to record
// the op.
func isCompactionUpdate(update pb.Update) (uint64, bool) {
	isCompaction := update.State.Term == compactionFlag
	if len(update.EntriesToSave) == 0 && pb.IsEmptySnapshot(update.Snapshot) &&
		isCompaction {
		return update.State.Commit, true
	}
	return 0, false
}

func getCompactionUpdate(clusterID uint64, nodeID uint64, index uint64) pb.Update {
	return pb.Update{
		ClusterID: clusterID,
		NodeID:    nodeID,
		State:     pb.State{Commit: index, Term: compactionFlag},
	}
}
