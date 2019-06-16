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

package rsm

// From identifies a component in the system.
type From uint64

const (
	// FromNodeHost indicates the data store has been loaded by or offloaded from
	// nodehost.
	FromNodeHost From = iota
	// FromStepWorker indicates that the data store has been loaded by or
	// offloaded from the step worker.
	FromStepWorker
	// FromCommitWorker indicates that the data store has been loaded by or
	// offloaded from the commit worker.
	FromCommitWorker
	// FromSnapshotWorker indicates that the data store has been loaded by or
	// offloaded from the snapshot worker.
	FromSnapshotWorker
)

var fromNames = [...]string{
	"FromNodeHost",
	"FromStepWorker",
	"FromCommitWorker",
	"FromSnapshotWorker",
}

func (f From) String() string {
	return fromNames[uint64(f)]
}

// OffloadedStatus is used for tracking whether the managed data store has been
// offloaded from various system components.
type OffloadedStatus struct {
	clusterID                   uint64
	nodeID                      uint64
	readyToDestroy              bool
	destroyed                   bool
	offloadedFromNodeHost       bool
	offloadedFromStepWorker     bool
	offloadedFromCommitWorker   bool
	offloadedFromSnapshotWorker bool
	loadedByStepWorker          bool
	loadedByCommitWorker        bool
	loadedBySnapshotWorker      bool
}

// ReadyToDestroy returns a boolean value indicating whether the the managed data
// store is ready to be destroyed.
func (o *OffloadedStatus) ReadyToDestroy() bool {
	return o.readyToDestroy
}

// Destroyed returns a boolean value indicating whether the belonging object
// has been destroyed.
func (o *OffloadedStatus) Destroyed() bool {
	return o.destroyed
}

// SetDestroyed set the destroyed flag to be true
func (o *OffloadedStatus) SetDestroyed() {
	o.destroyed = true
}

// SetLoaded marks the managed data store as loaded from the specified
// component.
func (o *OffloadedStatus) SetLoaded(from From) {
	if o.offloadedFromNodeHost {
		if from == FromStepWorker ||
			from == FromCommitWorker ||
			from == FromSnapshotWorker {
			plog.Panicf("loaded from %v after offloaded from nodehost", from)
		}
	}
	if from == FromNodeHost {
		panic("not suppose to get loaded notification from nodehost")
	} else if from == FromStepWorker {
		o.loadedByStepWorker = true
	} else if from == FromCommitWorker {
		o.loadedByCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.loadedBySnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
}

// SetOffloaded marks the managed data store as offloaded from the specified
// component.
func (o *OffloadedStatus) SetOffloaded(from From) {
	if from == FromNodeHost {
		o.offloadedFromNodeHost = true
	} else if from == FromStepWorker {
		o.offloadedFromStepWorker = true
	} else if from == FromCommitWorker {
		o.offloadedFromCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.offloadedFromSnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
	if from == FromNodeHost {
		if !o.loadedByStepWorker {
			o.offloadedFromStepWorker = true
		}
		if !o.loadedByCommitWorker {
			o.offloadedFromCommitWorker = true
		}
		if !o.loadedBySnapshotWorker {
			o.offloadedFromSnapshotWorker = true
		}
	}
	if o.offloadedFromNodeHost &&
		o.offloadedFromCommitWorker &&
		o.offloadedFromSnapshotWorker &&
		o.offloadedFromStepWorker {
		o.readyToDestroy = true
	}
}
