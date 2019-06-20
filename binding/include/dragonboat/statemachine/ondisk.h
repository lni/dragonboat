// Copyright 2017-2019 Jason Yuchen (jasonyuchen@foxmail.com) and other Dragonboat authors.
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

#ifndef BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_ONDISK_H_
#define BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_ONDISK_H_

#include <cstdint>
#include <cstddef>
#include <vector>
#include "dragonboat/dragonboat.h"
#include "dragonboat/types.h"
#include "dragonboat/snapshotio.h"
#include "dragonboat/binding.h"

namespace dragonboat {

// OnDiskStateMachine is the base class of all C++ on-disk state machines used
// in Dragonboat. Your on-disk state machine implementation in C++ should
// inherit from the OnDiskStateMachine class.
//
// There are two ways to apply cpp on-disk state machine in dragonboat:
//
//  1. Link your on-disk state machine implementation as a .so dynamic library
//     together with a factory function defined in the global scope for creating
//     the on-disk state machine. Specify the .so file name and the corresponding
//     factory name in NodeHost::StartCluster so it can be picked up by dragonboat
//     For more details, see github.com/lni/dragonboat/internal/tests/cpptest
//  2. Directly pass the factory function to NodeHost::StartCluster.
//     For more details, see github.com/lni/dragonboat/binding/tests/nodehost_tests.cpp

class OnDiskStateMachine
{
 public:
  // The clusterID and nodeID parameters are the cluster id and node id of
  // the node. They are provided for logging/debugging purposes.
  OnDiskStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  virtual ~OnDiskStateMachine();
  OpenResult Open(const DoneChan &done) noexcept;
  void Update(Entry &ent) noexcept;
  void BatchedUpdate(std::vector<Entry> &ents) noexcept;
  LookupResult Lookup(const Byte *data, size_t size) const noexcept;
  int Sync() const noexcept;
  uint64_t GetHash() const noexcept;
  PrepareSnapshotResult PrepareSnapshot() const noexcept;
  SnapshotResult SaveSnapshot(const void *context,
    SnapshotWriter *writer, const DoneChan &done) const noexcept;
  int RecoverFromSnapshot(SnapshotReader *reader,
    const DoneChan &done) noexcept;
  void FreeLookupResult(LookupResult r) noexcept;
 protected:
  uint64_t cluster_id_;
  uint64_t node_id_;
  virtual OpenResult open(const DoneChan &done) noexcept = 0;
  // update() updates the state machine object.
  // The Entry::index is the raft log index associated with this proposal.
  // The Entry::cmd is the proposed data provided by NodeHost::Propose, it is
  // up to the actual subclass of state machine to interpret the meaning of this
  // input byte array and update the state machine accordingly.
  // The Entry::result should be set in update() to indicate the result of the
  // update operation.
  // The input Entry is owned by the caller of the update method, the update
  // method should not keep a reference to it after the end of the update() call.
  virtual void update(Entry &ent) noexcept = 0;
  virtual void batchedUpdate(std::vector<Entry> &ents) noexcept = 0;
  // lookup() queries the state of the StateMachine and returns the query
  // result. The input byte array parameter is the data used to specify what
  // need to be queried, it is up to the actual subclass of StateMachine to
  // interpret the meaning of this input byte array. The input buffer is owned
  // by the caller of the lookup method, the lookup method should not keep any
  // reference of it after the call. lookup() returns a LookupResult struct. The
  // lookup result is provided in the result field of LookupResult and len is
  // the length of the result buffer. Dragonboat will eventually pass the
  // LookupResult back to the freeLookupResult method so the result buffer in
  // LookupResult can be released or reused.
  virtual LookupResult lookup(const Byte *data, size_t size) const noexcept = 0;
	// sync() synchronizes all in-core state of the state machine to permanent
	// storage so the state machine can continue from its latest state after reboot.
	// sync() is always invoked with mutual exclusion protection from the update(),
	// prepareSnapshot() and recoverFromSnapshot().
  virtual int sync() const noexcept = 0;
  // getHash() returns a uint64_t integer representing the state of the
  // state machine instance, it is usually a hash result of the object state.
  virtual uint64_t getHash() const noexcept = 0;
  // prepareSnapshot() prepares the snapshot to be concurrently captured and saved.
  // prepareSnapshot() is invoked before saveSnapshot() is called and it is invoked
  // with mutual exclusion protection from the update().
  // The returned PrepareSnapshotResult::result could point to any type and it is
  // immediately passed to saveSnapshot() as context.
  // Resource associated with the result should be released in the saveSnapshot.
  virtual PrepareSnapshotResult prepareSnapshot() const noexcept = 0;
	// saveSnapshot() saves the point in time state of the state machine identified
	// by the input context to the provided SnapshotWriter backed by a file on disk.
	// This is a read only method that should never change the state of the state
	// machine instance.
	// The resource associated with the context generated in prepareSnapshot()
	// should be released in saveSnapshot().
	// It is important to understand that saveSnapshot() should never save the
	// current latest state. The point in time state identified by the input context
	// is what suppose to be saved, the latest state might be different from such
  // specified point in time state as the state machine might have already been
  // updated by the update() method after the completion of prepareSnapshot().
  virtual SnapshotResult saveSnapshot(const void *context,
    SnapshotWriter *writer, const DoneChan &done) const noexcept = 0;
	// recoverFromSnapshot() recovers the state of the state machine instance from
	// a previously saved snapshot captured by the saveSnapshot() on a remote node.
	// The saved snapshot is provided as an SnapshotReader backed by a file already
	// fully available on disk. Dragonboat ensures that the update(), sync(),
	// prepareSnapshot() and saveSnapshot() will not be invoked when
	// recoverFromSnapshot() is in progress.
  virtual int recoverFromSnapshot(SnapshotReader *reader,
    const DoneChan &done) noexcept = 0;
  // freeLookupResult() receives a LookupResult struct previously returned by
  // lookup(), it is up to your StateMachine implementation to decide whether to
  // free the result buffer included in the specified LookupResult, or just put
  // it back to a pool or something similiar to reuse the buffer in the future.
  virtual void freeLookupResult(LookupResult r) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(OnDiskStateMachine);
};

} // namespace dragonboat

typedef struct CPPOnDiskStateMachine {
  dragonboat::OnDiskStateMachine *sm;
} CPPOnDiskStateMachine;

#endif  // BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_ONDISK_H_
