// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

#ifndef BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_H_
#define BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_H_

#include <cstdint>
#include <cstddef>
#include <vector>
#include "dragonboat/dragonboat.h"
#include "dragonboat/types.h"
#include "dragonboat/snapshotio.h"
#include "dragonboat/binding.h"

namespace dragonboat {

// SnapshotFile is the struct used to describe an external file included as a
// part of the snapshot.
struct SnapshotFile {
  uint64_t FileID;
  std::string Filepath;
  Byte *Metadata;
  size_t Length;
};

using SnapshotFile = struct SnapshotFile;

// StateMachine is the base class of all C++ state machines used in Dragonboat.
// Your state machine implementation in C++ should inherit from the StateMachine
// class.
//
// There are two ways to apply cpp state machine in dragonboat:
//
//  1. Link your state machine implementation as a .so dynamic library together
//     with a factory function defined in the global scope for creating the
//     state machine. See github.com/lni/dragonboat/internal/tests/cpptest
//     Specify the .so file name and the corresponding factory name in
//     NodeHost::StartCluster so it can be picked up by dragonboat
//
//  2. Directly parse the factory function to NodeHost::StartCluster.
//     See github.com/lni/dragonboat/binding/tests/dragonboat_tests.cpp

class RegularStateMachine
{
 public:
  // The clusterID and nodeID parameters are the cluster id and node id of
  // the node. They are provided for logging/debugging purposes.
  RegularStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  virtual ~RegularStateMachine();
  uint64_t Update(const Byte *data, size_t size) noexcept;
  LookupResult Lookup(const Byte *data, size_t size) const noexcept;
  uint64_t GetHash() const noexcept;
  SnapshotResult SaveSnapshot(SnapshotWriter *writer,
    SnapshotFileCollection *collection, const DoneChan &done) const noexcept;
  int RecoverFromSnapshot(SnapshotReader *reader,
    const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept;
  void FreeLookupResult(LookupResult r) noexcept;
 protected:
  // Cluster ID of the state machine. This is mainly used for logging/debugging
  // purposes.
  uint64_t cluster_id_;
  // Node ID of the state machine. This is mainly used for logging/debugging
  // purposes.
  uint64_t node_id_;
  // update() updates the state machine object. The input data buffer is the
  // proposed data provided to NodeHost.Propose or NodeHost.MakeProposal, it is
  // up to the actual subclass of StateMachine to interpret the meaning of this
  // input byte array and update the StateMachine accordingly. The input buffer
  // is owned by the caller of the update method, the update method should not
  // keep a reference to it after the end of the update() call. update() returns
  // an uint64 value used to indicate the result of the update operation.
  virtual uint64_t update(const Byte *data, size_t size) noexcept = 0;
  // lookup() queries the state of the StateMachine and returns the query
  // result. The input byte array parameter is the data used to specify what
  // need to be queried, it is up to the actual subclass of StateMachine to
  // interpret the meaning of this input byte array. The input buffer is owned
  // by the caller of the lookup method, the lookup method should not keep any
  // reference of it after the call. lookup() returns a LookupResult struct. The
  // lookup result is provided in the result field of LookupResult and len is
  // the length of the result buffer. The error field is the integer error code
  // for the lookup operation. Dragonboat will eventually pass the LookupResult
  // back to the freeLookupResult method so the result buffer in LookupResult
  // can be released or reused.
  virtual LookupResult lookup(const Byte *data, size_t size) const noexcept = 0;
  // getHash() returns a uint64_t integer representing the state of the
  // StateMachine instance, it is usually a hash result of the object state.
  virtual uint64_t getHash() const noexcept = 0;
  // saveSnapshot() saves the state of the StateMachine object to the specified
  // snapshot writer backed by a file on disk and the provided
  // SnapshotFileCollection instance. The data saved into the snapshot writer is
  // usually the in-memory data, while SnapshotFileCollection is used to record
  // finalized files that should also be included as a part of the snapshot. It
  // is application's responsibility to save the complete state so that the
  // recovered StateMachine state is considered as identical to the original
  // state.
  // The provided done instance can be used to check whether the saveSnapshot
  // operation has been requested to stop by the system. saveSnapshot returns a
  // SnapshotResult struct with the size field being the number of bytes that
  // has been written to the snapshot writer, error is the error code or
  // SNAPSHOT_OK when there is no error. When stopped by the system, the
  // saveSnapshot method can choose to returned error code SNAPSHOT_STOPPED and
  // stop the save snapshot operation.
  virtual SnapshotResult saveSnapshot(SnapshotWriter *writer,
    SnapshotFileCollection *collection,
    const DoneChan &done) const noexcept = 0;
  // recoverFromSnapshot() recovers the state of the StateMachine object from a
  // previously saved snapshot captured by the saveSnapshot() method. The saved
  // snapshot is provided as a snapshot reader backed by a file on disk and a
  // list of files previously recorded into the SnapshotFileCollection instance
  // in saveSnapshot(). The provided done instance can be queried to check
  // whether the recoverFromSnapshot operation has been requested to stop by the
  // system. recoverFromSnapshot returns a int error code or SNAPSHOT_OK when
  // there is no error. When stopped by the system, the recoverFromSnapshot
  // method can choose to return error code SNAPSHOT_STOPPED.
  virtual int recoverFromSnapshot(SnapshotReader *reader,
    const std::vector<SnapshotFile> &files,
    const DoneChan &done) noexcept = 0;
  // freeLookupResult() receives a LookupResult struct previously returned by
  // lookup(), it is up to your StateMachine implementation to decide whether to
  // free the result buffer included in the specified LookupResult, or just put
  // it back to a pool or something similiar to reuse the buffer in the future.
  virtual void freeLookupResult(LookupResult r) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(RegularStateMachine);
};

class ConcurrentStateMachine
{
 public:
  ConcurrentStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  virtual ~ConcurrentStateMachine();
  uint64_t Update(const Byte *data, size_t size) noexcept;
  LookupResult Lookup(const Byte *data, size_t size) const noexcept;
  uint64_t GetHash() const noexcept;
  PrepareSnapshotResult PrepareSnapshot() const noexcept;
  // the saved snapshot should be associated with the input ctx
  SnapshotResult SaveSnapshot(const Byte *ctx, size_t size,
    SnapshotWriter *writer, SnapshotFileCollection *collection,
    const DoneChan &done) const noexcept;
  int RecoverFromSnapshot(SnapshotReader *reader,
    const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept;
  void FreePrepareSnapshotResult(PrepareSnapshotResult r) noexcept;
  void FreeLookupResult(LookupResult r) noexcept;
 protected:
  uint64_t cluster_id_;
  uint64_t node_id_;
  virtual uint64_t update(const Byte *data, size_t size) noexcept = 0;
  virtual LookupResult lookup(const Byte *data, size_t size) const noexcept = 0;
  virtual uint64_t getHash() const noexcept = 0;
  // prepareSnapshot prepares the snapshot to be concurrently captured and saved.
  // prepareSnapshot is invoked before saveSnapshot is called and it is invoked
  // with mutual exclusion protection from the update method.
  virtual PrepareSnapshotResult prepareSnapshot() const noexcept = 0;
	// saveSnapshot saves the point in time state of the ConcurrentStateMachine
	// identified by the input state identifier to the provided SnapshotWriter backed
	// by a file on disk and the provided SnapshotFileCollection instance. This
	// is a read only method that should never change the state of the
	// ConcurrentStateMachine instance.
	//
	// It is important to understand that saveSnapshot should never save the
	// current latest state. The point in time state identified by the input state
	// identifier is what suppose to be saved, the latest state might be different
	// from such specified point in time state as the state machine might have
	// already been updated by the update() method after the completion of
	// the call to prepareSnapshot.
  virtual SnapshotResult saveSnapshot(const Byte *ctx, size_t size,
    SnapshotWriter *writer, SnapshotFileCollection *collection,
    const DoneChan &done) const noexcept = 0;
	// recoverFromSnapshot recovers the state of the ConcurrentStateMachine
	// instance from a previously saved snapshot captured by the saveSnapshot()
	// method. The saved snapshot is provided as an SnapshotReader backed by a file
	// on disk together with a list of files previously recorded into the
	// SnapshotFileCollection in saveSnapshot().
	//
	// Dragonboat ensures that update() will not be invoked when
	// recoverFromSnapshot() is in progress.
  virtual int recoverFromSnapshot(SnapshotReader *reader,
    const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept = 0;
  virtual void freePrepareSnapshotResult(PrepareSnapshotResult r) noexcept = 0;
  virtual void freeLookupResult(LookupResult r) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(ConcurrentStateMachine);
};

class OnDiskStateMachine
{
 public:
  OnDiskStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  virtual ~OnDiskStateMachine();
  OpenResult Open(const DoneChan &done) noexcept;
  uint64_t Update(const Byte *data, size_t size, uint64_t index) noexcept;
  LookupResult Lookup(const Byte *data, size_t size) const noexcept;
  int Sync() const noexcept;
  uint64_t GetHash() const noexcept;
  PrepareSnapshotResult PrepareSnapshot() const noexcept;
  SnapshotResult SaveSnapshot(const Byte *ctx, size_t size,
    SnapshotWriter *writer, const DoneChan &done) const noexcept;
  int RecoverFromSnapshot(SnapshotReader *reader,
    const DoneChan &done) noexcept;
  void FreePrepareSnapshotResult(PrepareSnapshotResult r) noexcept;
  void FreeLookupResult(LookupResult r) noexcept;
 protected:
  uint64_t cluster_id_;
  uint64_t node_id_;
  virtual OpenResult open(const DoneChan &done) noexcept = 0;
  virtual uint64_t update(const Byte *data, size_t size,
    uint64_t index) noexcept = 0;
  virtual LookupResult lookup(const Byte *data, size_t size) const noexcept = 0;
	// sync synchronizes all in-core state of the state machine to permanent
	// storage so the state machine can continue from its latest state after
	// reboot.
	// sync is always invoked with mutual exclusion protection from the update,
	// prepareSnapshot and recoverFromSnapshot methods.
  virtual int sync() const noexcept = 0;
  virtual uint64_t getHash() const noexcept = 0;
	// prepareSnapshot prepares the snapshot to be concurrently captured and
	// streamed. prepareSnapshot is invoked before saveSnapshot is called and it
	// is always invoked with mutual exclusion protection from the update, sync and
	// recoverFromSnapshot methods.
  virtual PrepareSnapshotResult prepareSnapshot() const noexcept = 0;
	// saveSnapshot saves the point in time state of the OnDiskStateMachine
	// instance identified by the input state identifier to the provided
	// SnapshotWriter. The SnapshotWriter is a connection to a remote node usually
	// significantly behind in terms of Raft log progress.
	// It is important to understand that saveSnapshot should never be implemented
	// to save the current latest state of the state machine when it is invoked.
	// saveSnapshot must be implemented to save the point in time state identified
	// by the input state identifier.
  virtual SnapshotResult saveSnapshot(const Byte *ctx, size_t size,
    SnapshotWriter *writer, const DoneChan &done) const noexcept = 0;
	// recoverFromSnapshot recovers the state of the OnDiskStateMachine instance
	// from a snapshot captured by the saveSnapshot() method on a remote node. The
	// saved snapshot is provided as an SnapshotReader backed by a file already fully
	// available on disk.
	// Dragonboat ensures that the update, sync, prepareSnapshot and saveSnapshot
	// methods will not be invoked when recoverFromSnapshot() is in progress.
  virtual int recoverFromSnapshot(SnapshotReader *reader,
    const DoneChan &done) noexcept = 0;
  virtual void freePrepareSnapshotResult(PrepareSnapshotResult r) noexcept = 0;
  virtual void freeLookupResult(LookupResult r) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(OnDiskStateMachine);
};

}  // namespace dragonboat

typedef struct CPPRegularStateMachine {
  dragonboat::RegularStateMachine *sm;
} CPPRegularStateMachine;

typedef struct CPPConcurrentStateMachine {
  dragonboat::ConcurrentStateMachine *sm;
} CPPConcurrentStateMachine;

typedef struct CPPOnDiskStateMachine {
  dragonboat::OnDiskStateMachine *sm;
} CPPOnDiskStateMachine;

#endif  // BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_H_
