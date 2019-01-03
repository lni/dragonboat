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
// Together with the following factory function defined in the global scope
// for creating your state machine instance, your state machine implementation
// should be linked as a .so dynamic library.
//
// extern "C" CPPStateMachine *CreateDragonboatPluginStateMachine()
//
// Generated .so file should following the following naming convention where
// xxxxx is the name of your state machine.
//
// dragonboat-cpp-plugin-xxxxx.so
//
// Place this .so file in the working directory of dragonboat so it can be
// picked up by dragonboat.
class StateMachine
{
 public:
  // The clusterID and nodeID parameters are the cluster id and node id of
  // the node. They are provided for logging/debugging purposes.
  StateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  virtual ~StateMachine();
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
  DISALLOW_COPY_MOVE_AND_ASSIGN(StateMachine);
};

}  // namespace dragonboat

typedef struct CPPStateMachine {
  dragonboat::StateMachine *sm;
} CPPStateMachine;

#endif  // BINDING_INCLUDE_DRAGONBOAT_STATEMACHINE_H_
