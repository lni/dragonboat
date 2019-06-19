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

#ifndef DRAGONBOAT_EXAMPLE_STATEMACHINE_H
#define DRAGONBOAT_EXAMPLE_STATEMACHINE_H

#include "dragonboat/statemachine.h"

// HelloWorldStateMachine is an example CPP StateMachine. It shows how to
// implement a StateMachine with your own application logic and interact with
// the rest of the Dragonboat system. 
// Basically, the logic is simple - this data store increases the update_count_
// member variable for each incoming update request no matter what is in the
// update request. Lookup requests always return the integer value stored in
// update_count_, same as the getHash method. 
// 
// See statemachine.h for more details about the StateMachine interface. 
class HelloWorldStateMachine : public dragonboat::RegularStateMachine
{
 public:
  HelloWorldStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  ~HelloWorldStateMachine();
 protected:
  uint64_t update(const dragonboat::Byte *data,
    size_t size) noexcept override;
  LookupResult lookup(const void *data) const noexcept override;
  uint64_t getHash() const noexcept override;
  SnapshotResult saveSnapshot(dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(HelloWorldStateMachine);
  uint64_t count_;
};

class TestConcurrentStateMachine : public dragonboat::ConcurrentStateMachine
{
 public:
  TestConcurrentStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  ~TestConcurrentStateMachine();
 protected:
  uint64_t update(const dragonboat::Byte *data, size_t size) noexcept override;
  LookupResult lookup(const void *data) const noexcept override;
  uint64_t getHash() const noexcept override;
  PrepareSnapshotResult prepareSnapshot() const noexcept override;
  SnapshotResult saveSnapshot(const dragonboat::Byte *ctx, size_t size,
    dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override;
  void freePrepareSnapshotResult(PrepareSnapshotResult r) noexcept override;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(TestConcurrentStateMachine);
  uint64_t count_;
};

class FakeOnDiskStateMachine : public dragonboat::OnDiskStateMachine {
 public:
  FakeOnDiskStateMachine(uint64_t clusterID, uint64_t nodeID,
    uint64_t initialApplied = 123) noexcept;
  ~FakeOnDiskStateMachine();
 protected:
  OpenResult open(const dragonboat::DoneChan &done) noexcept override;
  uint64_t update(const dragonboat::Byte *data, size_t size,
    uint64_t index) noexcept override;
  LookupResult lookup(const void *data) const noexcept override;
  int sync() const noexcept override;
  uint64_t getHash() const noexcept override;
  PrepareSnapshotResult prepareSnapshot() const noexcept override;
  SnapshotResult saveSnapshot(const dragonboat::Byte *ctx, size_t size,
    dragonboat::SnapshotWriter *writer,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(
    dragonboat::SnapshotReader *reader,
    const dragonboat::DoneChan &done) noexcept override;
  void freePrepareSnapshotResult(PrepareSnapshotResult r) noexcept override;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(FakeOnDiskStateMachine);
  uint64_t initialApplied_;
  uint64_t count_;
};

#endif // DRAGONBOAT_EXAMPLE_STATEMACHINE_H
