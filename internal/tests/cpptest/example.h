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

#include "dragonboat/statemachine/regular.h"
#include "dragonboat/statemachine/concurrent.h"
#include "dragonboat/statemachine/ondisk.h"

class HelloWorldStateMachine : public dragonboat::RegularStateMachine
{
 public:
  HelloWorldStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
  ~HelloWorldStateMachine();
 protected:
  void update(dragonboat::Entry &ent) noexcept override;
  void batchedUpdate(std::vector<dragonboat::Entry> &ents) noexcept override;
  LookupResult lookup(const dragonboat::Byte *data,
    size_t size) const noexcept override;
  uint64_t getHash() const noexcept override;
  SnapshotResult saveSnapshot(dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override;
  void freeLookupResult(LookupResult r) noexcept override;
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
  void update(dragonboat::Entry &ent) noexcept override;
  void batchedUpdate(std::vector<dragonboat::Entry> &ents) noexcept override;
  LookupResult lookup(const dragonboat::Byte *data,
    size_t size) const noexcept override;
  uint64_t getHash() const noexcept override;
  PrepareSnapshotResult prepareSnapshot() const noexcept override;
  SnapshotResult saveSnapshot(const void *context,
    dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override;
  void freeLookupResult(LookupResult r) noexcept;
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
  void update(dragonboat::Entry &ent) noexcept override;
  void batchedUpdate(std::vector<dragonboat::Entry> &ents) noexcept override;
  LookupResult lookup(const dragonboat::Byte *data,
    size_t size) const noexcept override;
  int sync() const noexcept override;
  uint64_t getHash() const noexcept override;
  PrepareSnapshotResult prepareSnapshot() const noexcept override;
  SnapshotResult saveSnapshot(const void *context,
    dragonboat::SnapshotWriter *writer,
    const dragonboat::DoneChan &done) const noexcept override;
  int recoverFromSnapshot(
    dragonboat::SnapshotReader *reader,
    const dragonboat::DoneChan &done) noexcept override;
  void freeLookupResult(LookupResult r) noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(FakeOnDiskStateMachine);
  uint64_t initialApplied_;
  uint64_t count_;
};

#endif // DRAGONBOAT_EXAMPLE_STATEMACHINE_H
