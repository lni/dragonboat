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

#include <cstdio>
#include <cstddef>
#include <cstring>
#include <unistd.h>
#include "example.h"

HelloWorldStateMachine::HelloWorldStateMachine(uint64_t clusterID,
  uint64_t nodeID) noexcept
  : dragonboat::RegularStateMachine(clusterID, nodeID), count_(0)
{
}

HelloWorldStateMachine::~HelloWorldStateMachine()
{
}

uint64_t HelloWorldStateMachine::update(const dragonboat::Byte *data,
  size_t sz) noexcept
{
  count_++;
  return count_;
}

LookupResult HelloWorldStateMachine::lookup(const dragonboat::Byte *data,
  size_t sz) const noexcept
{
  LookupResult r;
  r.result = new char[sizeof(uint64_t)];
  r.size = sizeof(uint64_t);
  *((uint64_t *)r.result) = count_;
  return r;
}

uint64_t HelloWorldStateMachine::getHash() const noexcept
{
  return count_;
}

SnapshotResult HelloWorldStateMachine::saveSnapshot(dragonboat::SnapshotWriter *writer,
  dragonboat::SnapshotFileCollection *collection,
  const dragonboat::DoneChan &done) const noexcept
{
  SnapshotResult r;
  dragonboat::IOResult ret;
  r.errcode = SNAPSHOT_OK;
  r.size = 0;
  ret = writer->Write((dragonboat::Byte *)&count_, sizeof(uint64_t));
  if (ret.size != sizeof(uint64_t)) {
    r.errcode = FAILED_TO_SAVE_SNAPSHOT;
    return r;
  }
  r.size = sizeof(uint64_t);
  return r;
}

int HelloWorldStateMachine::recoverFromSnapshot(dragonboat::SnapshotReader *reader,
  const std::vector<dragonboat::SnapshotFile> &files,
  const dragonboat::DoneChan &done) noexcept
{
  dragonboat::IOResult ret;
  dragonboat::Byte data[sizeof(uint64_t)];
  ret = reader->Read(data, sizeof(uint64_t));
  if (ret.size != sizeof(uint64_t)) {
    return FAILED_TO_RECOVER_FROM_SNAPSHOT;
  }
  count_ = (uint64_t)(*data);
  return SNAPSHOT_OK;
}

void HelloWorldStateMachine::freeLookupResult(LookupResult r) noexcept
{
  delete[] r.result;
}

TestConcurrentStateMachine::TestConcurrentStateMachine(
  uint64_t clusterID, uint64_t nodeID) noexcept
  : dragonboat::ConcurrentStateMachine(clusterID, nodeID),
    count_(0)
{
}

TestConcurrentStateMachine::~TestConcurrentStateMachine()
{
}

uint64_t TestConcurrentStateMachine::update(
  const dragonboat::Byte *data, size_t size) noexcept
{
  count_++;
  return count_;
}

LookupResult TestConcurrentStateMachine::lookup(const dragonboat::Byte *data,
  size_t size) const noexcept
{
  LookupResult r;
  r.result = new char[sizeof(uint64_t)];
  r.size = sizeof(uint64_t);
  *((uint64_t *) r.result) = count_;
  return r;
}

uint64_t TestConcurrentStateMachine::getHash() const noexcept
{
  return count_;
}

PrepareSnapshotResult TestConcurrentStateMachine::prepareSnapshot() const noexcept
{
  PrepareSnapshotResult r;
  r.result = new char[sizeof(uint64_t)];
  r.size = sizeof(uint64_t);
  r.errcode = 0;
  memcpy(r.result, &count_, sizeof(uint64_t));
  return r;
}

SnapshotResult TestConcurrentStateMachine::saveSnapshot(
  const dragonboat::Byte *ctx,
  size_t size,
  dragonboat::SnapshotWriter *writer,
  dragonboat::SnapshotFileCollection *collection,
  const dragonboat::DoneChan &done) const noexcept
{
  auto ret = writer->Write(ctx, size);
  SnapshotResult r;
  r.errcode = SNAPSHOT_OK;
  r.size = ret.size;
  if(ret.size != size || ret.error != 0) {
    r.errcode = FAILED_TO_SAVE_SNAPSHOT;
  }
  return r;
}

int TestConcurrentStateMachine::recoverFromSnapshot(
  dragonboat::SnapshotReader *reader,
  const std::vector<dragonboat::SnapshotFile> &files,
  const dragonboat::DoneChan &done) noexcept
{
  dragonboat::IOResult ret;
  dragonboat::Byte data[sizeof(uint64_t)];
  ret = reader->Read(data, sizeof(uint64_t));
  if (ret.size != sizeof(uint64_t)) {
    return FAILED_TO_RECOVER_FROM_SNAPSHOT;
  }
  count_ = *(uint64_t*)data;
  return SNAPSHOT_OK;
}

void TestConcurrentStateMachine::freePrepareSnapshotResult(
  PrepareSnapshotResult r) noexcept
{
  delete[] r.result;
}

void TestConcurrentStateMachine::freeLookupResult(
  LookupResult r) noexcept
{
  delete[] r.result;
}

FakeOnDiskStateMachine::FakeOnDiskStateMachine(uint64_t clusterID,
  uint64_t nodeID, uint64_t initialApplied) noexcept
  : dragonboat::OnDiskStateMachine(clusterID, nodeID),
    initialApplied_(initialApplied),
    count_(0)
{
}

FakeOnDiskStateMachine::~FakeOnDiskStateMachine()
{
}

OpenResult FakeOnDiskStateMachine::open(
  const dragonboat::DoneChan &done) noexcept
{
  OpenResult r;
  r.result = initialApplied_;
  r.errcode = OPEN_OK;
  return r;
}

uint64_t FakeOnDiskStateMachine::update(
  const dragonboat::Byte *data, size_t size, uint64_t index) noexcept
{
  count_++;
  return count_;
}

LookupResult FakeOnDiskStateMachine::lookup(
  const dragonboat::Byte *data, size_t size) const noexcept
{
  LookupResult r;
  r.result = new char[sizeof(uint64_t)];
  r.size = sizeof(uint64_t);
  *((uint64_t *) r.result) = count_;
  return r;
}

int FakeOnDiskStateMachine::sync() const noexcept
{
  return SYNC_OK;
}

uint64_t FakeOnDiskStateMachine::getHash() const noexcept
{
  return count_;
}

PrepareSnapshotResult FakeOnDiskStateMachine::prepareSnapshot() const noexcept
{
  PrepareSnapshotResult r;
  r.result = new char[2*sizeof(uint64_t)];
  r.size = 2*sizeof(uint64_t);
  r.errcode = 0;
  std::memcpy(r.result, &initialApplied_, sizeof(uint64_t));
  std::memcpy(r.result + sizeof(uint64_t), &count_, sizeof(uint64_t));
  return r;
}

SnapshotResult FakeOnDiskStateMachine::saveSnapshot(
  const dragonboat::Byte *ctx, size_t size,
  dragonboat::SnapshotWriter *writer,
  const dragonboat::DoneChan &done) const noexcept
{
  auto ret = writer->Write(ctx, size);
  SnapshotResult r;
  r.errcode = SNAPSHOT_OK;
  r.size = ret.size;
  if(ret.size != size || ret.error != 0) {
    r.errcode = FAILED_TO_SAVE_SNAPSHOT;
  }
  return r;
}

int FakeOnDiskStateMachine::recoverFromSnapshot(
  dragonboat::SnapshotReader *reader,
  const dragonboat::DoneChan &done) noexcept
{
  dragonboat::IOResult ret;
  dragonboat::Byte data[2*sizeof(uint64_t)];
  ret = reader->Read(data, 2*sizeof(uint64_t));
  if (ret.size != 2*sizeof(uint64_t)) {
    return FAILED_TO_RECOVER_FROM_SNAPSHOT;
  }
  initialApplied_ = *(uint64_t*)data;
  count_ = *(uint64_t*)(data + sizeof(uint64_t));
  return SNAPSHOT_OK;
}

void FakeOnDiskStateMachine::freePrepareSnapshotResult(
  PrepareSnapshotResult r) noexcept
{
  delete[] r.result;
}

void FakeOnDiskStateMachine::freeLookupResult(LookupResult r) noexcept
{
  delete[] r.result;
}