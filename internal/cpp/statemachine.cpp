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

#include <vector>
#include "dragonboat/statemachine/regular.h"
#include "dragonboat/statemachine/concurrent.h"
#include "dragonboat/statemachine/ondisk.h"
#include "dragonboat/binding.h"

namespace dragonboat {

RegularStateMachine::RegularStateMachine(uint64_t clusterID,
  uint64_t nodeID) noexcept
  : cluster_id_(clusterID), node_id_(nodeID)
{
}

RegularStateMachine::~RegularStateMachine()
{
}

void RegularStateMachine::Update(Entry &ent) noexcept
{
  update(ent);
}

LookupResult RegularStateMachine::Lookup(const Byte *data,
  size_t size) const noexcept
{
  return lookup(data, size);
}

uint64_t RegularStateMachine::GetHash() const noexcept
{
  return getHash();
}

SnapshotResult RegularStateMachine::SaveSnapshot(SnapshotWriter *writer,
  SnapshotFileCollection *collection,
  const DoneChan &done) const noexcept
{
  return saveSnapshot(writer, collection, done);
}

int RegularStateMachine::RecoverFromSnapshot(SnapshotReader *reader,
  const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept
{
  return recoverFromSnapshot(reader, files, done);
}

void RegularStateMachine::FreeLookupResult(LookupResult r) noexcept
{
  freeLookupResult(r);
}

ConcurrentStateMachine::ConcurrentStateMachine(uint64_t clusterID,
  uint64_t nodeID) noexcept
  : cluster_id_(clusterID), node_id_(nodeID)
{
}

ConcurrentStateMachine::~ConcurrentStateMachine()
{
}

void ConcurrentStateMachine::BatchedUpdate(std::vector<Entry> &ents) noexcept
{
  batchedUpdate(ents);
}

LookupResult ConcurrentStateMachine::Lookup(const Byte *data,
  size_t size) const noexcept
{
  return lookup(data, size);
}

uint64_t ConcurrentStateMachine::GetHash() const noexcept
{
  return getHash();
}

PrepareSnapshotResult ConcurrentStateMachine::PrepareSnapshot() const noexcept
{
  return prepareSnapshot();
}

SnapshotResult ConcurrentStateMachine::SaveSnapshot(const void *context,
  SnapshotWriter *writer, SnapshotFileCollection *collection,
  const DoneChan &done) const noexcept
{
  return saveSnapshot(context, writer, collection, done);
}

int ConcurrentStateMachine::RecoverFromSnapshot(SnapshotReader *reader,
  const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept
{
  return recoverFromSnapshot(reader, files, done);
}

void ConcurrentStateMachine::FreeLookupResult(LookupResult r) noexcept
{
  freeLookupResult(r);
}

OnDiskStateMachine::OnDiskStateMachine(uint64_t clusterID,
  uint64_t nodeID) noexcept
  : cluster_id_(clusterID), node_id_(nodeID)
{
}

OnDiskStateMachine::~OnDiskStateMachine()
{
}

OpenResult OnDiskStateMachine::Open(const DoneChan &done) noexcept
{
  return open(done);
}

void OnDiskStateMachine::BatchedUpdate(std::vector<Entry> &ents) noexcept
{
  batchedUpdate(ents);
}

LookupResult OnDiskStateMachine::Lookup(const Byte *data,
  size_t size) const noexcept
{
  return lookup(data, size);
}

int OnDiskStateMachine::Sync() const noexcept
{
  return sync();
}

uint64_t OnDiskStateMachine::GetHash() const noexcept
{
  return getHash();
}

PrepareSnapshotResult OnDiskStateMachine::PrepareSnapshot() const noexcept
{
  return prepareSnapshot();
}

SnapshotResult OnDiskStateMachine::SaveSnapshot(const void *context,
  SnapshotWriter *writer, const DoneChan &done) const noexcept
{
  return saveSnapshot(context, writer, done);
}

int OnDiskStateMachine::RecoverFromSnapshot(SnapshotReader *reader,
  const DoneChan &done) noexcept
{
  return recoverFromSnapshot(reader, done);
}

void OnDiskStateMachine::FreeLookupResult(LookupResult r) noexcept
{
  freeLookupResult(r);
}

}  // namespace dragonboat
