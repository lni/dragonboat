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
#include "dragonboat/statemachine.h"
#include "dragonboat/binding.h"

namespace dragonboat {

StateMachine::StateMachine(uint64_t clusterID, uint64_t nodeID) noexcept
  : cluster_id_(clusterID), node_id_(nodeID)
{
}

StateMachine::~StateMachine()
{
}

uint64_t StateMachine::Update(const Byte *data, size_t size) noexcept
{
  return update(data, size);
}

LookupResult StateMachine::Lookup(const Byte *data, size_t size) const noexcept
{
  return lookup(data, size);
}

uint64_t StateMachine::GetHash() const noexcept
{
  return getHash();
}

SnapshotResult StateMachine::SaveSnapshot(SnapshotWriter *writer,
  SnapshotFileCollection *collection,
  const DoneChan &done) const noexcept
{
  return saveSnapshot(writer, collection, done);
}

int StateMachine::RecoverFromSnapshot(SnapshotReader *reader,
  const std::vector<SnapshotFile> &files, const DoneChan &done) noexcept
{
  return recoverFromSnapshot(reader, files, done);
}

void StateMachine::FreeLookupResult(LookupResult r) noexcept
{
  freeLookupResult(r);
}

}  // namespace dragonboat
