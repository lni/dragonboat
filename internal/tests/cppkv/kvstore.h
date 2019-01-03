// Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
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

#ifndef DRAGONBOAT_KVTEST_STATEMACHINE_H
#define DRAGONBOAT_KVTEST_STATEMACHINE_H

#include <map>
#include "dragonboat/statemachine.h"

class KVTestStateMachine : public dragonboat::StateMachine
{
  public:
    KVTestStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept;
    ~KVTestStateMachine();
  protected:
    uint64_t update(const dragonboat::Byte *data,
      size_t size) noexcept override;
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
    DISALLOW_COPY_MOVE_AND_ASSIGN(KVTestStateMachine);
    std::map<std::string, std::string> kvstore_;
    int update_count_;
};

#endif // DRAGONBOAT_KVTEST_STATEMACHINE_H
