// Copyright 2017 Lei Ni (nilei81@gmail.com).
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
#include <istream>
#include <streambuf>
#include <memory>
#include <iostream>
#include <unordered_map>
#include "kvstore.h"
#include "kv.pb.h"
#include "json.hpp"

using json = nlohmann::json;

struct membuf : std::streambuf
{
  membuf(char *begin, char *end) 
  {
    this->setg(begin, begin, end);
  }
};

KVTestStateMachine::KVTestStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept
  : dragonboat::StateMachine(clusterID, nodeID), kvstore_(), update_count_(0)
{
  std::cout << "KVTestStateMachine created" << std::endl;
}

KVTestStateMachine::~KVTestStateMachine()
{
  std::cout << "KVTestStateMachine stopped" << std::endl;
}

uint64_t KVTestStateMachine::update(const dragonboat::Byte *data,
  size_t sz) noexcept
{
  membuf sbuf((char *)data, (char *)data + sz);
  std::istream indata(&sbuf);

  kvpb::PBKV kv;
  kv.ParseFromIstream(&indata);
  kvstore_[kv.key()] = kv.val();
  update_count_++;
  return static_cast<uint64_t>(sz);
}

LookupResult KVTestStateMachine::lookup(const dragonboat::Byte *data,
  size_t sz) const noexcept
{
  std::string key(data, data+sz);
  LookupResult r;
  r.size = 0;
  r.result = nullptr;
  auto it = kvstore_.find(key);
  if (it != kvstore_.end()) {
    std::string val = it->second;
    r.result = new char[val.size()];
    r.size = int(val.size());
    ::memcpy(r.result, val.c_str(), val.size());
  }
  return r;
}

uint64_t KVTestStateMachine::getHash() const noexcept
{
  std::map<std::string, std::string> tmp = kvstore_;
  tmp["KVTestStateMachine_update_count_value_"] = std::to_string(update_count_);
  json j(tmp);
  std::stringstream ss;
  ss << j;
  return (uint64_t)std::hash<std::string>{}(ss.str());
}

SnapshotResult KVTestStateMachine::saveSnapshot(dragonboat::SnapshotWriter *writer,
  dragonboat::SnapshotFileCollection *collection, 
  const dragonboat::DoneChan &done) const noexcept
{
  SnapshotResult r;
  r.error = SNAPSHOT_OK;
  r.size = 0;
  json j;
  j["count"] = update_count_;
  j["kvstore"] = kvstore_;
  std::stringstream ss;
  ss << j;
  size_t sz = ss.str().size();
  dragonboat::IOResult ret;
  ret = writer->Write((dragonboat::Byte *)&sz, sizeof(sz));
  if (ret.size != sizeof(sz)) {
    r.error = FAILED_TO_SAVE_SNAPSHOT;
    return r;
  }
  ret = writer->Write((dragonboat::Byte *)(ss.str().c_str()), sz);
  if (ret.size != sz) {
    r.error = FAILED_TO_SAVE_SNAPSHOT;
    return r;
  }
  r.size = int(sizeof(size_t) + sz);
  return r;
}

int KVTestStateMachine::recoverFromSnapshot(dragonboat::SnapshotReader *reader,
  const std::vector<dragonboat::SnapshotFile> &files, 
  const dragonboat::DoneChan &done) noexcept
{
  size_t sz;
  dragonboat::IOResult ret;
  ret = reader->Read((dragonboat::Byte *)&sz, sizeof(size_t));
  if (ret.size != sizeof(size_t)) {
    return FAILED_TO_RECOVER_FROM_SNAPSHOT;
  }
  std::unique_ptr<dragonboat::Byte[]> data(new dragonboat::Byte[sz]);
  ret = reader->Read(data.get(), sz);
  if (ret.size != sz) {
    return FAILED_TO_RECOVER_FROM_SNAPSHOT;
  }
  std::string snapshot(data.get(), data.get()+sz);
  std::stringstream ss;
  ss.str(snapshot);
  json j;
  j << ss;
  update_count_ = j["count"];
  std::map<std::string, std::string> nm;
  for (json::iterator it=j["kvstore"].begin();
       it != j["kvstore"].end();
       ++it) {
    nm[it.key()] = it.value();
  }
  kvstore_ = nm;
  return SNAPSHOT_OK;
}

void KVTestStateMachine::freeLookupResult(LookupResult r) noexcept
{
  if (r.size > 0 && r.result != nullptr) {
    delete[] r.result;
  }
}
