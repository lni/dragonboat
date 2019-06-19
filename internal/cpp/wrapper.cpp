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

#include <string>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstddef>
#include <dlfcn.h>
#include "wrapper.h"
#include "dragonboat/statemachine.h"
#include "dragonboat/snapshotio.h"

typedef struct CollectedFiles {
    dragonboat::CollectedFiles *cf;
} CollectedFiles;

void *LoadFactoryFromPlugin(char *soFilename, char *factoryName)
{
  void *handle;
  handle = ::dlopen(soFilename, RTLD_LAZY);
  if (!handle) {
    fputs(dlerror(), stderr);
    exit(1);
  }
  void *fn = ::dlsym(handle, factoryName);
  if(!fn) {
    fputs(dlerror(), stderr);
    exit(1);
  }
  return fn;
}

CPPRegularStateMachine *CreateDBRegularStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory, uint64_t cStyle)
{
  if (cStyle) {
    auto fn = reinterpret_cast<
      dragonboat::RegularStateMachine*(*)(uint64_t, uint64_t)>(factory);
    CPPRegularStateMachine *ds = new CPPRegularStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  } else {
    auto fn = reinterpret_cast<std::function<
      dragonboat::RegularStateMachine*(uint64_t, uint64_t)>*>(factory);
    CPPRegularStateMachine *ds = new CPPRegularStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  }
}

void DestroyDBRegularStateMachine(CPPRegularStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

uint64_t UpdateDBRegularStateMachine(CPPRegularStateMachine *ds,
  uint64_t index, const unsigned char *cmd, size_t cmdLen)
{
  uint64_t result = 0;
  dragonboat::Entry ent{index, cmd, cmdLen, result};
  ds->sm->Update(ent);
  return result;
}

void BatchedUpdateDBRegularStateMachine(CPPRegularStateMachine *ds,
  Entry *entries, size_t size)
{
  std::vector<dragonboat::Entry> ents;
  ents.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    ents.emplace_back(dragonboat::Entry{
      entries[i].index,
      entries[i].cmd,
      entries[i].cmdLen,
      entries[i].result});
  }
  ds->sm->BatchedUpdate(ents);
}

LookupResult LookupDBRegularStateMachine(CPPRegularStateMachine *ds,
  const void *data)
{
  return ds->sm->Lookup(data);
}

uint64_t GetHashDBRegularStateMachine(CPPRegularStateMachine *ds)
{
  return ds->sm->GetHash();
}

SnapshotResult SaveSnapshotDBRegularStateMachine(CPPRegularStateMachine *ds,
  uint64_t writerOID, uint64_t collectionOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  dragonboat::SnapshotFileCollection collection(collectionOID);
  return ds->sm->SaveSnapshot(&writer, &collection, done);
}

int RecoverFromSnapshotDBRegularStateMachine(CPPRegularStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotReader reader(readerOID);
  dragonboat::DoneChan done(doneChOID);
  return ds->sm->RecoverFromSnapshot(&reader, cf->cf->GetFiles(), done);
}

CPPConcurrentStateMachine *CreateDBConcurrentStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory, uint64_t cStyle)
{
  if (cStyle) {
    auto fn = reinterpret_cast<
      dragonboat::ConcurrentStateMachine*(*)(uint64_t, uint64_t)>(factory);
    CPPConcurrentStateMachine *ds = new CPPConcurrentStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  } else {
    auto fn = reinterpret_cast<std::function<
      dragonboat::ConcurrentStateMachine*(uint64_t, uint64_t)>*>(factory);
    CPPConcurrentStateMachine *ds = new CPPConcurrentStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  }
}

void DestroyDBConcurrentStateMachine(CPPConcurrentStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

uint64_t UpdateDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  uint64_t index, const unsigned char *cmd, size_t cmdLen)
{
  uint64_t result = 0;
  dragonboat::Entry ent{index, cmd, cmdLen, result};
  ds->sm->Update(ent);
  return result;
}

void BatchedUpdateDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  Entry *entries, size_t size)
{
  std::vector<dragonboat::Entry> ents;
  ents.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    ents.emplace_back(dragonboat::Entry{
      entries[i].index,
      entries[i].cmd,
      entries[i].cmdLen,
      entries[i].result});
  }
  ds->sm->BatchedUpdate(ents);
}

LookupResult LookupDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  const void *data)
{
  return ds->sm->Lookup(data);
}

uint64_t GetHashDBConcurrentStateMachine(CPPConcurrentStateMachine *ds)
{
  return ds->sm->GetHash();
}

PrepareSnapshotResult PrepareSnapshotDBConcurrentStateMachine(
  CPPConcurrentStateMachine *ds)
{
  return ds->sm->PrepareSnapshot();
}

SnapshotResult SaveSnapshotDBConcurrentStateMachine(
  CPPConcurrentStateMachine *ds, const void *context,
  uint64_t writerOID, uint64_t collectionOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  dragonboat::SnapshotFileCollection collection(collectionOID);
  return ds->sm->SaveSnapshot(context, &writer, &collection, done);
}

int RecoverFromSnapshotDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID)
{
    dragonboat::ProxySnapshotReader reader(readerOID);
    dragonboat::DoneChan done(doneChOID);
    return ds->sm->RecoverFromSnapshot(&reader, cf->cf->GetFiles(), done);
}

CPPOnDiskStateMachine *CreateDBOnDiskStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory, uint64_t cStyle)
{
  if (cStyle) {
    auto fn = reinterpret_cast<
      dragonboat::OnDiskStateMachine*(*)(uint64_t, uint64_t)>(factory);
    CPPOnDiskStateMachine *ds = new CPPOnDiskStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  } else {
    auto fn = reinterpret_cast<std::function<
      dragonboat::OnDiskStateMachine*(uint64_t, uint64_t)>*>(factory);
    CPPOnDiskStateMachine *ds = new CPPOnDiskStateMachine;
    ds->sm = (*fn)(clusterID, nodeID);
    return ds;
  }
}

void DestroyDBOnDiskStateMachine(CPPOnDiskStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

OpenResult OpenDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  uint64_t doneChOID)
{
  dragonboat::DoneChan done(doneChOID);
  return ds->sm->Open(done);
}

uint64_t UpdateDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  uint64_t index, const unsigned char *cmd, size_t cmdLen)
{
  uint64_t result = 0;
  dragonboat::Entry ent{index, cmd, cmdLen, result};
  ds->sm->Update(ent);
  return result;
}

void BatchedUpdateDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  Entry *entries, size_t size)
{
  std::vector<dragonboat::Entry> ents;
  ents.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    ents.emplace_back(dragonboat::Entry{
      entries[i].index,
      entries[i].cmd,
      entries[i].cmdLen,
      entries[i].result});
  }
  ds->sm->BatchedUpdate(ents);
}

LookupResult LookupDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  const void *data)
{
  return ds->sm->Lookup(data);
}

int SyncDBOnDiskStateMachine(CPPOnDiskStateMachine *ds)
{
  return ds->sm->Sync();
}

uint64_t GetHashDBOnDiskStateMachine(CPPOnDiskStateMachine *ds)
{
  return ds->sm->GetHash();
}

PrepareSnapshotResult PrepareSnapshotDBOnDiskStateMachine(
  CPPOnDiskStateMachine *ds)
{
  return ds->sm->PrepareSnapshot();
}

SnapshotResult SaveSnapshotDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  const void *context, uint64_t writerOID,
  uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  return ds->sm->SaveSnapshot(context, &writer, done);
}

int RecoverFromSnapshotDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  uint64_t readerOID, uint64_t doneChOID)
{
    dragonboat::ProxySnapshotReader reader(readerOID);
    dragonboat::DoneChan done(doneChOID);
    return ds->sm->RecoverFromSnapshot(&reader, done);
}

CollectedFiles *GetCollectedFile()
{
  CollectedFiles *result = new CollectedFiles();
  result->cf = new dragonboat::CollectedFiles();
  return result;
}

void FreeCollectedFile(CollectedFiles *cf)
{
  delete cf->cf;
  delete cf;
}

void AddToCollectedFile(CollectedFiles *cf, uint64_t fileID,
  const char *path, size_t pathLen, const unsigned char *metadata, size_t len)
{
  std::string p(path, pathLen);
  cf->cf->AddFile(fileID, p, metadata, len);
}
