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

#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstddef>
#include <dlfcn.h>
#include "wrapper.h"
#include "dragonboat/statemachine.h"
#include "dragonboat/snapshotio.h"

const char createStateMachineFuncName[] = "CreateDragonboatPluginStateMachine";


typedef struct CollectedFiles {
    dragonboat::CollectedFiles *cf;
} CollectedFiles;

//int IsValidDragonboatPlugin(char *soFilename)
//{
//  void *handle;
//  CPPStateMachine *(*fn)(uint64_t, uint64_t);
//  handle = ::dlopen(soFilename, RTLD_LAZY);
//  if (!handle) {
//    return 1;
//  }
//  fn = (CPPStateMachine *(*)(uint64_t, uint64_t))::dlsym(handle,
//    createStateMachineFuncName);
//  if (!fn) {
//    dlclose(handle);
//    return 1;
//  }
//  dlclose(handle);
//  return 0;
//}
//
//CPPStateMachine *CreateDBStateMachine(uint64_t clusterID,
//  uint64_t nodeID, char *soFilename)
//{
//  void *handle;
//  CPPStateMachine *(*fn)(uint64_t, uint64_t);
//  handle = ::dlopen(soFilename, RTLD_LAZY);
//  if (!handle) {
//    fputs(dlerror(), stderr);
//    exit(1);
//  }
//  fn = (CPPStateMachine *(*)(uint64_t, uint64_t))::dlsym(handle,
//    createStateMachineFuncName);
//  if (!fn) {
//    fputs(dlerror(), stderr);
//    exit(1);
//  }
//  CPPStateMachine *ds = (*fn)(clusterID, nodeID);
//  return ds;
//}

CPPRegularStateMachine *CreateDBRegularStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory)
{
  auto fn = reinterpret_cast<dragonboat::RegularStateMachine *(*)(uint64_t, uint64_t)>(factory);
  CPPRegularStateMachine *ds = new CPPRegularStateMachine;
  ds->sm = (*fn)(clusterID, nodeID);
  return ds;
}

void DestroyDBRegularStateMachine(CPPRegularStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

uint64_t UpdateDBRegularStateMachine(CPPRegularStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Update(data, size);
}

LookupResult LookupDBRegularStateMachine(CPPRegularStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Lookup(data, size);
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

void FreeLookupResultDBRegularStateMachine(CPPRegularStateMachine *ds,
  LookupResult r)
{
  ds->sm->FreeLookupResult(r);
}

CPPConcurrentStateMachine *CreateDBConcurrentStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory)
{
  auto fn = reinterpret_cast<dragonboat::ConcurrentStateMachine *(*)(uint64_t, uint64_t)>(factory);
  CPPConcurrentStateMachine *ds = new CPPConcurrentStateMachine;
  ds->sm = (*fn)(clusterID, nodeID);
  return ds;
}

void DestroyDBConcurrentStateMachine(CPPConcurrentStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

uint64_t UpdateDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Update(data, size);
}

LookupResult LookupDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Lookup(data, size);
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
  CPPConcurrentStateMachine *ds, const unsigned char *data, size_t size,
  uint64_t writerOID, uint64_t collectionOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  dragonboat::SnapshotFileCollection collection(collectionOID);
  return ds->sm->SaveSnapshot(data, size, &writer, &collection, done);
}

int RecoverFromSnapshotDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID)
{
    dragonboat::ProxySnapshotReader reader(readerOID);
    dragonboat::DoneChan done(doneChOID);
    return ds->sm->RecoverFromSnapshot(&reader, cf->cf->GetFiles(), done);
}

void FreePrepareSnapshotResultDBConcurrentStateMachine(
  CPPConcurrentStateMachine *ds, PrepareSnapshotResult r)
{
  ds->sm->FreePrepareSnapshotResult(r);
}

void FreeLookupResultDBConcurrentStateMachine(CPPConcurrentStateMachine *ds,
  LookupResult r)
{
  ds->sm->FreeLookupResult(r);
}

CPPOnDiskStateMachine *CreateDBOnDiskStateMachine(uint64_t clusterID,
  uint64_t nodeID, void *factory)
{
  auto fn = reinterpret_cast<dragonboat::OnDiskStateMachine *(*)(uint64_t, uint64_t)>(factory);
  CPPOnDiskStateMachine *ds = new CPPOnDiskStateMachine;
  ds->sm = (*fn)(clusterID, nodeID);
  return ds;
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
  const unsigned char *data, size_t size)
{
  return ds->sm->Update(data, size);
}

LookupResult LookupDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Lookup(data, size);
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

SnapshotResult SaveSnapshotDBOnDiskStateMachine(CPPConcurrentStateMachine *ds,
  const unsigned char *data, size_t size, uint64_t writerOID,
  uint64_t collectionOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  dragonboat::SnapshotFileCollection collection(collectionOID);
  return ds->sm->SaveSnapshot(data, size, &writer, &collection, done);
}

int RecoverFromSnapshotDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID)
{
    dragonboat::ProxySnapshotReader reader(readerOID);
    dragonboat::DoneChan done(doneChOID);
    return ds->sm->RecoverFromSnapshot(&reader, cf->cf->GetFiles(), done);
}

void FreePrepareSnapshotResultDBOnDiskStateMachine(
  CPPOnDiskStateMachine *ds, PrepareSnapshotResult r)
{
  ds->sm->FreePrepareSnapshotResult(r);
}

void FreeLookupResultDBOnDiskStateMachine(CPPOnDiskStateMachine *ds,
  LookupResult r)
{
  ds->sm->FreeLookupResult(r);
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
