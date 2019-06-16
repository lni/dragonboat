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

int IsValidDragonboatPlugin(char *soFilename)
{
  void *handle;
  CPPStateMachine *(*fn)(uint64_t, uint64_t);
  handle = ::dlopen(soFilename, RTLD_LAZY);
  if (!handle) {
    return 1;
  }
  fn = (CPPStateMachine *(*)(uint64_t, uint64_t))::dlsym(handle,
    createStateMachineFuncName);
  if (!fn) {
    dlclose(handle);
    return 1;
  }
  dlclose(handle);
  return 0;
}

CPPStateMachine *CreateDBStateMachine(uint64_t clusterID,
  uint64_t nodeID, char *soFilename)
{
  void *handle;
  CPPStateMachine *(*fn)(uint64_t, uint64_t);
  handle = ::dlopen(soFilename, RTLD_LAZY);
  if (!handle) {
    fputs(dlerror(), stderr);
    exit(1);
  }
  fn = (CPPStateMachine *(*)(uint64_t, uint64_t))::dlsym(handle,
    createStateMachineFuncName);
  if (!fn) {
    fputs(dlerror(), stderr);
    exit(1);
  }
  CPPStateMachine *ds = (*fn)(clusterID, nodeID);
  return ds;
}

CPPStateMachine *CreateDBStateMachineFromFactory(uint64_t clusterID,
  uint64_t nodeID, void *factory)
{
  auto fn = reinterpret_cast<dragonboat::StateMachine *(*)(uint64_t, uint64_t)>(factory);
  CPPStateMachine *ds = new CPPStateMachine;
  ds->sm = (*fn)(clusterID, nodeID);
  return ds;
}

void DestroyDBStateMachine(CPPStateMachine *ds)
{
  delete ds->sm;
  delete ds;
}

uint64_t UpdateDBStateMachine(CPPStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Update(data, size);
}

LookupResult LookupDBStateMachine(CPPStateMachine *ds,
  const unsigned char *data, size_t size)
{
  return ds->sm->Lookup(data, size);
}

uint64_t GetHashDBStateMachine(CPPStateMachine *ds)
{
  return ds->sm->GetHash();
}

SnapshotResult SaveSnapshotDBStateMachine(CPPStateMachine *ds,
  uint64_t writerOID, uint64_t collectionOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotWriter writer(writerOID);
  dragonboat::DoneChan done(doneChOID);
  dragonboat::SnapshotFileCollection collection(collectionOID);
  return ds->sm->SaveSnapshot(&writer, &collection, done);
}

int RecoverFromSnapshotDBStateMachine(CPPStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID)
{
  dragonboat::ProxySnapshotReader reader(readerOID);
  dragonboat::DoneChan done(doneChOID);
  return ds->sm->RecoverFromSnapshot(&reader, cf->cf->GetFiles(), done);
}

void FreeLookupResult(CPPStateMachine *ds, LookupResult r)
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
