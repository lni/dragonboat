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

#ifndef INTERNAL_CPP_WRAPPER_H_
#define INTERNAL_CPP_WRAPPER_H_

#include <stdint.h>
#include "dragonboat/binding.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CPPStateMachine CPPStateMachine;
typedef struct CollectedFiles CollectedFiles;

int IsValidDragonboatPlugin(char *soFilename);
CPPStateMachine *CreateDBStateMachine(uint64_t clusterID,
  uint64_t nodeID, char *soFilename);
CPPStateMachine *CreateDBStateMachineFromFactory(uint64_t clusterID,
  uint64_t nodeID, void *factory);
void DestroyDBStateMachine(CPPStateMachine *ds);
uint64_t UpdateDBStateMachine(CPPStateMachine *ds,
  const unsigned char *data, size_t size);
LookupResult LookupDBStateMachine(CPPStateMachine *ds,
  const unsigned char *data, size_t size);
uint64_t GetHashDBStateMachine(CPPStateMachine *ds);
SnapshotResult SaveSnapshotDBStateMachine(CPPStateMachine *ds,
  uint64_t writerOID, uint64_t collectionOID, uint64_t doneChOID);
int RecoverFromSnapshotDBStateMachine(CPPStateMachine *ds,
  CollectedFiles *cf, uint64_t readerOID, uint64_t doneChOID);
void FreeLookupResult(CPPStateMachine *ds, LookupResult r);
CollectedFiles *GetCollectedFile();
void FreeCollectedFile(CollectedFiles *cf);
void AddToCollectedFile(CollectedFiles *cf, uint64_t fileID,
  const char *path, size_t pathLen, const unsigned char *metadata, size_t len);

#ifdef __cplusplus
}
#endif

#endif  // INTERNAL_CPP_WRAPPER_H_
