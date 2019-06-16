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

#ifndef GOROCKSDB_EXTENDED_H
#define GOROCKSDB_EXTENDED_H

#ifdef __cplusplus
extern "C" {
#endif

#include "rocksdb/version.h"
#include "rocksdb/c.h"

int rocksdb_iter_valid_not_err(const rocksdb_iterator_t* iter);

void rocksdb_writebatch_batched_put(rocksdb_writebatch_t*,
  const char *key1, size_t klen1, const char* val1, size_t vlen1,
  const char *key2, size_t klen2, const char* val2, size_t vlen2,
  const char *key3, size_t klen3, const char* val3, size_t vlen3,
  const char *key4, size_t klen4, const char* val4, size_t vlen4,
  const char *key5, size_t klen5, const char* val5, size_t vlen5,
  const char *key6, size_t klen6, const char* val6, size_t vlen6,
  const char *key7, size_t klen7, const char* val7, size_t vlen7,
  const char *key8, size_t klen8, const char* val8, size_t vlen8);

typedef struct GetResult {
  int err;
  char* val;
  size_t len;
} GetResult;

GetResult db_rocksdb_get(rocksdb_t* db, const rocksdb_readoptions_t* options,
  const char* key, size_t keylen);

int get_rocksdb_major_version();
int get_rocksdb_minor_version();
int get_rocksdb_patch_version();

#ifdef __cplusplus
}
#endif
#endif // GOROCKSDB_EXTENDED_H
