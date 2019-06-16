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

#include "extended.h"

int rocksdb_iter_valid_not_err(const rocksdb_iterator_t* iter) {
  char *err = NULL;
  rocksdb_iter_get_error(iter, &err);
  if(err != NULL) {
    return -1;
  }
  if(rocksdb_iter_valid(iter)) {
    return 1;
  }
  return 0;
}

void rocksdb_writebatch_batched_put(rocksdb_writebatch_t* b,
  const char *key1, size_t klen1, const char* val1, size_t vlen1,
  const char *key2, size_t klen2, const char* val2, size_t vlen2,
  const char *key3, size_t klen3, const char* val3, size_t vlen3,
  const char *key4, size_t klen4, const char* val4, size_t vlen4,
  const char *key5, size_t klen5, const char* val5, size_t vlen5,
  const char *key6, size_t klen6, const char* val6, size_t vlen6,
  const char *key7, size_t klen7, const char* val7, size_t vlen7,
  const char *key8, size_t klen8, const char* val8, size_t vlen8) {
  rocksdb_writebatch_put(b, key1, klen1, val1, vlen1);
  rocksdb_writebatch_put(b, key2, klen2, val2, vlen2);
  rocksdb_writebatch_put(b, key3, klen3, val3, vlen3);
  rocksdb_writebatch_put(b, key4, klen4, val4, vlen4);
  rocksdb_writebatch_put(b, key5, klen5, val5, vlen5);
  rocksdb_writebatch_put(b, key6, klen6, val6, vlen6);
  rocksdb_writebatch_put(b, key7, klen7, val7, vlen7);
  rocksdb_writebatch_put(b, key8, klen8, val8, vlen8);
}

GetResult db_rocksdb_get(rocksdb_t* db, const rocksdb_readoptions_t* options,
  const char* key, size_t keylen)
{
  GetResult r;
  r.err = 0;
  char *err = NULL;
  r.val = rocksdb_get(db, options, key, keylen, &r.len, &err);
  if (err != NULL)
  {
    r.err = 1;
  }
  return r;
}

int get_rocksdb_major_version()
{
  return ROCKSDB_MAJOR;
}

int get_rocksdb_minor_version()
{
  return ROCKSDB_MINOR;
}

int get_rocksdb_patch_version()
{
  return ROCKSDB_PATCH;
}
