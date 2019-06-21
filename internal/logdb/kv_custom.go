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

// +build dragonboat_no_rocksdb

package logdb

import (
	"flag"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
)

const (
	// DefaultKVStoreTypeName is the type name of the default kv store
	DefaultKVStoreTypeName = "custom"
)

func newDefaultKVStore(dir string, wal string) (kv.IKVStore, error) {
	if v := flag.Lookup("test.v"); v == nil || v.Value.String() != "true" {
		panic("not suppose to be called")
	} else {
		panic("trying to run logdb tests with invalid build tag")
	}
}
