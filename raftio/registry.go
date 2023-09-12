// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

/*
Package raftio contains structs, interfaces and function definitions required
to build custom persistent Raft log storage and transport modules.

Structs, interfaces and functions defined in the raftio package are only
required when building your custom persistent Raft log storage or transport
modules. Skip this package if you plan to use the default built-in LogDB and
transport modules provided by Dragonboat.

Structs, interfaces and functions defined in the raftio package are not
considered as a part of Dragonboat's public APIs. Breaking changes might
happen in the coming minor releases.
*/
package raftio

// INodeRegistry is the registry interface used to resolve all known
// NodeHosts and their shards and replicas in the system.
type INodeRegistry interface {
	Close() error
	Add(shardID uint64, replicaID uint64, url string)
	Remove(shardID uint64, replicaID uint64)
	RemoveShard(shardID uint64)
	Resolve(shardID uint64, replicaID uint64) (string, string, error)
}
