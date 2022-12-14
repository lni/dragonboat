// Copyright 2017-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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

package dragonboat

// INodeHostRegistry provides APIs for querying data shared between NodeHost
// instances via gossip.
type INodeHostRegistry interface {
	NumOfShards() int
	GetMeta(nhID string) ([]byte, bool)
	GetShardInfo(shardID uint64) (ShardView, bool)
}
