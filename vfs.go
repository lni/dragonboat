// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

import (
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/vfs"
)

// MemFS is a in memory vfs intended to be used in testing. User applications
// can usually ignore such vfs related types and fields.
type MemFS = vfs.MemFS

// GetTestFS returns a vfs instance that can be used in testing. User
// applications can usually ignore such vfs related types and fields.
func GetTestFS() config.IFS {
	return vfs.GetTestFS()
}
