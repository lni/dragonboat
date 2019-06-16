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

/*
diskkv is IOnDiskStateMachine plugin used in various tests.
*/
package main

// // required by the plugin system
import "C"

import (
	"github.com/lni/dragonboat/internal/tests"
	sm "github.com/lni/dragonboat/statemachine"
)

// DragonboatApplicationName is the name of your plugin.
var DragonboatApplicationName string = "diskkv"

// CreateOnDiskStateMachine create the state machine IConcurrentStateMachine object.
func CreateOnDiskStateMachine(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return tests.NewDiskKVTest(clusterID, nodeID)
}
