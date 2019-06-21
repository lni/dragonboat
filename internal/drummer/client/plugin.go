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

package client

import (
	"fmt"

	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

type pluginDetails struct {
	filepath                     string
	createNativeStateMachine     func(uint64, uint64) sm.IStateMachine
	createConcurrentStateMachine func(uint64, uint64) sm.IConcurrentStateMachine
	createOnDiskStateMachine     func(uint64, uint64) sm.IOnDiskStateMachine
}

func (pd *pluginDetails) isRegularStateMachine() bool {
	return pd.createNativeStateMachine != nil
}

func (pd *pluginDetails) isConcurrentStateMachine() bool {
	return pd.createConcurrentStateMachine != nil
}

func (pd *pluginDetails) isOnDiskStateMachine() bool {
	return pd.createOnDiskStateMachine != nil
}

func getPluginMap(path string) map[string]pluginDetails {
	result := make(map[string]pluginDetails)
	result = getNativePlugins(path, result)
	result = getCppPlugins(path, result)
	return result
}

func getNativePlugins(path string,
	result map[string]pluginDetails) map[string]pluginDetails {
	result["kvtest"] = pluginDetails{createNativeStateMachine: tests.NewKVTest}
	result["concurrentkv"] = pluginDetails{createConcurrentStateMachine: tests.NewConcurrentKVTest}
	result["diskkv"] = pluginDetails{createOnDiskStateMachine: tests.NewDiskKVTest}
	return result
}

func getCppPlugins(path string,
	result map[string]pluginDetails) map[string]pluginDetails {
	for _, cp := range fileutil.GetPossibleCPPSOFiles(path) {
		// FIXME:
		// re-enable the following check
		// check whether using cgo in multiraft package is going trigger
		// the known bug that is affecting the go plugin.

		//if !isValidCPPPlugin(cp) {
		//	panic(fmt.Sprintf("invalid cpp plugin at %s", cp))
		//}
		appName := fileutil.GetAppNameFromFilename(cp)
		entryName := fmt.Sprintf("cpp-%s", appName)
		plog.Infof("adding a C++ plugin %s, entryName: %s, appName: %s",
			cp, entryName, appName)
		result[entryName] = pluginDetails{filepath: cp}
	}
	return result
}
