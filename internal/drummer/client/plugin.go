// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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
	"plugin"

	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/statemachine"
)

type pluginDetails struct {
	filepath                     string
	createNativeStateMachine     func(uint64, uint64) statemachine.IStateMachine
	createConcurrentStateMachine func(uint64, uint64) statemachine.IConcurrentStateMachine
	cresteOnDiskStateMachine     func(uint64, uint64) statemachine.IOnDiskStateMachine
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
	for _, cp := range fileutil.GetPossibleSOFiles(path) {
		p, err := plugin.Open(cp)
		if err != nil {
			plog.Panicf("failed to open so file %s, %s", cp, err.Error())
		}
		nf, err := p.Lookup("DragonboatApplicationName")
		if err != nil {
			plog.Panicf("no DragonboatApplicationName in plugin %s", cp)
		}
		appName := *nf.(*string)
		csm, err := p.Lookup("CreateStateMachine")
		if err == nil {
			cf := csm.(func(uint64, uint64) statemachine.IStateMachine)
			if _, ok := result[appName]; ok {
				plog.Panicf("plugins with the same appName %s already exist", appName)
			} else {
				result[appName] = pluginDetails{createNativeStateMachine: cf}
				plog.Infof("added a create sm function from %s, appName: %s", cp, appName)
			}
			continue
		}
		csm, err = p.Lookup("CreateConcurrentStateMachine")
		if err == nil {
			cf := csm.(func(uint64, uint64) statemachine.IConcurrentStateMachine)
			if _, ok := result[appName]; ok {
				plog.Panicf("plugins with the same appName %s already exist", appName)
			} else {
				result[appName] = pluginDetails{createConcurrentStateMachine: cf}
				plog.Infof("added a create sm function from %s, appName: %s", cp, appName)
			}
			continue
		}
		csm, err = p.Lookup("CreateOnDiskStateMachine")
		if err == nil {
			cf := csm.(func(uint64, uint64) statemachine.IOnDiskStateMachine)
			if _, ok := result[appName]; ok {
				plog.Panicf("plugins with the same appName %s already exist", appName)
			} else {
				result[appName] = pluginDetails{createOnDiskStateMachine: cf}
				plog.Infof("added a create sm function from %s, appName: %s", cp, appName)
			}
		}
	}
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
