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

package envutil

import (
	"os"
)

// GetConfigDirs returns a list of location where configuration files
// can be located.
func GetConfigDirs() []string {
	directories := make([]string, 0)
	wd, err := os.Getwd()
	if err == nil && len(wd) > 0 {
		directories = append(directories, wd)
	}
	hd, err := Dir()
	if err == nil && len(hd) > 0 {
		directories = append(directories, hd)
	}
	directories = append(directories, "/etc/dragonboat")
	return directories
}
