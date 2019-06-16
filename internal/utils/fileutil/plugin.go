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

package fileutil

import (
	"io/ioutil"
	"regexp"
	"strings"
)

var (
	appNameRegex     = regexp.MustCompile(`^dragonboat-cpp-plugin-(?P<appname>.+)\.so$`)
	cppFileNameRegex = regexp.MustCompile(`^dragonboat-cpp-plugin-.+\.so$`)
	soFileNameRegex  = regexp.MustCompile(`^dragonboat-plugin-.+\.so$`)
)

// GetAppNameFromFilename returns the app name from the filename.
func GetAppNameFromFilename(soName string) string {
	results := appNameRegex.FindStringSubmatch(soName)
	return results[1]
}

// GetPossibleCPPSOFiles returns a list of possible .so files found in the
// specified path.
func GetPossibleCPPSOFiles(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil
	}
	result := make([]string, 0)
	for _, f := range files {
		fn := strings.ToLower(f.Name())
		if !f.IsDir() && cppFileNameRegex.MatchString(fn) {
			result = append(result, fn)
		}
	}
	return result
}

// GetPossibleSOFiles returns a list of possible .so files.
func GetPossibleSOFiles(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil
	}
	result := make([]string, 0)
	for _, f := range files {
		fn := strings.ToLower(f.Name())
		if !f.IsDir() && soFileNameRegex.MatchString(fn) {
			result = append(result, fn)
		}
	}
	return result
}
