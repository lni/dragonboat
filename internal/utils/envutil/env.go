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
	"strings"
)

// GetBoolEnvVarOrDefault returns the boolean value specified in the
// environmential variable identified as name, or it returns the specified
// default value if the environmential variable is not found.
func GetBoolEnvVarOrDefault(name string, defaultValue bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return defaultValue
	}

	if v == "1" || strings.ToUpper(v) == "TRUE" {
		return true
	}

	return false
}

// GetStringEnvVarOrEmptyString returns a string value found in the specified
// environmential variable or it returns an empty string value.
func GetStringEnvVarOrEmptyString(name string) string {
	v := os.Getenv(name)
	if len(v) == 0 {
		return ""
	}

	return v
}
