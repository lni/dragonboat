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

package stringutil

import (
	"net"
	"regexp"
	"strconv"
	"strings"
)

var (
	// HostnameRegex is the regex for valid hostnames.
	HostnameRegex = regexp.MustCompile(`^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	// IPV4Regex is the regex for valid IPv4 addresses.
	IPV4Regex = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)
)

// IsValidAddress returns a boolean indicating whether the specified address
// is valid.
func IsValidAddress(addr string) bool {
	in := strings.TrimSpace(addr)
	parts := strings.Split(in, ":")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return false
	}
	i, err := strconv.Atoi(parts[1])
	if err != nil || i <= 0 || i > 65535 {
		return false
	}
	if HostnameRegex.MatchString(parts[0]) {
		return true
	}
	if IPV4Regex.MatchString(parts[0]) {
		if net.ParseIP(parts[0]) != nil {
			return true
		}
	}
	return false
}
