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

package invariants

import (
	"runtime"
)

// Is32BitArch returns a boolean value indicating whether running on a 32bit
// architecture.
func Is32BitArch() bool {
	return is32BitArch(runtime.GOARCH)
}

// IsSupportedArch returns a boolean value indicating whether running on a
// supported architecture.
func IsSupportedArch() bool {
	supported := []string{
		"arm64",
		"amd64",
	}
	for _, v := range supported {
		if runtime.GOARCH == v {
			return true
		}
	}
	return false
}

// IsSupportedOS returns a boolean value indicating whether running on a
// supported OS.
func IsSupportedOS() bool {
	return runtime.GOOS == "linux" || runtime.GOOS == "darwin"
}

func is32BitArch(arch string) bool {
	known := []string{
		"386",
		"amd64p32",
		"arm",
		"armbe",
		"mips",
		"mipsle",
		"mips64p32",
		"mips64p32le",
		"ppc",
		"riscv",
		"s390",
		"sparc",
	}
	for _, v := range known {
		if arch == v {
			return true
		}
	}
	return false
}
