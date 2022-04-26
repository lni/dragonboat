// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

//go:build linux
// +build linux

package tan

import (
	"os"
	"syscall"

	"github.com/lni/vfs"
	"golang.org/x/sys/unix"
)

func prealloc(f vfs.File, size int64) error {
	osf, ok := f.(*os.File)
	if !ok {
		return nil
	}
	return syscall.Fallocate(int(osf.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
}
