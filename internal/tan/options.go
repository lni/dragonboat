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

package tan

import (
	"github.com/lni/vfs"
)

const (
	// MaxManifestFileSize is the default max manifest file size
	MaxManifestFileSize int64 = 1024 * 1024 * 2
	// MaxLogFileSize is the default max log file size
	MaxLogFileSize int64 = 1024 * 1024 * 64
)

// Options is the option type used by tan
type Options struct {
	MaxLogFileSize      int64
	MaxManifestFileSize int64
	FS                  vfs.FS
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified. Returns the new options.
func (o *Options) EnsureDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.MaxLogFileSize == 0 {
		o.MaxLogFileSize = MaxLogFileSize
	}
	if o.MaxManifestFileSize == 0 {
		o.MaxManifestFileSize = MaxManifestFileSize
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	return o
}
