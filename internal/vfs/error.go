// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package vfs

import (
	gvfs "github.com/lni/vfs"
)

// ErrInjected is an error injected for testing purposes.
var ErrInjected = gvfs.ErrInjected

// Injector injects errors into FS.
type Injector = gvfs.Injector

// ErrorFS is a gvfs.FS implementation.
type ErrorFS = gvfs.ErrorFS

// InjectIndex implements Injector
type InjectIndex = gvfs.InjectIndex

// Op is an enum describing the type of FS operations.
type Op = gvfs.Op

// OpRead describes read operations
var OpRead = gvfs.OpRead

// OpWrite describes write operations
var OpWrite = gvfs.OpWrite

// OpSync describes the fsync operation
var OpSync = gvfs.OpSync

// OnIndex creates and returns an injector instance that returns an ErrInjected
// on the (n+1)-th invocation of its MaybeError function.
func OnIndex(index int32, op Op) *InjectIndex {
	return gvfs.OnIndex(index, op)
}

// Wrap wraps an existing IFS implementation with the specified injector.
func Wrap(fs IFS, inj Injector) *ErrorFS {
	return gvfs.Wrap(fs, inj)
}
