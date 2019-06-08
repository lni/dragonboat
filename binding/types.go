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

package main

//#include "dragonboat/binding.h"
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/lni/dragonboat"
)

func charToByte(data *C.char, len C.size_t) []byte {
	var value []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

func ucharToByte(data *C.uchar, len C.size_t) []byte {
	var value []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

func charArrayToString(data *C.char, len C.size_t) string {
	value := charToByte(data, len)
	str := string(value)
	return str
}

func charToBool(c C.char) bool {
	return c == 1
}

func cboolToBool(v C.Bool) bool {
	return v == 1
}

func getErrorCode(err error) int {
	if err == nil {
		return int(C.StatusOK)
	} else if err == dragonboat.ErrClusterNotFound {
		return int(C.ErrClusterNotFound)
	} else if err == dragonboat.ErrClusterAlreadyExist {
		return int(C.ErrClusterAlreadyExist)
	} else if err == dragonboat.ErrDeadlineNotSet {
		return int(C.ErrDeadlineNotSet)
	} else if err == dragonboat.ErrInvalidDeadline {
		return int(C.ErrInvalidDeadline)
	} else if err == dragonboat.ErrInvalidSession {
		return int(C.ErrInvalidSession)
	} else if err == dragonboat.ErrInvalidClusterSettings {
		return int(C.ErrInvalidClusterSettings)
	} else if err == dragonboat.ErrTimeoutTooSmall {
		return int(C.ErrTimeoutTooSmall)
	} else if err == dragonboat.ErrPayloadTooBig {
		return int(C.ErrPayloadTooBig)
	} else if err == dragonboat.ErrSystemBusy {
		return int(C.ErrSystemBusy)
	} else if err == dragonboat.ErrClusterClosed {
		return int(C.ErrClusterClosed)
	} else if err == dragonboat.ErrBadKey {
		return int(C.ErrBadKey)
	} else if err == dragonboat.ErrPendingConfigChangeExist {
		return int(C.ErrPendingConfigChangeExist)
	} else if err == dragonboat.ErrTimeout {
		return int(C.ErrTimeout)
	} else if err == dragonboat.ErrSystemStopped {
		return int(C.ErrSystemStopped)
	} else if err == dragonboat.ErrCanceled {
		return int(C.ErrCanceled)
	} else if err == dragonboat.ErrRejected {
		return int(C.ErrRejected)
	} else if err == dragonboat.ErrClusterNotStopped {
		return int(C.ErrClusterNotStopped)
	} else if err == dragonboat.ErrClusterNotInitialized {
		return int(C.ErrClusterNotInitialized)
	}
	panic(fmt.Sprintf("unknown error %v", err))
}
