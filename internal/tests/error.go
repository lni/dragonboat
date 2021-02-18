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

package tests

import (
	"github.com/cockroachdb/errors/errbase"
)

type stackTracer interface {
	StackTrace() errbase.StackTrace
}

// HasStack returns a boolean value indicating whether the specified error has
// stacktrace attached to it.
func HasStack(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(stackTracer); ok {
		return true
	}
	return HasStack(errbase.UnwrapOnce(err))
}
