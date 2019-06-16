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

package syncutil

import (
	"testing"
)

func TestLockCanBeLockedAndUnlocked(t *testing.T) {
	l := NewLock()
	l.Lock()
	_ = t
	l.Unlock()
}

func TestTryLockCanBeUnlocked(t *testing.T) {
	l := NewLock()
	if !l.TryLock() {
		t.Errorf("TryLock failed")
	}
	l.Unlock()
}

func TestTryLockFailsWhenLocked(t *testing.T) {
	l := NewLock()
	l.Lock()
	if l.TryLock() {
		t.Errorf("try lock not suppose to success")
	}
	l.Unlock()
	if !l.TryLock() {
		t.Errorf("try lock not suppose to fail")
	}
}
