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

package rsm

import (
	"testing"
)

func TestRegisteriAndUnregisterClient(t *testing.T) {
	sm := NewSessionManager()
	h1 := sm.GetSessionHash()
	_, ok := sm.ClientRegistered(123)
	if ok {
		t.Errorf("already has client with client id 123")
	}
	sm.RegisterClientID(123)
	_, ok = sm.ClientRegistered(123)
	if !ok {
		t.Errorf("client not registered")
	}
	h2 := sm.GetSessionHash()
	v := sm.UnregisterClientID(123)
	if v.Value != 123 {
		t.Errorf("failed to unregister client")
	}
	_, ok = sm.ClientRegistered(123)
	if ok {
		t.Errorf("still has client with client id 123")
	}
	h3 := sm.GetSessionHash()
	if h1 == h2 {
		t.Errorf("hash does not change")
	}
	if h1 != h3 {
		t.Errorf("hash unexpectedly changed")
	}
}
