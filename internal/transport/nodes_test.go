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

package transport

import (
	"testing"

	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/raftio"
)

func TestPeerCanBeAdded(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Fatalf("error not reported")
	}
	nodes.Add(100, 2, "a2:2")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
}

func TestPeerAddressCanNotBeUpdated(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.Add(100, 2, "a2:2")
	nodes.Add(100, 2, "a2:3")
	nodes.Add(100, 2, "a2:4")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
}

func TestPeerCanBeRemoved(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.Add(100, 2, "a2:2")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
	nodes.Remove(100, 2)
	_, _, err = nodes.Resolve(100, 2)
	if err == nil {
		t.Fatalf("error not reported")
	}
}

func TestRemoveCluster(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.Add(100, 2, "a2:2")
	nodes.Add(100, 3, "a2:3")
	nodes.Add(200, 2, "a3:2")
	nodes.RemoveCluster(100)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Errorf("cluster not removed")
	}
	_, _, err = nodes.Resolve(200, 2)
	if err != nil {
		t.Errorf("failed to get node")
	}
}

func TestRemoteAddressCanBeUsed(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Errorf("unexpected result")
	}
	nodes.AddRemote(100, 2, "a3:2")
	v, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to return the remote address")
	}
	if v != "a3:2" {
		t.Errorf("v %s, want a3:2", v)
	}
	nodes.nmu.nodes = make(map[raftio.NodeInfo]string)
	v, _, err = nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to return the remote address")
	}
	if v != "a3:2" {
		t.Errorf("v %s, want a3:2", v)
	}
}

func testInvalidAddressWillPanic(t *testing.T, addr string) {
	po := false
	nodes := NewNodes(settings.Soft.StreamConnections)
	defer func() {
		if r := recover(); r != nil {
			po = true
		}
		if !po {
			t.Errorf("failed to panic on invalid address")
		}
	}()
	nodes.Add(100, 2, addr)
}

func TestInvalidAddressWillPanic(t *testing.T) {
	testInvalidAddressWillPanic(t, "a3")
	testInvalidAddressWillPanic(t, "3")
	testInvalidAddressWillPanic(t, "abc:")
	testInvalidAddressWillPanic(t, ":")
	testInvalidAddressWillPanic(t, ":1243")
	testInvalidAddressWillPanic(t, "abc")
	testInvalidAddressWillPanic(t, "abc:67890")
}
