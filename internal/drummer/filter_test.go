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

// +build !dragonboat_slowtest
// +build !dragonboat_monkeytest

package drummer

import (
	"testing"
)

func getTestnodeHostSpecList() []*nodeHostSpec {
	n1 := &nodeHostSpec{
		Address:  "a1",
		Region:   "region-1",
		Tick:     100,
		Clusters: make(map[uint64]struct{}),
	}
	n1.Clusters[100] = struct{}{}
	n1.Clusters[200] = struct{}{}
	n2 := &nodeHostSpec{
		Address:  "a2",
		Region:   "region-2",
		Tick:     200,
		Clusters: make(map[uint64]struct{}),
	}
	n2.Clusters[200] = struct{}{}
	n3 := &nodeHostSpec{
		Address:  "a3",
		Region:   "region-2",
		Tick:     210,
		Clusters: make(map[uint64]struct{}),
	}
	n3.Clusters[100] = struct{}{}
	n3.Clusters[200] = struct{}{}
	n4 := &nodeHostSpec{
		Address:  "a4",
		Region:   "region-3",
		Tick:     300,
		Clusters: make(map[uint64]struct{}),
	}
	n4.Clusters[300] = struct{}{}
	n4.Clusters[200] = struct{}{}
	l := make([]*nodeHostSpec, 0)
	return append(l, n1, n2, n3, n4)
}

func TestBasicFilter(t *testing.T) {
	l := getTestnodeHostSpecList()
	bf := newBasicFilter(300)
	r := bf.filter(l)
	if len(r) != 3 {
		t.Errorf("len(r)=%d, want 3", len(r))
	}
	bf = newBasicFilter(100)
	r = bf.filter(l)
	if len(r) != 2 {
		t.Errorf("len(r)=%d, want 2", len(r))
	}
}

func TestLiveFilter(t *testing.T) {
	l := getTestnodeHostSpecList()
	lf := newLiveFilter(300, 100)
	r := lf.filter(l)
	if len(r) != 2 {
		t.Errorf("len(r)=%d, want 2", len(r))
	}
}

func TestRegionFilter(t *testing.T) {
	l := getTestnodeHostSpecList()
	rf := newRegionFilter("region-2")
	r := rf.filter(l)
	if len(r) != 2 {
		t.Errorf("len(r)=%d, want 2", len(r))
	}
}

func TestDrummerFilter(t *testing.T) {
	l := getTestnodeHostSpecList()
	df := newDrummerFilter(300, 300, 100)
	r := df.filter(l)
	if len(r) != 1 {
		t.Errorf("len(r)=%d, want 1", len(r))
	}
	if r[0].Address != "a3" {
		t.Errorf("got node %s, want node a3", r[0].Address)
	}
}

func TestDrummerRegionFilter(t *testing.T) {
	l := getTestnodeHostSpecList()
	df := newDrummerRegionFilter("region-2", 100, 300, 110)
	r := df.filter(l)
	if len(r) != 1 {
		t.Fatalf("len(r)=%d, want 1", len(r))
	}
	if r[0].Address != "a2" {
		t.Errorf("got node %s, want node a2", r[0].Address)
	}
}
