// Copyright 2017-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestClusterInfo() []ClusterInfo {
	ci1 := ClusterInfo{
		ClusterID:         100,
		ConfigChangeIndex: 200,
		Nodes: map[uint64]string{
			200: "address1",
			300: "address2",
			400: "address3",
		},
	}
	ci2 := ClusterInfo{
		ClusterID:         1340,
		ConfigChangeIndex: 126200,
		Nodes: map[uint64]string{
			1200: "myaddress1",
			4300: "theiraddress2",
			6400: "heraddress3",
		},
	}
	return []ClusterInfo{ci1, ci2}
}

func TestGetFullSyncData(t *testing.T) {
	v := newView(123)
	cil := getTestClusterInfo()
	v.update(cil)
	data := v.getFullSyncData()

	v2 := newView(123)
	v2.updateFrom(data)
	assert.Equal(t, v, v2)
}

func TestConfigChangeIndexIsChecked(t *testing.T) {
	v := newView(123)
	cil := getTestClusterInfo()
	v.update(cil)

	update := []ClusterInfo{
		{
			ClusterID:         1340,
			ConfigChangeIndex: 300,
			Nodes: map[uint64]string{
				1200: "myaddress1",
				4300: "theiraddress2",
			},
		},
	}
	v.update(update)
	ci, ok := v.mu.nodehosts[1340]
	assert.True(t, ok)
	assert.Equal(t, uint64(126200), ci.ConfigChangeIndex)
	assert.Equal(t, 3, len(ci.Nodes))

	update = []ClusterInfo{
		{
			ClusterID:         1340,
			ConfigChangeIndex: 226200,
			Nodes: map[uint64]string{
				1200: "myaddress1",
				4300: "theiraddress2",
			},
		},
	}
	v.update(update)
	ci, ok = v.mu.nodehosts[1340]
	assert.True(t, ok)
	assert.Equal(t, uint64(226200), ci.ConfigChangeIndex)
	assert.Equal(t, 2, len(ci.Nodes))
}

func TestDeploymentIDIsChecked(t *testing.T) {
	v := newView(123)
	cil := getTestClusterInfo()
	v.update(cil)
	data := v.getFullSyncData()

	v2 := newView(321)
	v2.updateFrom(data)
	assert.Equal(t, 0, len(v2.mu.nodehosts))
}

func TestGetGossipData(t *testing.T) {
	v := newView(123)
	cil := getTestClusterInfo()
	v.update(cil)
	data := v.getGossipData(340)
	assert.True(t, len(data) > 0)
}
