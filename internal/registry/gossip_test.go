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

package registry

import (
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/id"
)

const (
	testNodeHostID1 = "123e4567-e89b-12d3-a456-426614174000"
	testNodeHostID2 = "123e4567-e89b-12d3-a456-426614174001"
)

func TestMeta(t *testing.T) {
	m := meta{
		RaftAddress: "localhost:9090",
		Data:        []byte("localhost:1080"),
	}
	data := m.marshal()
	m2 := meta{}
	assert.True(t, m2.unmarshal(data))
	assert.Equal(t, m, m2)
}

func TestMetaStore(t *testing.T) {
	m := metaStore{}
	meta := meta{RaftAddress: "localhost:9090"}
	m.put("123", meta)
	v, ok := m.get("123")
	assert.True(t, ok)
	assert.Equal(t, meta, v)
	m.delete("123")
	_, ok = m.get("123")
	assert.False(t, ok)
}

func TestGossipRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid := testNodeHostID1
	nhConfig := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	r, err := NewGossipRegistry(nhid, nil, nhConfig, 1, id.IsNodeHostID)
	require.NoError(t, err)
	defer func() {
		err := r.Close()
		require.NoError(t, err)
	}()
	require.Equal(t, 1, r.NumMembers())
	r.Add(123, 456, nhid)
	addr, _, err := r.Resolve(123, 456)
	require.NoError(t, err)
	require.Equal(t, nhConfig.RaftAddress, addr)
	// remove node
	r.Remove(123, 456)
	_, _, err = r.Resolve(123, 456)
	require.Equal(t, ErrUnknownTarget, err)
	// add back
	r.Add(123, 456, nhid)
	addr, _, err = r.Resolve(123, 456)
	require.NoError(t, err)
	require.Equal(t, nhConfig.RaftAddress, addr)
	// remove shard
	r.RemoveShard(123)
	_, _, err = r.Resolve(123, 456)
	require.Equal(t, ErrUnknownTarget, err)
}

func TestGossipManagerCanBeCreatedAndStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid := testNodeHostID1
	nhConfig := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	m, err := newGossipManager(nhid, nil, nhConfig)
	require.NoError(t, err)
	defer func() {
		err := m.Close()
		require.NoError(t, err)
	}()
	require.Equal(t, 1, m.numMembers())
	require.Equal(t, "127.0.0.1:26001", m.advertiseAddress())
	addr, ok := m.GetRaftAddress(nhid)
	require.True(t, ok)
	require.Equal(t, nhConfig.RaftAddress, addr)
}

func TestGossipManagerCanGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid1 := testNodeHostID1
	nhConfig1 := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Expert: config.ExpertConfig{
			TestGossipProbeInterval: 10 * time.Millisecond,
		},
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	nhid2 := testNodeHostID2
	nhConfig2 := config.NodeHostConfig{
		RaftAddress: "localhost:27002",
		Expert: config.ExpertConfig{
			TestGossipProbeInterval: 10 * time.Millisecond,
		},
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26002",
			AdvertiseAddress: "127.0.0.1:26002",
			Seed:             []string{"127.0.0.1:26001"},
		},
	}
	m1, err := newGossipManager(nhid1, nil, nhConfig1)
	require.NoError(t, err)
	defer func() {
		err := m1.Close()
		require.NoError(t, err)
	}()
	m2, err := newGossipManager(nhid2, nil, nhConfig2)
	require.NoError(t, err)
	defer func() {
		err := m2.Close()
		require.NoError(t, err)
	}()
	retry := 0
	for retry < 1000 {
		retry++
		time.Sleep(5 * time.Millisecond)
		if m1.numMembers() != 2 || m2.numMembers() != 2 {
			continue
		}
		addr, ok := m1.GetRaftAddress(nhid2)
		if !ok || addr != nhConfig2.RaftAddress {
			continue
		}
		addr, ok = m2.GetRaftAddress(nhid1)
		if !ok || addr != nhConfig1.RaftAddress {
			continue
		}
		return
	}
	require.Fail(t, "failed to complete all queries")
}
