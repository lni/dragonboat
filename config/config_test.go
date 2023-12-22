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

package config

import (
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v4/raftio"
)

func ExampleNodeHostConfig() {
	nhc := NodeHostConfig{
		WALDir:         "/data/wal",
		NodeHostDir:    "/data/dragonboat-data",
		RTTMillisecond: 200,
		// RaftAddress is the public address that will be used by others to contact
		// this NodeHost instance.
		RaftAddress: "node01.raft.company.com:5012",
		// ListenAddress is the local address to listen on. This field is typically
		// set when there is port forwarding involved, e.g. your docker container
		// might has a private address of 172.17.0.2 when the public address of the
		// host is node01.raft.company.com and tcp port 5012 has been published.
		ListenAddress: "172.17.0.2:5012",
	}
	_ = nhc
}

func checkValidAddress(t *testing.T, addr string) {
	if !IsValidAddress(addr) {
		t.Errorf("valid addr %s considreed as invalid", addr)
	}
}

func checkInvalidAddress(t *testing.T, addr string) {
	if IsValidAddress(addr) {
		t.Errorf("invalid addr %s considered as valid", addr)
	}
}

func TestListenAddress(t *testing.T) {
	nhc := NodeHostConfig{
		ListenAddress: "listen.address:12345",
		RaftAddress:   "raft.address:23456",
	}
	if nhc.GetListenAddress() != nhc.ListenAddress {
		t.Errorf("unexpected listen address %s, want %s",
			nhc.GetListenAddress(), nhc.ListenAddress)
	}
	nhc.ListenAddress = ""
	if nhc.GetListenAddress() != nhc.RaftAddress {
		t.Errorf("unexpected listen address %s, want %s",
			nhc.GetListenAddress(), nhc.RaftAddress)
	}
}

func TestIsValidAddress(t *testing.T) {
	va := []string{
		"192.0.0.1:12345",
		"202.96.1.23:1234",
		"myhost:214",
		"0.0.0.0:12345",
		"node1.mydomain.com.cn:12345",
		"myhost.test:12345",
		"    myhost.test:12345 ",
	}
	for _, v := range va {
		checkValidAddress(t, v)
	}
	iva := []string{
		"192.168.0.1",
		"myhost",
		"192.168.0.1:",
		"192.168.0.1:0",
		"192.168.0.1:65536",
		"192.168.0.1:-1",
		":12345",
		":",
		"#$:%",
		"mytest:again",
		"myhost:",
		// FIXME:
		// current validator consider the below two as valid
		// "345.168.0.1:12345",
		// "192.345.0.1:12345",
		// "192.168.345.1:12345",
		// "192.168.1.345:12345",
		"192 .168.0.1:12345",
		"myhost :12345",
		"",
		"    ",
	}
	for _, v := range iva {
		checkInvalidAddress(t, v)
	}
}

func TestWitnessNodeCanNotBeNonVoting(t *testing.T) {
	cfg := Config{IsWitness: true, IsNonVoting: true}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("witness node can not be an observer")
	}
}

func TestWitnessCanNotTakeSnapshot(t *testing.T) {
	cfg := Config{IsWitness: true, SnapshotEntries: 100}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("witness node can not take snapshot")
	}
}

func TestLogDBConfigIsEmpty(t *testing.T) {
	cfg := LogDBConfig{}
	if !cfg.IsEmpty() {
		t.Fatalf("not empty")
	}
	cfg.KVMaxBackgroundCompactions = 1
	if cfg.IsEmpty() {
		t.Fatalf("still empty")
	}
}

func TestLogDBConfigMemSize(t *testing.T) {
	c := GetDefaultLogDBConfig()
	if c.MemorySizeMB() != 8192 {
		t.Errorf("unexpected default memory size")
	}
	c1 := GetTinyMemLogDBConfig()
	if c1.MemorySizeMB() != 256 {
		t.Errorf("size %d, want 256", c1.MemorySizeMB())
	}
	c2 := GetSmallMemLogDBConfig()
	if c2.MemorySizeMB() != 1024 {
		t.Errorf("size %d, want 1024", c2.MemorySizeMB())
	}
	c3 := GetMediumMemLogDBConfig()
	if c3.MemorySizeMB() != 4096 {
		t.Errorf("size %d, want 4096", c3.MemorySizeMB())
	}
	c4 := GetLargeMemLogDBConfig()
	if c4.MemorySizeMB() != 8192 {
		t.Errorf("size %d, want 8192", c4.MemorySizeMB())
	}
}

func TestTransportFactoryAndModuleCanNotBeSetTogether(t *testing.T) {
	m := &defaultTransport{}
	c := NodeHostConfig{
		RaftAddress:    "localhost:9010",
		RTTMillisecond: 100,
		NodeHostDir:    "/data",
		RaftRPCFactory: m.Create,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("cfg not valid")
	}
	c.Expert.TransportFactory = m
	if err := c.Validate(); err == nil {
		t.Fatalf("cfg not considered as invalid")
	}
}

func TestLogDBFactoryAndExpertLogDBFactoryCanNotBeSetTogether(t *testing.T) {
	f := func(NodeHostConfig,
		LogDBCallback, []string, []string) (raftio.ILogDB, error) {
		return nil, nil
	}
	c := NodeHostConfig{
		RaftAddress:    "localhost:9010",
		RTTMillisecond: 100,
		NodeHostDir:    "/data",
		LogDBFactory:   LogDBFactoryFunc(f),
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("cfg not valid")
	}
	c.Expert.LogDBFactory = &defaultLogDB{}
	if err := c.Validate(); err == nil {
		t.Fatalf("cfg not considered as invalid")
	}
}

func TestGossipMustBeConfiguredWhenDefaultNodeRegistryEnabled(t *testing.T) {
	c := NodeHostConfig{
		RaftAddress:    "localhost:9010",
		RTTMillisecond: 100,
		NodeHostDir:    "/data",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("invalid config")
	}
	c.DefaultNodeRegistryEnabled = true
	if err := c.Validate(); err == nil {
		t.Fatalf("unexpectedly considreed as valid config")
	}
	c.Gossip = GossipConfig{
		BindAddress: "localhost:12345",
		Seed:        []string{"localhost:23456"},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("invalid config")
	}
}

func TestGossipConfigIsEmtpy(t *testing.T) {
	gc := &GossipConfig{}
	if !gc.IsEmpty() {
		t.Errorf("not empty")
	}
	tests := []struct {
		bindAddr      string
		advertiseAddr string
		seed          []string
		empty         bool
	}{
		{"localhost:12345", "", []string{}, false},
		{"", "localhost:12345", []string{}, false},
		{"", "", []string{}, true},
		{"", "", []string{"127.0.0.1:12345"}, false},
	}
	for idx, tt := range tests {
		gc := &GossipConfig{
			BindAddress:      tt.bindAddr,
			AdvertiseAddress: tt.advertiseAddr,
			Seed:             tt.seed,
		}
		if gc.IsEmpty() != tt.empty {
			t.Errorf("%d, got %t, want %t", idx, gc.IsEmpty(), tt.empty)
		}
	}
}

func TestGossipConfigValidate(t *testing.T) {
	tests := []struct {
		bindAddr      string
		advertiseAddr string
		seed          []string
		valid         bool
	}{
		{"114.1.1.1:12345", "202.23.45.1:12345", []string{"128.0.0.1:1234"}, true},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"128.0.0.1:1234"}, true},
		{"myhost.com:12345", "", []string{"128.0.0.1:1234"}, true},
		{"", "202.23.45.1:12345", []string{"128.0.0.1:1234"}, false},
		{"myhost.com", "202.23.45.1:12345", []string{"128.0.0.1:1234"}, false},
		{"myhost.com:12345", "myhost2.net:12345", []string{"128.0.0.1:1234"}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"myhost.com:12345"}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"202.23.45.1:12345"}, false},
		{"myhost.com:12345", "202.23.45.1", []string{"128.0.0.1:1234"}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"128.0.0.1"}, false},
		{"myhost.com:12345", ":12345", []string{"128.0.0.1:12345"}, false},
		// FIXME:
		// current validator consider this as valid
		// {"300.0.0.1:12345", "202.23.45.1:12345", []string{"128.0.0.1:12345"}, false},
		{"myhost.com:66345", "202.23.45.1:12345", []string{"128.0.0.1:12345"}, false},
		{"myhost.com:12345", "302.23.45.1:12345", []string{"128.0.0.1:12345"}, false},
		{"myhost.com:12345", "202.23.45.1:72345", []string{"128.0.0.1:12345"}, false},
		// FIXME:
		// current validator consider this as valid
		// {"myhost.com:12345", "202.23.45.1:12345", []string{"328.0.0.1:12345"}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"128.0.0.1:65536"}, false},
		{"myhost.com:12345", "202.23.45.1:12345", []string{"128.0.0.1::12345"}, false},
		{"myhost.com:12345", "202.23.45.1::12345", []string{"128.0.0.1:12345"}, false},
		{"myhost.com::12345", "202.23.45.1:12345", []string{"128.0.0.1:12345"}, false},
		{"node1:12345", "202.96.23.1:12345", []string{"node3:12345", "node4:12345"}, true},
	}
	for idx, tt := range tests {
		gc := &GossipConfig{
			BindAddress:      tt.bindAddr,
			AdvertiseAddress: tt.advertiseAddr,
			Seed:             tt.seed,
		}
		err := gc.Validate()
		if (err != nil && tt.valid) || (err == nil && !tt.valid) {
			t.Errorf("%d, err: %v, valid: %t", idx, err, tt.valid)
		}
	}
}

func TestDefaultEngineConfig(t *testing.T) {
	nhc := &NodeHostConfig{}
	if err := nhc.Prepare(); err != nil {
		t.Errorf("prepare failed, %v", err)
	}
	ec := GetDefaultEngineConfig()
	if !reflect.DeepEqual(&nhc.Expert.Engine, &ec) {
		t.Errorf("default engine configure not set")
	}
}
