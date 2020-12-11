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

package config

import (
	"testing"
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
		// set this if you want to use gRPC based RPC module for exchanging raft data between raft nodes.
		// RaftRPCFactory: rpc.NewRaftGRPC,
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
		"345.168.0.1:12345",
		"192.345.0.1:12345",
		"192.168.345.1:12345",
		"192.168.1.345:12345",
		"192 .168.0.1:12345",
		"myhost :12345",
		"1host:12345",
		"",
		"    ",
	}
	for _, v := range iva {
		checkInvalidAddress(t, v)
	}
}

func TestWitnessNodeCanNotBeAnObserver(t *testing.T) {
	cfg := Config{IsWitness: true, IsObserver: true}
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
	m := &transportModule{}
	c := NodeHostConfig{
		RaftAddress:    "localhost:9010",
		RTTMillisecond: 100,
		NodeHostDir:    "/data",
		RaftRPCFactory: m.Create,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("cfg not valid")
	}
	c.TransportModule = m
	if err := c.Validate(); err == nil {
		t.Fatalf("cfg not considered as invalid")
	}
}
