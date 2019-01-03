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

package config

import (
	"testing"
)

func ExampleNodeHostConfig() {
	// import "github.com/lni/dragonboat/plugin/rpc"
	nhc := NodeHostConfig{
		WALDir:         "/data/wal",
		NodeHostDir:    "/data/dragonboat-data",
		RTTMillisecond: 200,
		RaftAddress:    "node01.raft.company.com:5012",
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

func TestIsValidAddress(t *testing.T) {
	va := []string{
		"192.0.0.1:12345",
		"202.96.1.23:1234",
		"myhost:214",
		"0.0.0.0:1", // your choice
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
