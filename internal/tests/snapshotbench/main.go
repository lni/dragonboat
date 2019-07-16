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

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/rsm"
	serverConfig "github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/goutils/fileutil"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

//
// This program is used to benchmark the snapshot streaming performance of
// dragonboat. It suppose to run on two networked machines. The purpose of
// this test is not to show how great is the streaming performance, but more
// to demonstrate that the steaming throughput is not unexpectedly slow.
// Our tests on a NVME SSD with 2GBytes/sec sequential write bandwidth shows
// that the performance is about 1GBytes/sec using regular 40GE NICs.
//

var (
	testDirName        string = "snapshot_benchmark_safe_to_delete"
	filename           string = "snapshot.data"
	blockCount         uint64 = 1024 * 8
	blockSize          uint64 = 1024 * 1024
	defaultClusterID   uint64 = 100
	defaultIndex       uint64 = 10000
	secondaryClusterID uint64 = 101
	serverNodeID       uint64 = 100
	clientNodeID       uint64 = 200
)

type messageHandler struct {
	unreachable uint64
	reqs        uint64
	completed   uint64
	failed      uint64
}

func (h *messageHandler) HandleMessageBatch(batch pb.MessageBatch) {
	log.Printf("received a snapshot")
	h.reqs++
}

func (h *messageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
}

func (h *messageHandler) HandleUnreachable(clusterID uint64, nodeID uint64) {
	h.unreachable++
}

func (h *messageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, rejected bool) {
	if rejected {
		h.failed++
	} else {
		h.completed++
	}
}

type snapshotLocator struct {
	nodeHostDir string
}

func newSnapshotLocator(dir string) *snapshotLocator {
	s := &snapshotLocator{
		nodeHostDir: dir,
	}
	return s
}

func (l *snapshotLocator) GetSnapshotDir(clusterID uint64, nodeID uint64) string {
	np := fmt.Sprintf("%d-%d", clusterID, nodeID)
	return filepath.Join(testDirName, l.nodeHostDir, np)
}

func (l *snapshotLocator) CreateDirectory(clusterID uint64, nodeID uint64) {
	dir := l.GetSnapshotDir(clusterID, nodeID)
	fileutil.MkdirAll(dir)
}

func removeDirectory(dir string) {
	os.RemoveAll(dir)
}

func checkFlag(addr string, remote string, server bool, client bool) {
	if len(addr) == 0 {
		panic("address must be specified")
	}
	if server && client {
		panic("server and client mode both specified")
	}
	if !server && !client {
		panic("must specify one of the server and client mode")
	}
	if client && len(remote) == 0 {
		panic("remote not specified in client mode")
	}
}

func createSnapshot(locator *snapshotLocator, clusterID uint64) pb.Message {
	dir := locator.GetSnapshotDir(clusterID, clientNodeID)
	m := pb.Message{
		Type:      pb.MTSnapshot,
		To:        serverNodeID,
		From:      clientNodeID,
		ClusterId: clusterID,
		Term:      100,
		LogTerm:   100,
		LogIndex:  20000,
		Commit:    10000,
		Snapshot: pb.Snapshot{
			Filename: filename,
			Filepath: filepath.Join(dir, filename),
			FileSize: blockSize*(blockCount+1) + rsm.SnapshotHeaderSize,
			Index:    defaultIndex,
			Term:     100,
		},
	}
	writer, err := rsm.NewSnapshotWriter(m.Snapshot.Filepath)
	if err != nil {
		panic(err)
	}
	data := make([]byte, blockSize)
	rand.Read(data)
	for i := uint64(0); i < blockCount+1; i++ {
		if i%1000 == 0 {
			log.Printf("%d blocks written", i)
		}
		n, err := writer.Write(data)
		if uint64(n) != blockSize {
			panic("short write")
		}
		if err != nil {
			panic(err)
		}
	}
	if err := writer.SaveHeader(blockSize, blockCount*blockSize); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}

	return m
}

func clientMain(t *transport.Transport,
	handler *messageHandler, parallel bool, msg pb.Message, msg2 pb.Message) {
	st := time.Now().UnixNano()
	if !t.ASyncSendSnapshot(msg) {
		panic("send snapshot failed")
	}
	if parallel {
		if !t.ASyncSendSnapshot(msg2) {
			panic("send snapshot failed")
		}
	}
	expected := uint64(1)
	if parallel {
		expected = 2
	}
	for {
		if handler.completed == expected {
			log.Printf("completed!")
			break
		}
		if handler.failed > 0 {
			panic("failed")
		}
		time.Sleep(time.Millisecond)
	}
	cost := (time.Now().UnixNano() - st) / 1000000
	bw := blockSize * blockCount * 1000 * expected / uint64(cost)
	bw = bw / 1000000
	log.Printf("bw: %dMBytes/sec", bw)
}

func serverMain() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	done := false
	for !done {
		select {
		case sig := <-c:
			if sig != syscall.SIGINT {
				continue
			}
			log.Print("Ctrl+C pressed!")
			done = true
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	addr := flag.String("addr", "", "address of the program")
	server := flag.Bool("server", false, "server mode")
	client := flag.Bool("client", false, "client mode")
	remote := flag.String("remote", "", "remote address")
	profiling := flag.Bool("profiling", false, "cpu profiling")
	parallel := flag.Bool("parallel", false, "parallel streaming")
	tls := flag.Bool("tls", false, "use MutualTLS")
	flag.Parse()
	if *profiling {
		f, err := os.Create("snapbench_cpu_profile.pprof")
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
		defer pprof.StopCPUProfile()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
	}

	checkFlag(*addr, *remote, *server, *client)
	defer removeDirectory(testDirName)
	nhConfig := config.NodeHostConfig{
		NodeHostDir: "snapbench",
		RaftAddress: *addr,
	}
	if *tls {
		log.Println("use Mutual TLS")
		nhConfig.MutualTLS = true
		nhConfig.CAFile = "cert/ca.crt"
		nhConfig.KeyFile = "cert/peer.key"
		nhConfig.CertFile = "cert/peer.crt"
	}
	if err := nhConfig.Validate(); err != nil {
		panic(fmt.Sprintf("nhConfig validation failed %v", err))
	}
	ctx := serverConfig.NewContext(nhConfig)
	nodes := transport.NewNodes(settings.Soft.StreamConnections)
	if *client {
		nodes.AddRemoteAddress(defaultClusterID, serverNodeID, *remote)
		nodes.AddRemoteAddress(secondaryClusterID, serverNodeID, *remote)
	}
	locator := newSnapshotLocator(nhConfig.NodeHostDir)
	var msg pb.Message
	var msg2 pb.Message
	if *client {
		locator.CreateDirectory(defaultClusterID, clientNodeID)
		msg = createSnapshot(locator, defaultClusterID)
		if *parallel {
			locator.CreateDirectory(secondaryClusterID, clientNodeID)
			msg2 = createSnapshot(locator, secondaryClusterID)
		}
		log.Printf("snapshot is ready")
	} else {
		locator.CreateDirectory(defaultClusterID, serverNodeID)
		if *parallel {
			locator.CreateDirectory(secondaryClusterID, serverNodeID)
		}
	}
	handler := &messageHandler{}
	trans := transport.NewTransport(nhConfig, ctx, nodes, locator.GetSnapshotDir)
	trans.SetDeploymentID(1)
	trans.SetMessageHandler(handler)
	log.Printf("transport is ready")
	if *client {
		log.Printf("going to send snapshot")
		clientMain(trans, handler, *parallel, msg, msg2)
	} else {
		serverMain()
	}
}
