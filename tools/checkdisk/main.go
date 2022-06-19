// Copyright 2018-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/logdb/kv/pebble"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	dataDirectoryName  = "checkdisk-data-safe-to-delete/data1"
	dataDirectoryName2 = "checkdisk-data-safe-to-delete/data2"
	raftAddress        = "localhost:26000"
	raftAddress2       = "localhost:26001"
)

var shardcount = flag.Int("num-of-shards", 48, "number of raft shards")
var read = flag.Bool("enable-read", false, "enable read")
var readonly = flag.Bool("read-only", false, "read only")
var batched = flag.Bool("batched-logdb", false, "use batched logdb")
var cpupprof = flag.Bool("cpu-profiling", false, "run CPU profiling")
var mempprof = flag.Bool("mem-profiling", false, "run mem profiling")
var inmemfs = flag.Bool("inmem-fs", false, "use in-memory filesystem")
var clientcount = flag.Int("num-of-clients", 10000, "number of clients to use")
var seconds = flag.Int("seconds-to-run", 60, "number of seconds to run")
var ckpt = flag.Int("checkpoint-interval", 0, "checkpoint interval")
var tiny = flag.Bool("tiny-memory", false, "tiny LogDB memory limit")
var twonh = flag.Bool("two-nodehosts", false, "use two nodehosts")

type batchedLogDBFactory struct{}

func (batchedLogDBFactory) Create(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return logdb.NewLogDB(cfg,
		cb, dirs, lldirs, true, false, pebble.NewKVStore)
}

func (batchedLogDBFactory) Name() string {
	return "Sharded-Pebble"
}

type dummyStateMachine struct{}

func newDummyStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &dummyStateMachine{}
}

func (s *dummyStateMachine) Lookup(query interface{}) (interface{}, error) {
	return query, nil
}

func (s *dummyStateMachine) Update(e sm.Entry) (sm.Result, error) {
	return sm.Result{Value: 1}, nil
}

func (s *dummyStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	v := make([]byte, 4)
	if _, err := w.Write(v); err != nil {
		return err
	}
	return nil
}

func (s *dummyStateMachine) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	v := make([]byte, 4)
	if _, err := r.Read(v); err != nil {
		return err
	}
	return nil
}

func (s *dummyStateMachine) Close() error { return nil }

func main() {
	flag.Parse()
	fs := vfs.DefaultFS
	if *inmemfs {
		log.Println("using in-memory fs")
		fs = vfs.NewMemFS()
	}
	if *mempprof {
		defer func() {
			f, err := os.Create("mem.pprof")
			if err != nil {
				log.Fatal("could not create memory profile: ", err)
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Fatal("could not write memory profile: ", err)
			}
		}()
		log.Println("memory profile will be saved into file mem.pprof")
	}
	if err := fs.RemoveAll(dataDirectoryName); err != nil {
		panic(err)
	}
	if err := fs.RemoveAll(dataDirectoryName2); err != nil {
		panic(err)
	}
	defer func() {
		if err := fs.RemoveAll(dataDirectoryName); err != nil {
			panic(err)
		}
	}()
	defer func() {
		if err := fs.RemoveAll(dataDirectoryName2); err != nil {
			panic(err)
		}
	}()
	logger.GetLogger("raft").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	lc := config.GetLargeMemLogDBConfig()
	lc.SaveBufferSize = 64 * 1024 * 1024
	lc.KVMaxWriteBufferNumber = 8
	lc.KVWriteBufferSize = 256 * 1024 * 1024
	lc.KVLevel0FileNumCompactionTrigger = 6
	nhc := config.NodeHostConfig{
		NodeHostDir:    dataDirectoryName,
		RTTMillisecond: 200,
		RaftAddress:    raftAddress,
		Expert:         config.ExpertConfig{FS: fs, LogDB: lc},
	}
	if *tiny {
		log.Println("using tiny LogDB memory limit")
		nhc.Expert.LogDB = config.GetTinyMemLogDBConfig()
	}
	if *batched {
		log.Println("using batched logdb")
		nhc.Expert.LogDBFactory = batchedLogDBFactory{}
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	defer nh.Close()
	var nh2 *dragonboat.NodeHost
	if *twonh {
		nhc.NodeHostDir = dataDirectoryName2
		nhc.RaftAddress = raftAddress2
		nh2, err = dragonboat.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}
		defer nh2.Close()
	}
	rc := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     10,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		SnapshotEntries: uint64(*ckpt),
	}
	nodes := make(map[uint64]string)
	nodes[1] = raftAddress
	if *twonh {
		nodes[2] = raftAddress2
	}
	nhList := make([]*dragonboat.NodeHost, 0)
	for i := uint64(1); i <= uint64(*shardcount); i++ {
		rc.ShardID = i
		if err := nh.StartReplica(nodes, false, newDummyStateMachine, rc); err != nil {
			panic(err)
		}
		if *twonh {
			rc2 := rc
			rc2.ReplicaID = 2
			if err := nh2.StartReplica(nodes, false, newDummyStateMachine, rc2); err != nil {
				panic(err)
			}
		}
	}
	for i := uint64(1); i <= uint64(*shardcount); i++ {
		for j := 0; j < 10000; j++ {
			leaderID, _, ok, err := nh.GetLeaderID(i)
			if err != nil {
				panic(err)
			}
			if !*twonh {
				if ok && leaderID == 1 {
					nhList = append(nhList, nh)
					break
				}
			} else {
				if ok && (leaderID == 1 || leaderID == 2) {
					if leaderID == 1 {
						nhList = append(nhList, nh)
					} else {
						nhList = append(nhList, nh2)
					}
					break
				}
			}
			time.Sleep(time.Millisecond)
			if j == 9999 {
				panic("failed to elect leader")
			}
		}
	}
	if len(nhList) != *shardcount {
		panic(fmt.Sprintf("nhList len unexpected, %d", len(nhList)))
	}
	fmt.Printf("shards are ready, will run for %d seconds\n", *seconds)
	if *cpupprof {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		log.Println("cpu profile will be saved into file cpu.pprof")
	}

	doneCh := make(chan struct{}, 1)
	timer := time.NewTimer(time.Duration(*seconds) * time.Second)
	defer timer.Stop()
	go func() {
		<-timer.C
		close(doneCh)
	}()
	// keep proposing for 60 seconds
	stopper := syncutil.NewStopper()
	results := make([]uint64, *clientcount)
	if !*readonly {
		for i := uint64(0); i < uint64(*clientcount); i++ {
			workerID := i
			stopper.RunWorker(func() {
				shardID := (workerID % uint64(*shardcount)) + 1
				nh := nhList[shardID-1]
				cs := nh.GetNoOPSession(shardID)
				cmd := make([]byte, 16)
				results[workerID] = 0
				for {
					for j := 0; j < 32; j++ {
						rs, err := nh.Propose(cs, cmd, 4*time.Second)
						if err != nil {
							panic(err)
						}
						v := <-rs.ResultC()
						if v.Completed() {
							results[workerID] = results[workerID] + 1
							rs.Release()
						}
					}
					select {
					case <-doneCh:
						return
					default:
					}
				}
			})
		}
	}
	reads := struct{ v uint64 }{}
	if *read || *readonly {
		for i := uint64(0); i < uint64(*clientcount); i++ {
			workerID := i
			stopper.RunWorker(func() {
				shardID := (workerID % uint64(*shardcount)) + 1
				nh := nhList[shardID-1]
				for {
					for j := 0; j < 32; j++ {
						rs, err := nh.ReadIndex(shardID, 4*time.Second)
						if err != nil {
							panic(err)
						}
						v := <-rs.ResultC()
						if v.Completed() {
							atomic.AddUint64(&reads.v, 1)
							if _, err := nh.ReadLocalNode(rs, nil); err != nil {
								panic(err)
							}
							rs.Release()
						}
					}
					select {
					case <-doneCh:
						return
					default:
					}
				}
			})
		}
	}
	stopper.Stop()
	total := uint64(0)
	for _, v := range results {
		total = total + v
	}
	rv := atomic.LoadUint64(&reads.v)
	fmt.Printf("read %d, %d reads per second\n", rv, rv/uint64(*seconds))
	fmt.Printf("total %d, %d proposals per second\n", total, total/uint64(*seconds))
}
