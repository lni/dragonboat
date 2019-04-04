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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/tests"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

func benchmarkNoPool128Allocs(b *testing.B, sz uint64) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := make([]byte, sz)
			b.SetBytes(int64(sz))
			if uint64(len(m)) < sz {
				b.Errorf("len(m) < %d", sz)
			}
		}
	})
}

func BenchmarkNoPool128Allocs512Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 512)
}

func BenchmarkNoPool128Allocs15Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 15)
}

func BenchmarkNoPool128Allocs2Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 2)
}

func BenchmarkNoPool128Allocs16Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 16)
}

func BenchmarkNoPool128Allocs17Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 17)
}

func BenchmarkAddToEntryQueue(b *testing.B) {
	b.ReportAllocs()
	q := newEntryQueue(1000000, 0)
	total := uint64(0)
	entry := pb.Entry{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := atomic.AddUint64(&total, 1)
			if t%2048 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get(false)
			} else {
				q.add(entry)
			}
		}
	})
}

func benchmarkProposeN(b *testing.B, sz int) {
	b.ReportAllocs()
	data := make([]byte, sz)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint64(0)
	q := newEntryQueue(2048, 0)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	session := client.NewNoOPSession(1, random.LockGuardedRand)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&total, 1)
			b.SetBytes(int64(sz))
			rs, err := pp.propose(session, data, nil, time.Second)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get(false)
			}
			pp.applied(rs.key, rs.clientID, rs.seriesID, 1, false)
			rs.Release()
		}
	})
}

func BenchmarkPropose16(b *testing.B) {
	benchmarkProposeN(b, 16)
}

func BenchmarPropose128(b *testing.B) {
	benchmarkProposeN(b, 128)
}

func BenchmarkPropose1024(b *testing.B) {
	benchmarkProposeN(b, 1024)
}

func BenchmarkPendingProposalNextKey(b *testing.B) {
	b.ReportAllocs()
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	q := newEntryQueue(2048, 0)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	b.RunParallel(func(pb *testing.PB) {
		clientID := rand.Uint64()
		for pb.Next() {
			pp.nextKey(clientID)
		}
	})
}

func BenchmarkReadIndexRead(b *testing.B) {
	b.ReportAllocs()
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint64(0)
	q := newReadIndexQueue(2048)
	pri := newPendingReadIndex(p, q, 200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&total, 1)
			rs, err := pri.read(nil, time.Second)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get()
			}
			rs.Release()
		}
	})
}

func benchmarkMarshalEntryN(b *testing.B, sz int) {
	b.ReportAllocs()
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, sz),
	}
	data := make([]byte, e.Size())
	for i := 0; i < b.N; i++ {
		n, err := e.MarshalTo(data)
		if n > len(data) {
			b.Errorf("n > len(data)")
		}
		b.SetBytes(int64(n))
		if err != nil {
			b.Errorf("%v", err)
		}
	}
}

func BenchmarkMarshalEntry16(b *testing.B) {
	benchmarkMarshalEntryN(b, 16)
}

func BenchmarkMarshalEntry128(b *testing.B) {
	benchmarkMarshalEntryN(b, 128)
}

func BenchmarkMarshalEntry1024(b *testing.B) {
	benchmarkMarshalEntryN(b, 1024)
}

func BenchmarkWorkerReady(b *testing.B) {
	b.ReportAllocs()
	rc := newWorkReady(1)
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.clusterReady(1)
		}
	})
}

func BenchmarkReadyCluster(b *testing.B) {
	b.ReportAllocs()
	rc := newReadyCluster()
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.setClusterReady(1)
		}
	})
}

func BenchmarkFSyncLatency(b *testing.B) {
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb")
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, 8*1024),
	}
	u := pb.Update{
		ClusterID:     1,
		NodeID:        1,
		EntriesToSave: []pb.Entry{e},
	}
	rdbctx := db.GetLogDBThreadContext()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		if err := db.SaveRaftState([]pb.Update{u}, rdbctx); err != nil {
			b.Fatalf("%v", err)
		}
		rdbctx.Reset()
	}
}

func benchmarkSaveRaftState(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb")
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	clusterID := uint64(1)
	b.StartTimer()
	b.RunParallel(func(pbt *testing.PB) {
		rdbctx := db.GetLogDBThreadContext()
		e := pb.Entry{
			Index:       12843560,
			Term:        123,
			Type:        pb.ApplicationEntry,
			Key:         13563799145,
			ClientID:    234926831800,
			SeriesID:    12843560,
			RespondedTo: 12843550,
			Cmd:         make([]byte, sz),
		}
		cid := atomic.AddUint64(&clusterID, 1)
		bytes := e.Size() * 128
		u := pb.Update{
			ClusterID: cid,
			NodeID:    1,
		}
		iidx := e.Index
		for i := uint64(0); i < 128; i++ {
			e.Index = iidx + i
			u.EntriesToSave = append(u.EntriesToSave, e)
		}
		for pbt.Next() {
			rdbctx.Reset()
			if err := db.SaveRaftState([]pb.Update{u}, rdbctx); err != nil {
				b.Errorf("%v", err)
			}
			b.SetBytes(int64(bytes))
		}
	})
}

func BenchmarkSaveRaftState16(b *testing.B) {
	benchmarkSaveRaftState(b, 16)
}

func BenchmarkSaveRaftState128(b *testing.B) {
	benchmarkSaveRaftState(b, 128)
}

func BenchmarkSaveRaftState1024(b *testing.B) {
	benchmarkSaveRaftState(b, 1024)
}

type benchmarkMessageHandler struct {
	expected uint64
	count    uint64
	ch       chan struct{}
}

func (h *benchmarkMessageHandler) wait() {
	<-h.ch
}

func (h *benchmarkMessageHandler) reset() {
	atomic.StoreUint64(&h.count, 0)
}

func (h *benchmarkMessageHandler) HandleMessageBatch(batch pb.MessageBatch) {
	v := atomic.AddUint64(&h.count, uint64(len(batch.Requests)))
	if v >= h.expected {
		h.ch <- struct{}{}
	}
}

func (h *benchmarkMessageHandler) HandleUnreachable(clusterID uint64, nodeID uint64) {
}

func (h *benchmarkMessageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, rejected bool) {
}

func (h *benchmarkMessageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
}

func benchmarkTransport(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("transport")
	l.SetLevel(logger.ERROR)
	l = logger.GetLogger("grpc")
	l.SetLevel(logger.ERROR)
	addr1 := "localhost:43567"
	addr2 := "localhost:43568"
	nhc1 := config.NodeHostConfig{
		RaftAddress: addr1,
	}
	ctx1 := server.NewContext(nhc1)
	nhc2 := config.NodeHostConfig{
		RaftAddress: addr2,
	}
	ctx2 := server.NewContext(nhc2)
	nodes1 := transport.NewNodes(settings.Soft.StreamConnections)
	nodes2 := transport.NewNodes(settings.Soft.StreamConnections)
	nodes1.AddRemoteAddress(1, 2, addr2)
	t1 := transport.NewTransport(nhc1, ctx1, nodes1, nil)
	t1.SetUnmanagedDeploymentID()
	t2 := transport.NewTransport(nhc2, ctx2, nodes2, nil)
	t2.SetUnmanagedDeploymentID()
	defer t2.Stop()
	defer t1.Stop()
	handler1 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	handler2 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	t2.SetMessageHandler(handler1)
	t1.SetMessageHandler(handler2)
	msgs := make([]pb.Message, 0)
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, sz),
	}
	for i := 0; i < 128; i++ {
		m := pb.Message{
			Type:      pb.Replicate,
			To:        2,
			From:      1,
			ClusterId: 1,
			Term:      100,
			LogTerm:   100,
			LogIndex:  123456789,
			Commit:    123456789,
		}
		for j := 0; j < 64; j++ {
			m.Entries = append(m.Entries, e)
		}
		msgs = append(msgs, m)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		handler1.reset()
		for _, msg := range msgs {
			t1.ASyncSend(msg)
		}
		handler1.wait()
	}
}

func BenchmarkTransport16(b *testing.B) {
	benchmarkTransport(b, 16)
}

func BenchmarkTransport128(b *testing.B) {
	benchmarkTransport(b, 128)
}

func BenchmarkTransport1024(b *testing.B) {
	benchmarkTransport(b, 1024)
}

type noopNodeProxy struct {
}

func (n *noopNodeProxy) RestoreRemotes(pb.Snapshot)                     {}
func (n *noopNodeProxy) ApplyUpdate(pb.Entry, uint64, bool, bool, bool) {}
func (n *noopNodeProxy) ApplyConfigChange(pb.ConfigChange)              {}
func (n *noopNodeProxy) ConfigChangeProcessed(uint64, bool)             {}
func (n *noopNodeProxy) NodeID() uint64                                 { return 1 }
func (n *noopNodeProxy) ClusterID() uint64                              { return 1 }

func benchmarkStateMachineStep(b *testing.B, sz int, noopSession bool) {
	b.ReportAllocs()
	b.StopTimer()
	ds := &tests.NoOP{}
	done := make(chan struct{})
	nds := rsm.NewNativeStateMachine(1, 1, rsm.NewRegularStateMachine(ds), done)
	smo := rsm.NewStateMachine(nds, nil, false, &noopNodeProxy{})
	idx := uint64(0)
	var s *client.Session
	if noopSession {
		s = client.NewNoOPSession(1, random.LockGuardedRand)
	} else {
		s = &client.Session{
			ClusterID: 1,
			ClientID:  1234576,
		}
	}
	e := pb.Entry{
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    s.ClientID,
		SeriesID:    s.SeriesID,
		RespondedTo: s.RespondedTo,
		Cmd:         make([]byte, sz),
	}
	entries := make([]pb.Entry, 0)
	batch := make([]rsm.Commit, 0)
	smEntries := make([]sm.Entry, 0)
	commit := rsm.Commit{
		SnapshotAvailable: false,
	}
	if !noopSession {
		idx++
		e.Index = idx
		e.SeriesID = client.SeriesIDForRegister
		entries = append(entries, e)
		commit.Entries = entries
		smo.CommitC() <- commit
		smo.Handle(batch, smEntries)
	}
	b.StartTimer()
	for x := 0; x < b.N; x++ {
		entries = entries[:0]
		for i := uint64(0); i < 128; i++ {
			idx++
			e.Index = idx
			entries = append(entries, e)
		}
		commit.Entries = entries
		smo.CommitC() <- commit
		smo.Handle(batch, smEntries)
	}
}

func BenchmarkStateMachineStepNoOPSession16(b *testing.B) {
	benchmarkStateMachineStep(b, 16, true)
}

func BenchmarkStateMachineStepNoOPSession128(b *testing.B) {
	benchmarkStateMachineStep(b, 128, true)
}

func BenchmarkStateMachineStepNoOPSession1024(b *testing.B) {
	benchmarkStateMachineStep(b, 1024, true)
}

func BenchmarkStateMachineStep16(b *testing.B) {
	benchmarkStateMachineStep(b, 16, false)
}

func BenchmarkStateMachineStep128(b *testing.B) {
	benchmarkStateMachineStep(b, 128, false)
}

func BenchmarkStateMachineStep1024(b *testing.B) {
	benchmarkStateMachineStep(b, 1024, false)
}
