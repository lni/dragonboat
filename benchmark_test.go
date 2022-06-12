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

package dragonboat

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/snappy"
	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/registry"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/tests"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/internal/utils/dio"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func benchmarkAllocs(b *testing.B, sz uint64) {
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

func BenchmarkAllocs16Bytes(b *testing.B) {
	benchmarkAllocs(b, 16)
}

func BenchmarkAllocs512Bytes(b *testing.B) {
	benchmarkAllocs(b, 512)
}

func BenchmarkAllocs4096Bytes(b *testing.B) {
	benchmarkAllocs(b, 4096)
}

func benchmarkEncodedPayload(b *testing.B, ct dio.CompressionType, sz uint64) {
	b.ReportAllocs()
	b.SetBytes(int64(sz))
	input := make([]byte, sz)
	rand.Read(input)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rsm.GetEncoded(ct, input, nil)
	}
}

func BenchmarkSnappyEncodedPayload16Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.Snappy, 16)
}

func BenchmarkSnappyEncodedPayload512Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.Snappy, 512)
}

func BenchmarkSnappyEncodedPayload4096Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.Snappy, 4096)
}

func BenchmarkNoCompressionEncodedPayload16Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.NoCompression, 16)
}

func BenchmarkNoCompressionEncodedPayload512Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.NoCompression, 512)
}

func BenchmarkNoCompressionEncodedPayload4096Bytes(b *testing.B) {
	benchmarkEncodedPayload(b, dio.NoCompression, 4096)
}

func BenchmarkAddToEntryQueue(b *testing.B) {
	b.ReportAllocs()
	q := newEntryQueue(1000000, 0)
	total := uint32(0)
	entry := pb.Entry{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := atomic.AddUint32(&total, 1)
			if t%2048 == 0 {
				atomic.StoreUint32(&total, 0)
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
	total := uint32(0)
	q := newEntryQueue(2048, 0)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	pp := newPendingProposal(cfg, false, p, q)
	session := client.NewNoOPSession(1, random.LockGuardedRand)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint32(&total, 1)
			b.SetBytes(int64(sz))
			rs, err := pp.propose(session, data, 100)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint32(&total, 0)
				q.get(false)
			}
			pp.applied(rs.key, rs.clientID, rs.seriesID, sm.Result{Value: 1}, false)
			rs.readyToRelease.set()
			rs.Release()
		}
	})
}

func BenchmarkPropose16(b *testing.B) {
	benchmarkProposeN(b, 16)
}

func BenchmarkPropose128(b *testing.B) {
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
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	pp := newPendingProposal(cfg, false, p, q)
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
	total := uint32(0)
	q := newReadIndexQueue(2048)
	pri := newPendingReadIndex(p, q)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint32(&total, 1)
			rs, err := pri.read(100)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint32(&total, 0)
				q.get()
			}
			rs.readyToRelease.set()
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
			rc.shardReady(1)
		}
	})
}

func BenchmarkReadyShard(b *testing.B) {
	b.ReportAllocs()
	rc := newReadyShard()
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.setShardReady(1)
		}
	})
}

func BenchmarkFSyncLatency(b *testing.B) {
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb", vfs.DefaultFS)
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
		ShardID:       1,
		ReplicaID:     1,
		EntriesToSave: []pb.Entry{e},
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		if err := db.SaveRaftState([]pb.Update{u}, 1); err != nil {
			b.Fatalf("%v", err)
		}
	}
}

func benchmarkSaveRaftState(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb", vfs.DefaultFS)
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	shardID := uint32(1)
	b.StartTimer()
	b.RunParallel(func(pbt *testing.PB) {
		ldb, ok := db.(*logdb.ShardedDB)
		if !ok {
			b.Fatalf("not a logdb.ShardedDB instance")
		}
		rdbctx := ldb.GetLogDBThreadContext()
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
		cid := uint64(atomic.AddUint32(&shardID, 1))
		bytes := e.Size() * 128
		u := pb.Update{
			ShardID:   cid,
			ReplicaID: 1,
		}
		iidx := e.Index
		for i := uint64(0); i < 128; i++ {
			e.Index = iidx + i
			u.EntriesToSave = append(u.EntriesToSave, e)
		}
		for pbt.Next() {
			rdbctx.Reset()
			if err := ldb.SaveRaftStateCtx([]pb.Update{u}, rdbctx); err != nil {
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

func (h *benchmarkMessageHandler) HandleMessageBatch(batch pb.MessageBatch) (uint64, uint64) {
	v := atomic.AddUint64(&h.count, uint64(len(batch.Requests)))
	if v >= h.expected {
		h.ch <- struct{}{}
	}
	return 0, 0
}

func (h *benchmarkMessageHandler) HandleUnreachable(shardID uint64, replicaID uint64) {
}

func (h *benchmarkMessageHandler) HandleSnapshotStatus(shardID uint64,
	replicaID uint64, rejected bool) {
}

func (h *benchmarkMessageHandler) HandleSnapshot(shardID uint64,
	replicaID uint64, from uint64) {
}

type dummyTransportEvent struct{}

func (d *dummyTransportEvent) ConnectionEstablished(addr string, snapshot bool) {}
func (d *dummyTransportEvent) ConnectionFailed(addr string, snapshot bool)      {}

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
		Expert: config.ExpertConfig{
			FS: vfs.DefaultFS,
		},
	}
	env1, err := server.NewEnv(nhc1, vfs.DefaultFS)
	if err != nil {
		b.Fatalf("failed to new context %v", err)
	}
	nhc2 := config.NodeHostConfig{
		RaftAddress: addr2,
		Expert: config.ExpertConfig{
			FS: vfs.DefaultFS,
		},
	}
	env2, err := server.NewEnv(nhc2, vfs.DefaultFS)
	if err != nil {
		b.Fatalf("failed to new context %v", err)
	}
	nodes1 := registry.NewNodeRegistry(settings.Soft.StreamConnections, nil)
	nodes2 := registry.NewNodeRegistry(settings.Soft.StreamConnections, nil)
	nodes1.Add(1, 2, addr2)
	handler1 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	handler2 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	t1, err := transport.NewTransport(nhc1,
		handler1, env1, nodes1, nil, &dummyTransportEvent{}, vfs.DefaultFS)
	if err != nil {
		b.Fatalf("failed to create transport %v", err)
	}
	t2, err := transport.NewTransport(nhc2,
		handler2, env2, nodes2, nil, &dummyTransportEvent{}, vfs.DefaultFS)
	if err != nil {
		b.Fatalf("failed to create transport %v", err)
	}
	defer func() {
		if err := t2.Close(); err != nil {
			b.Fatalf("failed to stop the transport module %v", err)
		}
	}()
	defer func() {
		if err := t1.Close(); err != nil {
			b.Fatalf("failed to stop the transport module %v", err)
		}
	}()
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
			Type:     pb.Replicate,
			To:       2,
			From:     1,
			ShardID:  1,
			Term:     100,
			LogTerm:  100,
			LogIndex: 123456789,
			Commit:   123456789,
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
			t1.Send(msg)
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

func BenchmarkLookup(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	ds := &tests.NoOP{}
	done := make(chan struct{})
	config := config.Config{ShardID: 1, ReplicaID: 1}
	nds := rsm.NewNativeSM(config, rsm.NewInMemStateMachine(ds), done)
	input := make([]byte, 1)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		result, err := nds.Lookup(input)
		if err != nil {
			b.Fatalf("lookup failed %v", err)
		}
		if result == nil || len(result.([]byte)) != 1 {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkNALookup(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	ds := &tests.NoOP{}
	done := make(chan struct{})
	config := config.Config{ShardID: 1, ReplicaID: 1}
	nds := rsm.NewNativeSM(config, rsm.NewInMemStateMachine(ds), done)
	input := make([]byte, 1)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		result, err := nds.NALookup(input)
		if err != nil {
			b.Fatalf("lookup failed %v", err)
		}
		if len(result) != 1 {
			b.Fatalf("unexpected result")
		}
	}
}

func benchmarkStateMachineStep(b *testing.B, sz int, noopSession bool) {
	b.ReportAllocs()
	b.StopTimer()
	ds := &tests.NoOP{NoAlloc: true}
	done := make(chan struct{})
	config := config.Config{ShardID: 1, ReplicaID: 1}
	nds := rsm.NewNativeSM(config, rsm.NewInMemStateMachine(ds), done)
	smo := rsm.NewStateMachine(nds, nil, config, &testDummyNodeProxy{}, vfs.DefaultFS)
	idx := uint64(0)
	var s *client.Session
	if noopSession {
		s = client.NewNoOPSession(1, random.LockGuardedRand)
	} else {
		s = &client.Session{
			ShardID:  1,
			ClientID: 1234576,
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
	batch := make([]rsm.Task, 0, 100000)
	smEntries := make([]sm.Entry, 0)
	task := rsm.Task{Recover: false}
	if !noopSession {
		idx++
		e.Index = idx
		e.SeriesID = client.SeriesIDForRegister
		entries = append(entries, e)
		task.Entries = entries
		smo.TaskQ().Add(task)
		if _, err := smo.Handle(batch, smEntries); err != nil {
			b.Fatalf("handle failed %v", err)
		}
	}
	b.StartTimer()
	for x := 0; x < b.N; x++ {
		entries = entries[:0]
		for i := uint64(0); i < 128; i++ {
			idx++
			e.Index = idx
			entries = append(entries, e)
		}
		task.Entries = entries
		smo.TaskQ().Add(task)
		if _, err := smo.Handle(batch, smEntries); err != nil {
			b.Fatalf("handle failed %v", err)
		}
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

type noopSink struct{}

func (n *noopSink) Receive(pb.Chunk) (bool, bool) { return true, false }
func (n *noopSink) Close() error                  { return nil }
func (n *noopSink) ShardID() uint64               { return 1 }
func (n *noopSink) ToReplicaID() uint64           { return 1 }

func BenchmarkChunkWriter(b *testing.B) {
	sink := &noopSink{}
	meta := rsm.SSMeta{}
	cw := rsm.NewChunkWriter(sink, meta)
	sz := int64(1024 * 256)
	data := make([]byte, sz)
	rand.Read(data)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cw.Write(data); err != nil {
			b.Fatalf("failed to write %v", err)
		}
	}
}

func BenchmarkSnappyCompressedChunkWriter(b *testing.B) {
	sink := &noopSink{}
	meta := rsm.SSMeta{}
	cw := rsm.NewChunkWriter(sink, meta)
	w := snappy.NewBufferedWriter(cw)
	sz := int64(1024 * 256)
	data := make([]byte, sz)
	rand.Read(data)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(data); err != nil {
			b.Fatalf("failed to write %v", err)
		}
	}
}

type marshaler interface {
	Marshal() ([]byte, error)
}

func mustMarshal(m marshaler) []byte {
	result, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	return result
}

func marshalData(e pb.Entry) {
	_, err := e.Marshal()
	if err != nil {
		panic(err)
	}
}

func mustMarshalData(e pb.Entry) {
	mustMarshal(&e)
}

func BenchmarkMarshal(b *testing.B) {
	b.ReportAllocs()
	e := pb.Entry{}
	for i := 0; i < b.N; i++ {
		marshalData(e)
	}
}

func BenchmarkMustMarshal(b *testing.B) {
	b.ReportAllocs()
	e := pb.Entry{}
	for i := 0; i < b.N; i++ {
		mustMarshalData(e)
	}
}
