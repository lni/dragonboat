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

package lcm

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/internal/drummer/client"
	mr "github.com/lni/dragonboat/internal/drummer/multiraftpb"
	kvpb "github.com/lni/dragonboat/internal/tests/kvpb"
)

var (
	lcmKey  = "lcm_key"
	timeout = 3 * time.Second
)

type historyRecorder interface {
	recordWriteInvoked(id uint64, value uint64)
	recordWriteFailed(id uint64)
	recordWriteCompleted(id uint64, value uint64)
	recordReadInvoked(id uint64)
	recordReadFailed(id uint64)
	recordReadCompleted(id uint64, value uint64)
}

type process struct {
	id          uint64
	stoppedFlag uint32
	idleFlag    uint32
	recorder    historyRecorder
	ctx         context.Context
	pool        *client.Pool
}

func newProcess(ctx context.Context,
	id uint64, recorder historyRecorder) *process {
	p := &process{
		id:       id,
		idleFlag: 1,
		recorder: recorder,
		ctx:      ctx,
		pool:     client.NewConnectionPool(),
	}

	return p
}

func (p *process) stop() {
	if !p.isStopped() {
		p.setStopped()
	}
}

func (p *process) setStopped() {
	atomic.StoreUint32(&p.stoppedFlag, 1)
	p.pool.Close()
}

func (p *process) isStopped() bool {
	return atomic.LoadUint32(&p.stoppedFlag) == 1
}

func (p *process) setIdle() {
	atomic.StoreUint32(&p.idleFlag, 1)
}

func (p *process) setBusy() {
	atomic.StoreUint32(&p.idleFlag, 0)
}

func (p *process) isIdle() bool {
	return atomic.LoadUint32(&p.idleFlag) == 1
}

func (p *process) getValue(val uint64) string {
	return fmt.Sprintf("%d", val)
}

func (p *process) getUint64Value(val string) uint64 {
	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		panic(err)
	}

	return v
}

func (p *process) recordWriteFailed() {
	p.recorder.recordWriteFailed(p.id)
}

func (p *process) recordWriteCompleted(value uint64) {
	p.recorder.recordWriteCompleted(p.id, value)
}

func (p *process) recordReadFailed() {
	p.recorder.recordReadFailed(p.id)
}

func (p *process) recordReadCompleted(value uint64) {
	p.recorder.recordReadCompleted(p.id, value)
}

func (p *process) StartWrite(addr string, clusterID uint64, value uint64) {
	p.setBusy()
	go func() {
		defer p.setIdle()
		err := p.write(addr, clusterID, value)
		if err != nil {
			p.setStopped()
			p.recordWriteFailed()
		} else {
			p.recordWriteCompleted(value)
		}
	}()
}

func (p *process) StartRead(addr string, clusterID uint64) {
	p.setBusy()
	go func() {
		defer p.setIdle()
		value, err := p.read(addr, clusterID)
		if err != nil {
			p.setStopped()
			p.recordReadFailed()
		} else {
			p.recordReadCompleted(value)
		}
	}()
}

func (p *process) write(addr string, clusterID uint64, value uint64) error {
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	writeConn, err := p.pool.GetInsecureConnection(ctx, addr)
	if err != nil {
		return err
	}
	writeClient := mr.NewNodehostAPIClient(writeConn.ClientConn())
	req := &mr.SessionRequest{ClusterId: clusterID}
	cs, err := writeClient.GetSession(ctx, req)
	if err != nil {
		writeConn.Close()
		return err
	}
	v := p.getValue(value)
	kv := &kvpb.PBKV{
		Key: lcmKey,
		Val: v,
	}
	data, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	raftProposal := &mr.RaftProposal{
		Session: *cs,
		Data:    data,
	}
	ctx, cancel = context.WithTimeout(p.ctx, timeout)
	defer cancel()
	_, err = writeClient.Propose(ctx, raftProposal)
	if err != nil {
		writeConn.Close()
		return err
	}

	return nil
}

func (p *process) read(addr string, clusterID uint64) (uint64, error) {
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	readConn, err := p.pool.GetInsecureConnection(ctx, addr)
	if err != nil {
		return 0, err
	}
	readClient := mr.NewNodehostAPIClient(readConn.ClientConn())
	ri := &mr.RaftReadIndex{
		ClusterId: clusterID,
		Data:      []byte(lcmKey),
	}
	ctx, cancel = context.WithTimeout(p.ctx, timeout)
	defer cancel()
	resp, err := readClient.Read(ctx, ri)
	if err != nil {
		readConn.Close()
		return 0, err
	}
	if len(resp.Data) == 0 {
		return math.MaxUint64, nil
	}

	return p.getUint64Value(string(resp.Data)), nil
}
