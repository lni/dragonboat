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

/*
Package lcm is a linearizable checker manager.
*/
package lcm

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/lni/dragonboat/internal/drummer/client"
	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/utils/syncutil"
)

const (
	eventInvoked uint64 = iota
	eventCompleted
	eventFailed
)

const (
	eventTypeRead uint64 = iota
	eventTypeWrite
)

type event struct {
	eventType   uint64
	eventResult uint64
	id          uint64
	value       uint64
}

func (e *event) stringValue() string {
	if e.value == math.MaxUint64 {
		return "nil"
	}

	return fmt.Sprintf("%d", e.value)
}

func (e *event) toEDNLogEntry() string {
	var t string
	var f string
	var v string
	if e.eventType == eventTypeRead {
		f = "read"
	} else {
		f = "write"
	}
	if e.eventResult == eventInvoked {
		t = "invoke"
	} else if e.eventResult == eventFailed {
		if e.eventType == eventTypeWrite {
			t = "info"
		} else {
			t = "fail"
		}
	} else if e.eventResult == eventCompleted {
		t = "ok"
	} else {
		panic("unknown event result")
	}
	if e.eventType == eventTypeRead {
		if e.eventResult == eventInvoked {
			v = "nil"
		} else if e.eventResult == eventFailed {
			v = "nil"
		} else if e.eventResult == eventCompleted {
			v = e.stringValue()
		} else {
			panic("unknown event result")
		}
	} else if e.eventType == eventTypeWrite {
		if e.eventResult == eventInvoked {
			v = e.stringValue()
		} else if e.eventResult == eventFailed {
			v = e.stringValue()
		} else if e.eventResult == eventCompleted {
			v = e.stringValue()
		} else {
			panic("unknown event result")
		}
	} else {
		panic("unknown eventResult")
	}

	return fmt.Sprintf("{:process %d, :type :%s, :f :%s, :value %s}",
		e.id, t, f, v)
}

func (e *event) toJepsenLogEntry() string {
	var t string
	var r string
	if e.eventType == eventTypeRead {
		t = ":read"
	} else {
		t = ":write"
	}
	if e.eventResult == eventInvoked {
		r = ":invoke"
	} else if e.eventResult == eventCompleted {
		r = ":ok"
	} else if e.eventResult == eventFailed {
		if e.eventType == eventTypeRead {
			r = ":fail"
		} else {
			r = ":info"
		}
	} else {
		panic("unknown eventResult")
	}
	var v string
	if e.eventType == eventTypeRead {
		if e.eventResult == eventInvoked {
			v = "nil"
		} else if e.eventResult == eventFailed {
			v = ":timed-out"
		} else if e.eventResult == eventCompleted {
			v = e.stringValue()
		} else {
			panic("unknown event result")
		}
	} else if e.eventType == eventTypeWrite {
		if e.eventResult == eventInvoked {
			v = e.stringValue()
		} else if e.eventResult == eventFailed {
			v = ":timed-out"
		} else if e.eventResult == eventCompleted {
			v = e.stringValue()
		} else {
			panic("unknown event result")
		}
	} else {
		panic("unknown eventResult")
	}

	return fmt.Sprintf("INFO  jepsen.util - %-4d%-8s%-8s%s\n", e.id, r, t, v)
}

const (
	initialWait uint64 = 25
	maxWait     uint64 = 100
)

// Coordinator is used to manage clients used for collecting test results.
type Coordinator struct {
	mu               sync.Mutex
	clusterID        uint64
	processes        []*process
	events           []event
	drummerAddresses []string
	value            uint64
	completeCount    uint64
	wait             uint64
	ctx              context.Context
	cancel           context.CancelFunc
	stopper          *syncutil.Stopper
	pool             *client.Pool
}

// NewCoordinator returns a new Coordinator object.
func NewCoordinator(ctx context.Context,
	size uint64, clusterID uint64, drummerAddresses []string) *Coordinator {
	cctx, cancel := context.WithCancel(ctx)
	c := &Coordinator{
		clusterID:        clusterID,
		drummerAddresses: drummerAddresses,
		processes:        make([]*process, 0),
		events:           make([]event, 0),
		ctx:              cctx,
		cancel:           cancel,
		stopper:          syncutil.NewStopper(),
		pool:             client.NewDrummerConnectionPool(),
		value:            1,
		wait:             uint64(initialWait),
	}
	for i := uint64(0); i < size; i++ {
		p := newProcess(cctx, i, c)
		c.processes = append(c.processes, p)
	}

	return c
}

// Start starts the Coordinator.
func (c *Coordinator) Start() {
	c.stopper.RunWorker(func() {
		c.mainLoop()
	})
}

// Stop stops the Coordinator
func (c *Coordinator) Stop() {
	c.cancel()
	c.stopper.Stop()
	c.pool.Close()
	for _, p := range c.processes {
		if !p.isStopped() {
			p.setStopped()
		}
	}
}

// SaveAsJepsenLog saves the collected results in Japsen log format.
func (c *Coordinator) SaveAsJepsenLog(fn string) {
	f, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.events {
		line := e.toJepsenLogEntry()
		if _, err = f.WriteString(line); err != nil {
			panic(err)
		}
	}
}

// SaveAsEDNLog saves the collected results in .edn format.
func (c *Coordinator) SaveAsEDNLog(fn string) {
	f, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	isFirst := true
	if _, err = f.WriteString("["); err != nil {
		panic(err)
	}
	for _, e := range c.events {
		if isFirst {
			isFirst = false
		} else {
			if _, err = f.WriteString("\n"); err != nil {
				panic(err)
			}
		}
		line := e.toEDNLogEntry()
		if _, err := f.WriteString(line); err != nil {
			panic(err)
		}
	}
	if _, err = f.WriteString("]"); err != nil {
		panic(err)
	}
}

func (c *Coordinator) mainLoop() {
	count := uint64(0)
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-c.stopper.ShouldStop():
			return
		case <-timer.C:
			count++
			if c.allStopped() {
				return
			}
			if count >= c.getWaitSecond() {
				c.scheduleProcesses()
				count = 0
			}
		}
	}
}

func (c *Coordinator) allStopped() bool {
	for _, p := range c.processes {
		if !p.isStopped() {
			return false
		}
	}
	return true
}

func (c *Coordinator) hasAvailableProcess() bool {
	for _, p := range c.processes {
		if p.isIdle() && !p.isStopped() {
			return true
		}
	}
	return false
}

func (c *Coordinator) scheduleProcesses() {
	writeAddr, readAddr, ok := c.getTargetAddress()
	if !ok {
		return
	}
	expectToSelect := 4
	count := 0
	for c.hasAvailableProcess() && count < expectToSelect {
		idx := rand.Int() % len(c.processes)
		p := c.processes[idx]
		if !p.isIdle() || p.isStopped() {
			continue
		}
		count++
		rw := rand.Int() % 2
		if rw == 0 {
			// read
			c.recordReadInvoked(p.id)
			p.StartRead(readAddr, c.clusterID)
		} else {
			// write
			value := c.value
			c.value = c.value + 1
			c.recordWriteInvoked(p.id, value)
			p.StartWrite(writeAddr, c.clusterID, value)
		}
	}
}

func (c *Coordinator) getTargetAddress() (string, string, bool) {
	ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
	defer cancel()
	conn, err := c.pool.GetInsecureConnection(ctx, c.drummerAddresses[0])
	if err != nil {
		return "", "", false
	}
	client := pb.NewDrummerClient(conn.ClientConn())
	req := &pb.ClusterStateRequest{ClusterIdList: []uint64{c.clusterID}}
	resp, err := client.GetClusterStates(ctx, req)
	if err != nil {
		conn.Close()
		return "", "", false
	}
	if len(resp.Collection) != 1 {
		return "", "", false
	}
	ci := resp.Collection[0]
	leaderRPCAddress := ""
	readNodeAddress := ""
	readNodeIdx := rand.Int() % len(ci.RPCAddresses)
	leaderIdx := rand.Int() % len(ci.RPCAddresses)
	nodeIDList := make([]uint64, 0)
	for nid := range ci.RPCAddresses {
		nodeIDList = append(nodeIDList, nid)
	}
	for nodeID, addr := range ci.RPCAddresses {
		if nodeID == nodeIDList[leaderIdx] {
			leaderRPCAddress = addr
		}
		if nodeID == nodeIDList[readNodeIdx] {
			readNodeAddress = addr
		}
	}
	if len(leaderRPCAddress) == 0 {
		return "", "", false
	}
	return leaderRPCAddress, readNodeAddress, true
}

func (c *Coordinator) recordWriteInvoked(id uint64, value uint64) {
	e := event{
		eventType:   eventTypeWrite,
		eventResult: eventInvoked,
		id:          id,
		value:       value,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *Coordinator) recordWriteFailed(id uint64) {
	e := event{
		eventType:   eventTypeWrite,
		eventResult: eventFailed,
		id:          id,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetCompleteCount()
	c.events = append(c.events, e)
}

func (c *Coordinator) recordWriteCompleted(id uint64, value uint64) {
	e := event{
		eventType:   eventTypeWrite,
		eventResult: eventCompleted,
		id:          id,
		value:       value,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.increaseCompleteCount()
	c.events = append(c.events, e)
}

func (c *Coordinator) recordReadInvoked(id uint64) {
	e := event{
		eventType:   eventTypeRead,
		eventResult: eventInvoked,
		id:          id,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *Coordinator) recordReadFailed(id uint64) {
	e := event{
		eventType:   eventTypeRead,
		eventResult: eventFailed,
		id:          id,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetCompleteCount()
	c.events = append(c.events, e)
}

func (c *Coordinator) recordReadCompleted(id uint64, value uint64) {
	e := event{
		eventType:   eventTypeRead,
		eventResult: eventCompleted,
		id:          id,
		value:       value,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.increaseCompleteCount()
	c.events = append(c.events, e)
}

func (c *Coordinator) increaseCompleteCount() {
	c.completeCount++
	if c.completeCount == 4 {
		c.wait = initialWait
		c.completeCount = 0
	}
}

func (c *Coordinator) resetCompleteCount() {
	c.completeCount = 0
	c.wait = c.wait * 2
	if c.wait >= maxWait {
		c.wait = maxWait
	}
}

func (c *Coordinator) getWaitSecond() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wait
}
