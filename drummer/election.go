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

package drummer

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/lni/dragonboat/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/utils/lang"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/internal/utils/syncutil"
)

const (
	deadLeaderMinRound      = 3
	leadershipRenewalSecond = 10
)

type nodeState uint64

const (
	stateFollower nodeState = iota
	stateLeader
)

type leaderInfo struct {
	instanceID  uint64
	tick        uint64
	staticRound uint64
}

type electionManager struct {
	mu            sync.Mutex
	instanceID    uint64
	currentLeader *leaderInfo
	state         nodeState
	drummerServer *server
	randSrc       random.Source
	stopper       *syncutil.Stopper
	cancel        context.CancelFunc
	*sessionUser
}

func newElectionManager(stopper *syncutil.Stopper,
	drummerServer *server, randSrc random.Source) *electionManager {
	e := &electionManager{
		state:         stateFollower,
		drummerServer: drummerServer,
		randSrc:       randSrc,
		stopper:       stopper,
	}
	e.instanceID = e.randSrc.Uint64()
	e.sessionUser = &sessionUser{
		nh: drummerServer.nh,
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	stopper.RunWorker(func() {
		e.workerMain(ctx)
	})
	return e
}

func (d *electionManager) stop() {
	d.cancel()
}

func (d *electionManager) isLeader() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state == stateLeader
}

func (d *electionManager) setLeaderInfo(l *leaderInfo) {
	if d.currentLeader == nil {
		d.currentLeader = l
		d.currentLeader.staticRound = 0
		return
	}
	if d.currentLeader.instanceID == l.instanceID &&
		d.currentLeader.tick < l.tick {
		d.currentLeader.tick = l.tick
		d.currentLeader.staticRound = 0
	} else if d.currentLeader.instanceID == l.instanceID &&
		d.currentLeader.tick == l.tick {
		d.currentLeader.staticRound++
	} else if d.currentLeader.instanceID != l.instanceID {
		d.currentLeader = l
		d.currentLeader.staticRound = 0
	} else {
		panic("unknown state")
	}
}

func (d *electionManager) leaderDead() bool {
	if d.currentLeader == nil {
		return false
	}
	return d.currentLeader.staticRound > deadLeaderMinRound
}

func (d *electionManager) becomeFollower(l *leaderInfo) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.currentLeader = l
	if l != nil {
		d.currentLeader.staticRound = 0
	}
	d.state = stateFollower
}

func (d *electionManager) resetFollower() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.currentLeader != nil {
		d.currentLeader.staticRound = 0
	}
	d.state = stateFollower
}

func (d *electionManager) becomeLeader() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.currentLeader = nil
	d.state = stateLeader
}

func (d *electionManager) workerMain(ctx context.Context) {
	tick := uint64(0)
	td := time.Duration(leadershipRenewalSecond) * time.Second
	tf := func() bool {
		tick++
		if d.isLeader() {
			d.leaderMain(ctx, tick)
		} else {
			d.followerMain(ctx, tick)
		}
		return false
	}
	lang.RunTicker(td, tf, ctx.Done(), d.stopper.ShouldStop())
}

func (d *electionManager) leaderMain(ctx context.Context, tick uint64) {
	// check who is the leader
	kv, err := d.drummerServer.getElectionInfo(ctx)
	if err != nil {
		d.becomeFollower(nil)
		return
	}
	// someone else became the leader
	if kv.InstanceId != d.instanceID {
		l := &leaderInfo{
			instanceID: kv.InstanceId,
			tick:       kv.Tick,
		}
		d.becomeFollower(l)
		return
	}
	if err := d.renewLeadership(ctx, tick); err != nil {
		plog.Errorf(err.Error())
	}
}

func (d *electionManager) renewLeadership(ctx context.Context,
	tick uint64) error {
	// leader is still the leader, renew tick on drummerdb so followers can
	// observe that the leader is still around.
	renewalKV := pb.KV{
		Key:        electionKey,
		Value:      electionKey,
		InstanceId: d.instanceID,
		Tick:       tick,
		Finalized:  false,
	}
	session := d.getSession(ctx)
	if session != nil {
		code, err := d.drummerServer.makeDrummerVote(ctx, session, renewalKV)
		if err != nil {
			d.resetSession(ctx)
			d.becomeFollower(nil)
			return err
		}
		d.clientProposalCompleted()
		if code != DBKVUpdated {
			d.becomeFollower(nil)
		}
		return nil
	}
	return errors.New("failed to get session")
}

func (d *electionManager) followerMain(ctx context.Context, tick uint64) {
	// who is the leader?
	kv, err := d.drummerServer.getElectionInfo(ctx)
	if err != nil {
		plog.Warningf("failed to get election info")
		d.resetFollower()
		return
	}
	// no leader at all
	if kv.InstanceId == 0 {
		plog.Infof("no leader, start to campaign")
		d.campaign(ctx, tick)
		return
	} else if kv.InstanceId == d.instanceID {
		err = d.renewLeadership(ctx, tick)
		if err != nil {
			plog.Warningf("leadership renewal failed")
			d.resetFollower()
			return
		}
		plog.Infof("node %d become leader", kv.InstanceId)
		d.becomeLeader()
		return
	}
	l := &leaderInfo{
		instanceID: kv.InstanceId,
		tick:       kv.Tick,
	}
	d.setLeaderInfo(l)
	if d.leaderDead() {
		plog.Infof("leader is dead, start to campaign")
		d.campaign(ctx, tick)
		return
	}
}

func (d *electionManager) campaign(ctx context.Context, tick uint64) {
	if d.state != stateFollower {
		panic("campaign requested by leader")
	}
	oldInstanceID := uint64(0)
	if d.currentLeader != nil {
		oldInstanceID = d.currentLeader.instanceID
	}
	renewalKV := pb.KV{
		Key:           electionKey,
		Value:         electionKey,
		InstanceId:    d.instanceID,
		OldInstanceId: oldInstanceID,
		Tick:          tick,
		Finalized:     false,
	}
	session := d.getSession(ctx)
	if session == nil {
		return
	}
	code, err := d.drummerServer.makeDrummerVote(ctx, session, renewalKV)
	if err != nil || code != DBKVUpdated {
		d.resetSession(ctx)
		d.resetFollower()
		return
	}
	d.clientProposalCompleted()
	kv, err := d.drummerServer.getElectionInfo(ctx)
	if err != nil {
		d.resetFollower()
		return
	}
	if kv.InstanceId == d.instanceID {
		d.becomeLeader()
		return
	}
}
