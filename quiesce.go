// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"sync/atomic"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

type quiesceState struct {
	shardID             uint64
	replicaID           uint64
	currentTick         uint64
	electionTick        uint64
	quiescedSince       uint64
	idleSince           uint64
	exitQuiesceTick     uint64
	enabled             bool
	newQuiesceStateFlag uint32
}

func (q *quiesceState) setNewQuiesceStateFlag() {
	atomic.StoreUint32(&q.newQuiesceStateFlag, 1)
}

func (q *quiesceState) newQuiesceState() bool {
	return atomic.SwapUint32(&q.newQuiesceStateFlag, 0) == 1
}

func (q *quiesceState) tick() uint64 {
	if !q.enabled {
		return 0
	}
	threshold := q.threshold()
	q.currentTick++
	if !q.quiesced() {
		if (q.currentTick - q.idleSince) > threshold {
			q.enterQuiesce()
		}
	}
	return q.currentTick
}

func (q *quiesceState) quiesced() bool {
	return q.enabled && q.quiescedSince > 0
}

func (q *quiesceState) record(msgType pb.MessageType) {
	if !q.enabled {
		return
	}
	if msgType == pb.Heartbeat || msgType == pb.HeartbeatResp {
		if !q.quiesced() {
			return
		}
		if q.newToQuiesce() {
			return
		}
	}
	q.idleSince = q.currentTick
	if q.quiesced() {
		q.exitQuiesce()
		plog.Infof("%s exited from quiesce, msg type %s, current tick %d",
			dn(q.shardID, q.replicaID), msgType, q.currentTick)
	}
}

func (q *quiesceState) threshold() uint64 {
	return q.electionTick * 10
}

func (q *quiesceState) newToQuiesce() bool {
	if !q.quiesced() {
		return false
	}
	return q.currentTick-q.quiescedSince < q.electionTick
}

func (q *quiesceState) justExitedQuiesce() bool {
	if q.quiesced() {
		return false
	}
	return q.currentTick-q.exitQuiesceTick < q.threshold()
}

func (q *quiesceState) tryEnterQuiesce() {
	if q.justExitedQuiesce() {
		return
	}
	if !q.quiesced() {
		plog.Infof("%s going to enter quiesce due to quiesce message",
			dn(q.shardID, q.replicaID))
		q.enterQuiesce()
	}
}

func (q *quiesceState) enterQuiesce() {
	q.quiescedSince = q.currentTick
	q.idleSince = q.currentTick
	q.setNewQuiesceStateFlag()
	plog.Infof("%s entered quiesce", dn(q.shardID, q.replicaID))
}

func (q *quiesceState) exitQuiesce() {
	q.quiescedSince = 0
	q.exitQuiesceTick = q.currentTick
}
