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

package dragonboat

import (
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type quiesceManager struct {
	clusterID           uint64
	nodeID              uint64
	tick                uint64
	electionTick        uint64
	quiescedSince       uint64
	noActivitySince     uint64
	exitQuiesceTick     uint64
	enabled             bool
	newQuiesceStateFlag uint32
}

func (q *quiesceManager) setNewQuiesceStateFlag() {
	atomic.StoreUint32(&q.newQuiesceStateFlag, 1)
}

func (q *quiesceManager) newQuiesceState() bool {
	return atomic.SwapUint32(&q.newQuiesceStateFlag, 0) == 1
}

func (q *quiesceManager) increaseQuiesceTick() uint64 {
	if !q.enabled {
		return 0
	}
	threshold := q.quiesceThreshold()
	q.tick++
	if !q.quiesced() {
		if (q.tick - q.noActivitySince) > threshold {
			q.enterQuiesce()
		}
	}
	return q.tick
}

func (q *quiesceManager) quiesced() bool {
	if !q.enabled {
		return false
	}
	return q.quiescedSince > 0
}

func (q *quiesceManager) recordActivity(msgType pb.MessageType) {
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
	q.noActivitySince = q.tick
	if q.quiesced() {
		q.exitQuiesce()
		plog.Infof("%s exited from quiesce, msg type %s, current tick %d",
			logutil.DescribeNode(q.clusterID, q.nodeID), msgType, q.tick)
	}
}

func (q *quiesceManager) quiesceThreshold() uint64 {
	return q.electionTick * 10
}

func (q *quiesceManager) newToQuiesce() bool {
	if !q.quiesced() {
		return false
	}
	return q.tick-q.quiescedSince < q.electionTick
}

func (q *quiesceManager) justExitedQuiesce() bool {
	if q.quiesced() {
		return false
	}
	return q.tick-q.exitQuiesceTick < q.quiesceThreshold()
}

func (q *quiesceManager) tryEnterQuiesce() {
	if q.justExitedQuiesce() {
		return
	}
	if !q.quiesced() {
		plog.Infof("%s going to enter quiesce due to quiesce message",
			logutil.DescribeNode(q.clusterID, q.nodeID))
		q.enterQuiesce()
	}
}

func (q *quiesceManager) enterQuiesce() {
	q.quiescedSince = q.tick
	q.noActivitySince = q.tick
	q.setNewQuiesceStateFlag()
	plog.Infof("%s entered quiesce",
		logutil.DescribeNode(q.clusterID, q.nodeID))
}

func (q *quiesceManager) exitQuiesce() {
	q.quiescedSince = 0
	q.exitQuiesceTick = q.tick
}
