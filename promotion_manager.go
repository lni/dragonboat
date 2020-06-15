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
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/settings"
)

type state string

const (
	Started          state = "Started"
	PendingReadIndex state = "PendingReadIndex"
	PendingPromotion state = "PendingPromotion"
	BackOff          state = "BackOff"
	Done             state = "Done"
)

var (
	maxBackOff             = settings.Soft.PromotionReadIndexBackOff
	pConfigChangeTimeoutMs = settings.Soft.PromotionConfigChangeTimeoutMs
	minInsyncIterations    = settings.Soft.MinInsyncIterations
	validTransitions       map[state][]state
)

// PromotionManager tracks and conducts auto promotion for observer nodes, so that user
// no longer needs to do RPC call when the node is ready to be add into the quorum.
// The workflow is as follow: Perform a couple rounds of readIndex and make sure they don't timeout
// within election timeout. Then perform the config change from observer to require a promotion to follower.
// Note that within the entire process if any of the steps fails, we will reset the counter, back-off a bit,
// and do the whole thing again.
type PromotionManager struct {
	readIndexTimeoutMs     uint64
	configChangeTimeoutMs  uint64
	insyncIterationCounter uint64
	minInsyncIterations    uint64
	readIndex              func(readIndexTimeout uint64) (*RequestState, error)
	promote                func(configChangeTimeout uint64) (*RequestState, error)
	readIndexC             chan RequestResult
	promoC                 chan RequestResult
	pmState                state
	backOffCounter         uint64
	doneCb                 config.AutoPromoteCallbackFunc
}

func NewPromotionManager(readIndexTimeoutMs uint64,
	readIndex func(readIndexTimeout uint64) (*RequestState, error),
	promote func(configChangeTimeout uint64) (*RequestState, error),
	doneCallback config.AutoPromoteCallbackFunc) *PromotionManager {
	validTransitions = map[state][]state{
		Started:          {PendingReadIndex},
		BackOff:          {PendingReadIndex},
		PendingReadIndex: {PendingPromotion, BackOff},
		PendingPromotion: {Done, BackOff},
	}
	return &PromotionManager{
		readIndexTimeoutMs:     readIndexTimeoutMs,
		configChangeTimeoutMs:  pConfigChangeTimeoutMs,
		insyncIterationCounter: 0,
		minInsyncIterations:    minInsyncIterations,
		readIndex:              readIndex,
		promote:                promote,
		promoC:                 nil,
		pmState:                Started,
		doneCb:                 doneCallback,
	}
}

func (m *PromotionManager) transitTo(newState state) {
	isValidTransition := false
	for _, validState := range validTransitions[m.pmState] {
		if validState == newState {
			isValidTransition = true
			break
		}
	}
	if !isValidTransition {
		plog.Panicf("Invalid transition from state %v to state %v", m.pmState, newState)
	}
	plog.Infof("Promotion manager is transiting from state %v to state %v", m.pmState, newState)
	m.pmState = newState
}

// Process call triggers on every iteration when exec engine ticks, check
// the promotion request result if there is an ongoing configuration
// change and sends out readIndex request if needed.
func (m *PromotionManager) Process() {
	switch m.pmState {
	case Started:
		m.maybePerformReadIndex()
	case BackOff:
		m.maybePerformReadIndex()
	case PendingReadIndex:
		select {
		case r := <-m.readIndexC:
			m.processReadIndexResult(r)
		default:
		}
	case PendingPromotion:
		select {
		case r := <-m.promoC:
			m.processPromotionResult(r)
			m.doneCb(r.result)
		default:
		}
	case Done:
	default:
		plog.Panicf("Unknown state %v", m.pmState)
	}

	plog.Debugf("Current state: %v\n"+
		"in sync iteration count: %v \n"+
		"min in sync iteration: %v \n"+
		"back off counter: %v",
		m.pmState,
		m.insyncIterationCounter,
		m.minInsyncIterations,
		m.backOffCounter)
}

func (m *PromotionManager) maybePerformReadIndex() {
	if m.backOffCounter > 0 {
		m.backOffCounter -= 1
	} else {
		r, err := m.readIndex(m.readIndexTimeoutMs)
		if err != nil {
			plog.Errorf("Promotion manager reads index failed: %v", err)
			m.incrementBackOff()
		} else {
			m.readIndexC = r.CompletedC
			m.transitTo(PendingReadIndex)
		}
	}
}

// A callback for the readIndex request Process. If consecutive successful readIndex requests are collected,
// trigger the actual promotion. If any read index request times out or is too slow,
// we will reset counter and do it again.
func (m *PromotionManager) processReadIndexResult(result RequestResult) {
	if result.Completed() {
		m.insyncIterationCounter += 1
		if m.insyncIterationCounter >= m.minInsyncIterations {
			rs, err := m.promote(m.configChangeTimeoutMs)
			if err != nil {
				plog.Errorf("The config change request for observer promotion has failed: %v. Will reset", err)
				m.reset()
			} else {
				m.promoC = rs.CompletedC
				m.transitTo(PendingPromotion)
			}
		} else {
			m.transitTo(BackOff)
			m.maybePerformReadIndex()
		}
	} else {
		plog.Infof("Promotion failed, will reset and retry.")
		m.reset()
	}
}

func (m *PromotionManager) processPromotionResult(r RequestResult) {
	if r.Completed() {
		m.transitTo(Done)
		plog.Infof("The promotion is complete, deactivating the promotion tracker.")
	} else {
		m.insyncIterationCounter = 0
		m.incrementBackOff()
		m.transitTo(BackOff)
		plog.Warningf("Last promotion was not successful as: %v, resetting the counter.", r)
	}
}

func (m *PromotionManager) reset() {
	m.insyncIterationCounter = 0
	m.incrementBackOff()
	m.transitTo(BackOff)
}

func (m *PromotionManager) incrementBackOff() {
	m.backOffCounter *= 2
	if m.backOffCounter > maxBackOff {
		m.backOffCounter = maxBackOff
	} else if m.backOffCounter == 0 {
		m.backOffCounter = 1
	}
	return
}
