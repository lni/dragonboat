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
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/pkg/errors"
	"testing"
)

func TestInitializePromotionManager(t *testing.T) {
	m := NewPromotionManager(1000, nil, nil, nil)

	if m.readIndexTimeoutMs != 1000 {
		t.Errorf("read index timeout not 1000 milliseconds: %v", m.readIndexTimeoutMs)
	}

	if m.configChangeTimeoutMs != 5000 {
		t.Errorf("config change timeout not 5000 milliseconds: %v", m.configChangeTimeoutMs)
	}

	if minInsyncIterations != 5 {
		t.Errorf("minInsyncIterations not 5: %v", minInsyncIterations)
	}

	verifyIterationCounter(t, m, 0)

	if m.promoC != nil {
		t.Errorf("promoC not nil: %v", m)
	}

	if m.readIndexC != nil {
		t.Errorf("readIndexC not nil: %v", m)
	}

	verifyBackOffCounter(t, m, 0)
	verifyState(t, m, Started)
}

func verifyBackOffCounter(t *testing.T, m *PromotionManager, expected uint64) {
	if m.backOffCounter != expected {
		t.Errorf("backOffCounter not %v: actually get %v", expected, m.backOffCounter)
	}
}

func TestStateTransition(t *testing.T) {
	m := NewPromotionManager(10, nil, nil, nil)

	tests := []struct {
		from        state
		to          state
		shouldPanic bool
	}{
		// No state could be transit to Started
		{BackOff, Started, true},
		{PendingReadIndex, Started, true},
		{PendingPromotion, Started, true},
		// Done could not transit to any state
		{Done, Started, true},
		{Done, PendingReadIndex, true},
		{Done, PendingPromotion, true},
		{Done, BackOff, true},
		// Pending states could go to BackOff
		{PendingReadIndex, BackOff, false},
		{PendingPromotion, BackOff, false},
		// BackOff state could only go to PendingIndex
		{BackOff, PendingReadIndex, false},
		{BackOff, PendingPromotion, true},
		// Started could only go to PendingReadIndex
		{Started, PendingReadIndex, false},
		{Started, PendingPromotion, true},
		{Started, BackOff, true},
	}
	for idx, tt := range tests {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					if tt.shouldPanic {
						t.Errorf("%d, failed to panic", idx)
					}
				}
			}()
			m.pmState = tt.from
			m.transitTo(tt.to)
		}()
	}
}

func TestIncrementBackOff(t *testing.T) {
	m := NewPromotionManager(10, nil, nil, nil)
	tests := []struct {
		initial  uint64
		expected uint64
	}{
		{0, 1},
		{4, 8},
		{50, 64},
		{128, 64},
	}

	for _, tt := range tests {
		m.backOffCounter = tt.initial
		m.incrementBackOff()
		verifyBackOffCounter(t, m, tt.expected)
	}
}

func TestReset(t *testing.T) {
	m := NewPromotionManager(10, nil, nil, nil)
	m.transitTo(PendingReadIndex)
	m.backOffCounter = 4
	m.reset()

	verifyBackOffCounter(t, m, 8)
	verifyIterationCounter(t, m, 0)
	verifyState(t, m, BackOff)
}

func TestPerformReadIndex(t *testing.T) {
	readIndexC := make(chan RequestResult)
	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		return &RequestState{
			CompletedC: readIndexC,
		}, nil
	}, nil, nil)
	m.maybePerformReadIndex()

	verifyState(t, m, PendingReadIndex)

	if m.readIndexC != readIndexC {
		t.Errorf("ReadIndex channel not setup")
	}

	m.backOffCounter = 1
	m.transitTo(BackOff)

	m.maybePerformReadIndex()

	verifyIterationCounter(t, m, 0)
	verifyState(t, m, BackOff)
}

func TestReadIndexInnerFail(t *testing.T) {
	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		return &RequestState{}, errors.Errorf("Unknown error")
	}, nil, nil)
	m.maybePerformReadIndex()

	if m.readIndexC != nil {
		t.Errorf("readIndex channel should be nil")
	}

	verifyBackOffCounter(t, m, 1)
	verifyIterationCounter(t, m, 0)
	verifyState(t, m, Started)
}

func TestProcessReadIndexResultSuccess(t *testing.T) {
	promoC := make(chan RequestResult)

	m := NewPromotionManager(10, nil, func(configChangeTimeout uint64) (requestState *RequestState, e error) {
		return &RequestState{
			CompletedC: promoC,
		}, nil
	}, nil)
	m.transitTo(PendingReadIndex)

	m.insyncIterationCounter = 5
	m.processReadIndexResult(RequestResult{
		code: requestCompleted,
	})

	if m.promoC != promoC {
		t.Errorf("Promotion not started")
	}

	verifyState(t, m, PendingPromotion)
}

func TestProcessReadIndexResultNotReady(t *testing.T) {
	promoC := make(chan RequestResult)

	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		return &RequestState{}, nil
	}, func(configChangeTimeout uint64) (requestState *RequestState, e error) {
		return &RequestState{
			CompletedC: promoC,
		}, nil
	}, nil)

	m.insyncIterationCounter = 0
	m.transitTo(PendingReadIndex)
	m.processReadIndexResult(RequestResult{
		code: requestCompleted,
	})

	if m.promoC != nil {
		t.Errorf("PromoC not nil")
	}

	verifyIterationCounter(t, m, 1)

	verifyState(t, m, PendingReadIndex)
}

func TestProcessReadIndexResultFail(t *testing.T) {
	promoC := make(chan RequestResult)

	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		return &RequestState{}, nil
	}, func(configChangeTimeout uint64) (requestState *RequestState, e error) {
		return &RequestState{
			CompletedC: promoC,
		}, nil
	}, nil)

	m.insyncIterationCounter = 0
	m.transitTo(PendingReadIndex)
	m.processReadIndexResult(RequestResult{
		code: requestTerminated,
	})

	verifyBackOffCounter(t, m, 1)
	verifyIterationCounter(t, m, 0)
	verifyState(t, m, BackOff)
}

func TestProcessReadIndexResultPromoFail(t *testing.T) {
	promoC := make(chan RequestResult)

	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		return &RequestState{}, nil
	}, func(configChangeTimeout uint64) (requestState *RequestState, e error) {
		return &RequestState{
			CompletedC: promoC,
		}, errors.Errorf("expected error")
	}, nil)

	m.insyncIterationCounter = 5
	m.transitTo(PendingReadIndex)
	m.processReadIndexResult(RequestResult{
		code: requestCompleted,
	})

	verifyBackOffCounter(t, m, 1)
	verifyIterationCounter(t, m, 0)
	verifyState(t, m, BackOff)
}

func verifyIterationCounter(t *testing.T, m *PromotionManager, expected uint64) {
	if m.insyncIterationCounter != expected {
		t.Errorf("insyncIterationCounter not %v: actually got %v", expected, m.insyncIterationCounter)
	}
}

func verifyState(t *testing.T, m *PromotionManager, expectedState state) {
	if m.pmState != expectedState {
		t.Errorf("state not changing to %v, actually get %v", expectedState, m.pmState)
	}
}

func TestProcessPromotionSuccess(t *testing.T) {
	m := NewPromotionManager(10, nil, nil, nil)

	m.transitTo(PendingReadIndex)
	m.transitTo(PendingPromotion)
	m.processPromotionResult(RequestResult{
		code: requestCompleted,
	})

	verifyState(t, m, Done)
}

func TestProcessPromotionFail(t *testing.T) {
	m := NewPromotionManager(10, nil, nil, nil)
	m.insyncIterationCounter = 3

	m.transitTo(PendingReadIndex)
	m.transitTo(PendingPromotion)
	m.processPromotionResult(RequestResult{
		code: requestTimeout,
	})

	verifyIterationCounter(t, m, 0)

	verifyState(t, m, BackOff)
}

func TestFullProcess(t *testing.T) {
	readIndexCounter := uint64(0)
	readIndexC := make(chan RequestResult, 1)
	promoC := make(chan RequestResult, 1)
	promoC <- RequestResult{
		code: requestCompleted,
	}

	promoCt := uint64(0)
	m := NewPromotionManager(10, func(timeout uint64) (requestState *RequestState, e error) {
		readIndexCounter += 1
		return &RequestState{
			CompletedC: readIndexC,
		}, nil
	}, func(configChangeTimeout uint64) (requestState *RequestState, e error) {
		promoCt += 1
		return &RequestState{
			CompletedC: promoC,
		}, nil
	}, func(result statemachine.Result) {})

	tests := []struct {
		expectedState       state
		expectedReadIndexCt uint64
		expectedPromoCt     uint64
		shouldPushReadIndex bool
	}{
		{PendingReadIndex, 1, 0, true},
		{PendingReadIndex, 2, 0, true},
		{PendingReadIndex, 3, 0, true},
		{BackOff, 3, 0, false},
		{BackOff, 3, 0, false},
		{PendingReadIndex, 4, 0, true},
		{PendingReadIndex, 5, 0, true},
		{PendingReadIndex, 6, 0, true},
		{PendingReadIndex, 7, 0, true},
		{PendingReadIndex, 8, 0, true},
		{PendingPromotion, 8, 1, false},
		{Done, 8, 1, false},
		{Done, 8, 1, false},
		{Done, 8, 1, false},
	}

	injectFailureIdx := 2

	for idx, tt := range tests {
		m.Process()
		verifyState(t, m, tt.expectedState)
		if readIndexCounter != tt.expectedReadIndexCt {
			t.Errorf("test failed at step %v for mismatching read index ct: expected %v, actual %v",
				idx, tt.expectedReadIndexCt, readIndexCounter)
		}

		if promoCt != tt.expectedPromoCt {
			t.Errorf("test failed at step %v for mismatching promo ct: expected %v, actual %v",
				idx, tt.expectedPromoCt, promoCt)
		}

		if tt.shouldPushReadIndex {
			if idx == injectFailureIdx {
				readIndexC <- RequestResult{
					code: requestTimeout,
				}
			} else {
				readIndexC <- RequestResult{
					code: requestCompleted,
				}
			}
		}
	}
}
