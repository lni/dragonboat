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

package server

import (
	"time"
)

// TickerFunc function that will be called by RunTicker after each tick. The
// returned boolean value indicates whether the ticker should stop.
type TickerFunc func() bool

// StartTicker runs a ticker at the specified interval, the provided TickerFunc
// tf will be called after each tick. The ticker will be stopped when tf returns
// a true value or when the stopc channel is signalled.
func StartTicker(td time.Duration, tf TickerFunc, stopc <-chan struct{}) {
	if td.Milliseconds() == 0 {
		panic("invalid duration")
	}
	ticker := time.NewTicker(td)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if tf() {
				return
			}
		case <-stopc:
			return
		}
	}
}
