// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package transport

import (
	"github.com/VictoriaMetrics/metrics"
)

type transportMetrics struct {
	snapshotSent       *metrics.Counter
	messageConns       *metrics.Gauge
	snapshotConns      *metrics.Gauge
	messageDropped     *metrics.Counter
	messageSent        *metrics.Counter
	snapshotDropped    *metrics.Counter
	messageConnFailed  *metrics.Counter
	snapshotConnFailed *metrics.Counter
	messageReceived    *metrics.Counter
	messageRecvDropped *metrics.Counter
	snapshotReceived   *metrics.Counter
	useMetrics         bool
}

func newTransportMetrics(useMetrics bool,
	msgCount func() float64, ssCount func() float64) *transportMetrics {
	tm := &transportMetrics{useMetrics: useMetrics}
	if useMetrics {
		name := "dragonboat_transport_message_connections"
		tm.messageConns = metrics.GetOrCreateGauge(name, msgCount)
		name = "dragonboat_transport_snapshot_connections"
		tm.snapshotConns = metrics.GetOrCreateGauge(name, ssCount)
		name = "dragonboat_transport_message_send_failure_total"
		tm.messageDropped = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_message_send_success_total"
		tm.messageSent = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_snapshot_send_failure_total"
		tm.snapshotDropped = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_snapshot_send_success_total"
		tm.snapshotSent = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_received_message_total"
		tm.messageReceived = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_received_message_dropped_total"
		tm.messageRecvDropped = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_received_snapshot_total"
		tm.snapshotReceived = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_failed_message_connection_attempt_total"
		tm.messageConnFailed = metrics.GetOrCreateCounter(name)
		name = "dragonboat_transport_failed_snapshot_connection_attempt_total"
		tm.snapshotConnFailed = metrics.GetOrCreateCounter(name)
	}
	return tm
}

func (tm *transportMetrics) messageConnectionFailure() {
	if tm.useMetrics {
		tm.messageConnFailed.Add(1)
	}
}

func (tm *transportMetrics) snapshotCnnectionFailure() {
	if tm.useMetrics {
		tm.snapshotConnFailed.Add(1)
	}
}

func (tm *transportMetrics) receivedMessages(ss uint64,
	msg uint64, dropped uint64) {
	if tm.useMetrics {
		tm.messageReceived.Add(int(msg))
		tm.messageRecvDropped.Add(int(dropped))
		tm.snapshotReceived.Add(int(ss))
	}
}

func (tm *transportMetrics) messageSendSuccess(count uint64) {
	if tm.useMetrics {
		tm.messageSent.Add(int(count))
	}
}

func (tm *transportMetrics) messageSendFailure(count uint64) {
	if tm.useMetrics {
		tm.messageDropped.Add(int(count))
	}
}

func (tm *transportMetrics) snapshotSendSuccess() {
	if tm.useMetrics {
		tm.snapshotSent.Add(1)
	}
}

func (tm *transportMetrics) snapshotSendFailure() {
	if tm.useMetrics {
		tm.snapshotDropped.Add(1)
	}
}
