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

package quic

import (
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/goutils/stringutil"
	"github.com/lni/goutils/syncutil"
)

// TransportFactory is a prototype QUIC transport factory, use at your own risk.
type TransportFactory struct {
}

// Create a new raftio.ITransport over QUIC protocol.
func (t *TransportFactory) Create(config config.NodeHostConfig, mHandler raftio.MessageHandler, chHandler raftio.ChunkHandler) raftio.ITransport {
	return &quicTransport{
		nhConfig:       config,
		messageHandler: mHandler,
		chunkHandler:   chHandler,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
		encrypted:      config.MutualTLS,
	}
}

// Validate address.
func (t *TransportFactory) Validate(addr string) bool {
	return stringutil.IsValidAddress(addr)
}
