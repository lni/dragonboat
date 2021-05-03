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
	"context"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lucas-clemente/quic-go"
)

func (q *quicTransport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	str, err := q.openStreamTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewMessageConnection(str, q.encrypted), nil
}

func (q *quicTransport) GetSnapshotConnection(ctx context.Context, target string) (raftio.ISnapshotConnection, error) {
	str, err := q.openStreamTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewSnapshotConnection(str, q.encrypted), nil
}

func (q *quicTransport) openStreamTo(ctx context.Context, target string) (quic.Stream, error) {
	tlsConf, err := q.nhConfig.GetClientTLSConfig(target)
	if err != nil {
		return nil, err
	}
	session, err := quic.DialAddrContext(ctx, target, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	str, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return str, nil
}
