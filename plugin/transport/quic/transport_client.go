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
	"time"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/lucas-clemente/quic-go"
)

func (q *quicTransport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	str, err := q.openSessionTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewMessageConnection(str), nil
}

func (q *quicTransport) GetSnapshotConnection(ctx context.Context, target string) (raftio.ISnapshotConnection, error) {
	str, err := q.openSessionTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewSnapshotConnection(str), nil
}

func (q *quicTransport) openSessionTo(ctx context.Context, target string) (quic.Session, error) {
	tlsConf, err := q.nhConfig.GetClientTLSConfig(target)
	if err != nil {
		return nil, err
	}
	tlsConf = tlsConf.Clone()
	tlsConf.NextProtos = []string{TransportName}
	session, err := quic.DialAddrContext(ctx, target, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	return session, nil
}

type quicConnWriter struct {
	header  []byte
	payload []byte
	session quic.Session
}

func NewMessageConnection(session quic.Session) raftio.IConnection {
	return &quicMessageConnection{
		quicConnWriter: &quicConnWriter{
			session: session,
			header:  make([]byte, requestHeaderSize),
			payload: make([]byte, perConnBufSize),
		},
	}
}

type quicMessageConnection struct {
	*quicConnWriter
}

func (q *quicMessageConnection) SendMessageBatch(batch raftpb.MessageBatch) error {
	header := requestHeader{method: raftType}
	sz := batch.SizeUpperLimit()
	var buf []byte
	if len(q.payload) < sz {
		buf = make([]byte, sz)
	} else {
		buf = q.payload
	}
	n, err := batch.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return q.writeMessage(header, buf[:n])
}

func (q *quicMessageConnection) Close() {
	_ = q.session.CloseWithError(0, "")
}

func NewSnapshotConnection(session quic.Session) raftio.ISnapshotConnection {
	return &quicSnapshotConnection{
		quicConnWriter: &quicConnWriter{
			session: session,
			header:  make([]byte, requestHeaderSize),
			payload: make([]byte, perConnBufSize),
		},
	}
}

type quicSnapshotConnection struct {
	*quicConnWriter
}

func (q *quicSnapshotConnection) SendChunk(chunk raftpb.Chunk) error {
	header := requestHeader{method: snapshotType}
	sz := chunk.Size()
	buf := make([]byte, sz)
	n, err := chunk.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return q.writeMessage(header, buf[:n])
}

func (q *quicSnapshotConnection) Close() {
	str, err := q.session.OpenStreamSync(context.Background())
	if err != nil {
		return
	}
	defer func() {
		_ = str.Close()
	}()
	if err := sendPoison(str, poisonNumber[:]); err != nil {
		return
	}
	waitPoisonAck(q.session)
	_ = q.session.CloseWithError(0, "")
}

func (q *quicConnWriter) writeMessage(header requestHeader, buf []byte) error {
	str, err := q.session.OpenUniStreamSync(context.Background())
	if err != nil {
		return err
	}
	header.size = uint64(len(buf))
	q.header = header.encode(q.header)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := str.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := str.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := str.Write(q.header); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := str.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := str.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}
	return nil
}
