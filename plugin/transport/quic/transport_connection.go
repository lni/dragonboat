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
	"hash/crc32"
	"time"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/lucas-clemente/quic-go"
)

type quicConnWriter struct {
	header    []byte
	payload   []byte
	encrypted bool
	str       quic.Stream
}

func NewMessageConnection(str quic.Stream, encrypted bool) raftio.IConnection {
	return &quicMessageConnection{
		quicConnWriter: &quicConnWriter{
			str:       str,
			header:    make([]byte, requestHeaderSize),
			payload:   make([]byte, perConnBufSize),
			encrypted: encrypted,
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
	_ = q.str.Close()
}

func NewSnapshotConnection(str quic.Stream, encrypted bool) raftio.ISnapshotConnection {
	return &quicSnapshotConnection{
		quicConnWriter: &quicConnWriter{
			str:       str,
			header:    make([]byte, requestHeaderSize),
			payload:   make([]byte, perConnBufSize),
			encrypted: encrypted,
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
	defer func() {
		_ = q.str.Close()
	}()
	if err := sendPoison(q.str, poisonNumber[:]); err != nil {
		return
	}
	waitPoisonAck(q.str)
}

func (q *quicConnWriter) writeMessage(header requestHeader, buf []byte) error {
	header.size = uint64(len(buf))
	if !q.encrypted {
		header.crc = crc32.ChecksumIEEE(buf)
	}
	q.header = header.encode(q.header)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := q.str.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := q.str.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := q.str.Write(q.header); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := q.str.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := q.str.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}
	return nil
}
