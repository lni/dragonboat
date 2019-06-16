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

// +build gofuzz

package transport

import (
	"bytes"
	"net"
	"time"
)

type fuzzROConn struct {
	buf *bytes.Buffer
}

func newFuzzROConn(data []byte) *fuzzROConn {
	conn := &fuzzROConn{
		buf: bytes.NewBuffer(data),
	}
	return conn
}

func (f *fuzzROConn) Read(b []byte) (n int, err error) {
	n, err = f.buf.Read(b)
	return n, err
}

func (f *fuzzROConn) Write(b []byte) (n int, err error) {
	panic("not suppose to be called")
}

func (f *fuzzROConn) Close() error {
	panic("not suppose to be called")
}

func (f *fuzzROConn) LocalAddr() net.Addr {
	panic("not suppose to be called")
}

func (f *fuzzROConn) RemoteAddr() net.Addr {
	panic("not suppose to be called")
}

func (f *fuzzROConn) SetDeadline(t time.Time) error {
	panic("not suppose to be called")
}

func (f *fuzzROConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (f *fuzzROConn) SetWriteDeadline(t time.Time) error {
	panic("not suppose to be called")
}

func Fuzz(data []byte) int {
	roconn := newFuzzROConn(data)
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	if _, _, err := readMessage(roconn, header, tbuf); err != nil {
		return 0
	}
	return 1
}
