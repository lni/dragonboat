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
	"encoding/binary"
	"hash/crc32"
)

type requestHeader struct {
	method uint16
	size   uint64
	crc    uint32
}

func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		plog.Panicf("requestHeader input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint64(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[10:], 0)
	binary.BigEndian.PutUint32(buf[14:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[10:], v)
	return buf[:requestHeaderSize]
}

func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[10:])
	binary.BigEndian.PutUint32(buf[10:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		plog.Errorf("requestHeader crc check failed")
		return false
	}
	binary.BigEndian.PutUint32(buf[10:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != raftType && method != snapshotType {
		plog.Errorf("invalid method type")
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint64(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[14:])
	return true
}
