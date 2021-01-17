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
	"encoding/binary"
	"reflect"
	"testing"
)

func TestRequstHeaderCanBeEncodedAndDecoded(t *testing.T) {
	r := requestHeader{
		method: raftType,
		size:   1024,
		crc:    1000,
	}
	buf := make([]byte, requestHeaderSize)
	result := r.encode(buf)
	if len(result) != requestHeaderSize {
		t.Fatalf("unexpected size")
	}
	rr := requestHeader{}
	if !rr.decode(result) {
		t.Fatalf("decode failed")
	}
	if !reflect.DeepEqual(&r, &rr) {
		t.Errorf("request header changed")
	}
}

func TestRequestHeaderCRCIsChecked(t *testing.T) {
	r := requestHeader{
		method: raftType,
		size:   1024,
		crc:    1000,
	}
	buf := make([]byte, requestHeaderSize)
	result := r.encode(buf)
	if len(result) != requestHeaderSize {
		t.Fatalf("unexpected size")
	}
	rr := requestHeader{}
	if !rr.decode(result) {
		t.Fatalf("decode failed")
	}
	crc := binary.BigEndian.Uint32(result[10:])
	binary.BigEndian.PutUint32(result[10:], crc+1)
	if rr.decode(result) {
		t.Fatalf("crc error not reported")
	}
	binary.BigEndian.PutUint32(result[10:], crc)
	if !rr.decode(result) {
		t.Fatalf("decode failed")
	}
	binary.BigEndian.PutUint64(result[2:], 0)
	if rr.decode(result) {
		t.Fatalf("crc error not reported")
	}
}

func TestInvalidMethodNameIsReported(t *testing.T) {
	r := requestHeader{
		method: 1024,
		size:   1024,
		crc:    1000,
	}
	buf := make([]byte, requestHeaderSize)
	result := r.encode(buf)
	if len(result) != requestHeaderSize {
		t.Fatalf("unexpected size")
	}
	rr := requestHeader{}
	if rr.decode(result) {
		t.Fatalf("decode did not report invalid method name")
	}
}
