// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package id

import (
	"testing"
)

func TestIsNodeHostID(t *testing.T) {
	nhidTests := []string{
		"nhid-1",
		"nhid-123456789",
		"nhid-4023449441798808321",
	}
	notnhidTests := []string{
		"",
		"nhid-0",
		"1234567890",
		"nhid1234567890",
		"nhid4023449441798808321",
		"NHID-4023449441798808321",
		"nhi-4023449441798808321",
	}
	for _, v := range nhidTests {
		if !IsNodeHostID(v) {
			t.Errorf("%s not considered as a nhid", v)
		}
	}
	for _, v := range notnhidTests {
		if IsNodeHostID(v) {
			t.Errorf("%s considered as a nhid", v)
		}
	}
}

func TestParseNodeHostID(t *testing.T) {
	nhidTests := []string{
		"nhid-1",
		"nhid-123456789",
		"nhid-4023449441798808321",
	}
	notnhidTests := []string{
		"",
		"nhid-0",
		"1234567890",
		"nhid1234567890",
		"nhid4023449441798808321",
		"NHID-4023449441798808321",
		"nhi-4023449441798808321",
	}
	for _, v := range nhidTests {
		if _, err := ParseNodeHostID(v); err != nil {
			t.Errorf("%s considered as a nhid", v)
		}
	}
	for _, v := range notnhidTests {
		if _, err := ParseNodeHostID(v); err == nil {
			t.Errorf("%s not considered as a nhid", v)
		}
	}
}

func TestZeroIDNotAllowed(t *testing.T) {
	if _, err := NewNodeHostID(0); err == nil {
		t.Errorf("0 nhid allowed")
	}
}

func TestNHIDCanBeMarshaled(t *testing.T) {
	for _, v := range []uint64{1, 123, 1234567890, 4023449441798808321} {
		nhid, err := NewNodeHostID(v)
		if err != nil {
			t.Fatalf("failed to get nhid")
		}
		data, err := nhid.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal %v", err)
		}
		nn := &NodeHostID{}
		if err := nn.Unmarshal(data); err != nil {
			t.Fatalf("failed to unmarshal %v", err)
		}
		if nn.Value() != v {
			t.Errorf("value changed, got %d, expect %d", nn.Value(), v)
		}
	}
}

func TestString(t *testing.T) {
	nhidTests := []string{
		"nhid-1",
		"nhid-123456789",
		"nhid-4023449441798808321",
	}
	for _, v := range nhidTests {
		nhid, err := ParseNodeHostID(v)
		if err != nil {
			t.Fatalf("failed to parse nhid")
		}
		if v != nhid.String() {
			t.Fatalf("string value changed, got %s, expect %s", nhid.String(), v)
		}
	}
}
