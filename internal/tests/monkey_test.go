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

package tests

import (
	"testing"
)

func TestMonkeyFlagIsNotSet(t *testing.T) {
	if TestMonkeyEnabled {
		t.Fatalf("test monkey unexpectedly enabled")
	}
}

func TestReadyToReturnTestKnobAlwaysReturnFalse(t *testing.T) {
	ch := make(chan struct{})
	if ReadyToReturnTestKnob(ch, false, "") {
		t.Fatalf("didn't return false")
	}
	close(ch)
	if ReadyToReturnTestKnob(ch, false, "") {
		t.Fatalf("didn't return false")
	}
}
