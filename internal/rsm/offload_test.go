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

package rsm

/*
import (
	"testing"
)

func TestOffloadedStatusReadyToDestroy(t *testing.T) {
	o := OffloadedStatus{}
	o.DestroyedC = make(chan struct{})
	if o.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	if o.Destroyed() {
		t.Errorf("destroyed, not expected")
	}
	o.SetDestroyed()
	if !o.Destroyed() {
		t.Errorf("not destroyed, not expected")
	}
}

func TestOffloadedStatusAllOffloadedWillMakeItReadyToDestroy(t *testing.T) {
	o := OffloadedStatus{}
	o.DestroyedC = make(chan struct{})
	o.SetOffloaded(FromStepWorker)
	o.SetOffloaded(FromCommitWorker)
	o.SetOffloaded(FromApplyWorker)
	o.SetOffloaded(FromSnapshotWorker)
	if o.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	o.SetOffloaded(FromNodeHost)
	if !o.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
}

func TestOffloadedStatusOffloadedFromNodeHostIsHandled(t *testing.T) {
	o := OffloadedStatus{}
	o.DestroyedC = make(chan struct{})
	o.SetOffloaded(FromNodeHost)
	if !o.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
	o1 := OffloadedStatus{}
	o1.SetLoaded(FromStepWorker)
	if !o1.loadedByStepWorker {
		t.Errorf("set loaded didn't set component as loaded")
	}
	o1.SetOffloaded(FromNodeHost)
	if o1.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	o1.SetOffloaded(FromStepWorker)
	if !o1.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
}*/
