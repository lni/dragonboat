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

package dragonboat

import (
	"reflect"
	"testing"

	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v4/internal/rsm"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestSnapshotTaskCanBeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	sr.setTask(rec)
	if !sr.hasTask {
		t.Errorf("rec not set")
	}
}

func TestSnapshotTaskCanNotBeSetTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	sr := snapshotTask{}
	rec := rsm.Task{}
	sr.setTask(rec)
	sr.setTask(rec)
}

func TestCanGetSnapshotTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	if _, ok := sr.getTask(); ok {
		t.Errorf("unexpected record")
	}
	rec := rsm.Task{}
	sr.setTask(rec)
	r, ok := sr.getTask()
	if !ok {
		t.Errorf("no record to get")
	}
	if !reflect.DeepEqual(&rec, &r) {
		t.Errorf("unexpected rec")
	}
	rec, ok = sr.getTask()
	if ok {
		t.Errorf("record is still available")
	}
}

func TestStreamTaskCanBeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	fn := func() pb.IChunkSink { return nil }
	sr.setStreamTask(rec, fn)
	if !sr.hasTask || !reflect.DeepEqual(&(sr.t), &rec) || sr.getSinkFn == nil {
		t.Errorf("failed to set stream task")
	}
}

func TestStreamTaskCanNotBeSetTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	fn := func() pb.IChunkSink { return nil }
	sr.setStreamTask(rec, fn)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	sr.setStreamTask(rec, fn)
}
