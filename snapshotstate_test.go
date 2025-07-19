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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/internal/rsm"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestSnapshotTaskCanBeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	sr.setTask(rec)
	assert.True(t, sr.hasTask, "rec not set")
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
	_, ok := sr.getTask()
	assert.False(t, ok, "unexpected record")

	rec := rsm.Task{}
	sr.setTask(rec)
	r, ok := sr.getTask()
	assert.True(t, ok, "no record to get")
	assert.True(t, reflect.DeepEqual(&rec, &r), "unexpected rec")

	rec, ok = sr.getTask()
	assert.False(t, ok, "record is still available")
}

func TestStreamTaskCanBeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	fn := func() pb.IChunkSink { return nil }
	sr.setStreamTask(rec, fn)

	assert.True(t, sr.hasTask, "hasTask should be true")
	assert.True(t, reflect.DeepEqual(&(sr.t), &rec),
		"task should be set correctly")
	assert.NotNil(t, sr.getSinkFn, "getSinkFn should not be nil")
}

func TestStreamTaskCanNotBeSetTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sr := snapshotTask{}
	rec := rsm.Task{}
	fn := func() pb.IChunkSink { return nil }
	sr.setStreamTask(rec, fn)
	require.Panics(t, func() {
		sr.setStreamTask(rec, fn)
	})
}
