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

import (
	"io"
	"math/rand"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestCountedWriteCanReportTotalWritten(t *testing.T) {
	cw := countedWriter{
		w: fileutil.Discard,
	}
	total := uint64(0)
	for i := 0; i < 16; i++ {
		sz := rand.Uint64() % 1024
		v := make([]byte, sz)
		total += sz
		_, err := cw.Write(v)
		require.NoError(t, err)
	}
	require.Equal(t, total, cw.total)
}

type dummySM struct{}

func (d *dummySM) Open(<-chan struct{}) (uint64, error)          { return 0, nil }
func (d *dummySM) Update(entries []sm.Entry) ([]sm.Entry, error) { return nil, nil }
func (d *dummySM) Lookup(query interface{}) (interface{}, error) { return nil, nil }
func (d *dummySM) NALookup(query []byte) ([]byte, error)         { return nil, nil }
func (d *dummySM) Sync() error                                   { return nil }
func (d *dummySM) Prepare() (interface{}, error)                 { return nil, nil }
func (d *dummySM) Save(interface{},
	io.Writer, sm.ISnapshotFileCollection, <-chan struct{}) error {
	return nil
}
func (d *dummySM) Recover(io.Reader, []sm.SnapshotFile, <-chan struct{}) error {
	return nil
}
func (d *dummySM) Close() error             { return nil }
func (d *dummySM) GetHash() (uint64, error) { return 0, nil }
func (d *dummySM) Concurrent() bool         { return false }
func (d *dummySM) OnDisk() bool             { return false }
func (d *dummySM) Type() pb.StateMachineType {
	return pb.OnDiskStateMachine
}

func TestDestroyedFlagIsSetWhenDestroyed(t *testing.T) {
	sm := NewNativeSM(config.Config{}, &dummySM{}, nil)
	sm.Loaded()
	sm.Offloaded()
	require.Equal(t, uint64(0), sm.loadedCount)
	require.False(t, sm.destroyed)
	select {
	case <-sm.DestroyedC():
		require.Fail(t, "destroyedC unexpected closed")
	default:
	}

	require.NoError(t, sm.Close())
	require.True(t, sm.destroyed)
	select {
	case <-sm.DestroyedC():
	default:
		require.Fail(t, "destroyed ch not closed")
	}
}

func TestLookupWillFailOnClosedStateMachine(t *testing.T) {
	sm := NewNativeSM(config.Config{}, &dummySM{}, nil)
	sm.Loaded()
	sm.Offloaded()
	require.NoError(t, sm.Close())
	_, err := sm.Lookup(nil)
	require.Equal(t, ErrShardClosed, err)
	_, err = sm.NALookup(nil)
	require.Equal(t, ErrShardClosed, err)
}
