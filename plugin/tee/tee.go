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

package tee

import (
	"github.com/lni/dragonboat/v4/config"
	tl "github.com/lni/dragonboat/v4/internal/logdb/tee"
	"github.com/lni/dragonboat/v4/internal/tan"
	"github.com/lni/dragonboat/v4/raftio"
)

// CreateTanPebbleLogDB creates a Tee LogDB backed by Tan and Pebble.
func CreateTanPebbleLogDB(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	ndirs := make([]string, 0)
	nwals := make([]string, 0)
	fs := cfg.Expert.FS
	for _, v := range dirs {
		ndirs = append(ndirs, fs.PathJoin(v, "tee-tan"))
	}
	for _, v := range wals {
		nwals = append(nwals, fs.PathJoin(v, "tee-tan"))
	}
	tdb, err := tan.CreateTan(cfg, cb, ndirs, nwals)
	if err != nil {
		return nil, err
	}
	pdb, err := tl.NewPebbleLogDB(cfg, cb, dirs, wals)
	if err != nil {
		return nil, err
	}
	return tl.MakeTeeLogDB(tdb, pdb), nil
}

// TanPebbleLogDBFactory is the factory for creating a tan and pebble backed
// tee LogDB instance.
var TanPebbleLogDBFactory = tanPebbleLogDBFactory{}

type tanPebbleLogDBFactory struct{}

func (tanPebbleLogDBFactory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback, dirs []string, wals []string) (raftio.ILogDB, error) {
	return CreateTanPebbleLogDB(cfg, cb, dirs, wals)
}

func (tanPebbleLogDBFactory) Name() string {
	return "tan-pebble-tee"
}
