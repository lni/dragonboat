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

/*
Package settings is used for managing internal parameters that can be set at
compile time by expert level users.
*/
package settings

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v3/logger"
)

var (
	plog = logger.GetLogger("settings")
)

const (
	defaultStepEngineWorkerCount uint64 = 16
	defaultLogDBPoolSize         uint64 = 16
)

//
// Parameters in both hard.go and soft.go are _NOT_ a part of the public API.
// There is no guarantee that any of these parameters are going to be available
// in future releases. Change them only when you know what you are doing.
//
// This file contain hard configuration values that should _NEVER_ be changed
// after your system has been deployed. Changing any value here will CORRUPT
// the data in your existing deployment.
//
// We do have an mechanism to overwrite the default values for the hard struct.
// To tune these parameters, place a json file named
// dragonboat-hard-settings.json in the current working directory of your
// dragonboat application, all fields in the json file will be applied to
// overwrite the default setting values. e.g. for a json file with the
// following content -
//
// {
//   "StepEngineWorkerCount": 32,
// }
//
// hard.StepEngineWorkerCount will be set to 32
//
// The application need to be restarted to apply such configuration changes.
// Again - tuning these hard parameters using the above described json file
// will cause your existing data to be corrupted. Decide them in your dev/test
// phase, once your system is deployed in production, _NEVER_ change them.
//

// Hard is the hard settings that can not be changed after the system has been
// deployed.
var Hard = getHardSettings()

type hard struct {
	// StepEngineWorkerCount defines number of workers to use to process raft node
	// changes. Together with the LogDBPoolSize parameters below, they determine
	// the content of each logdb shards. You will have to build your own tools to
	// move logdb data around to be able to change StepEngineWorkerCount and
	// LogDBPoolSize after your system is deployed.
	StepEngineWorkerCount uint64
	// LogDBPoolSize defines the number of logdb shards to use. When you get slow
	// performance when using the default LogDBPoolSize value, it typically means
	// your disk is not good enough for concurrent write acdesses.
	LogDBPoolSize uint64
	// LRUMaxSessionCount is the max number of client sessions that can be
	// concurrently held and managed by each raft cluster.
	LRUMaxSessionCount uint64
	// LogDBEntryBatchSize is the max size of each entry batch.
	LogDBEntryBatchSize uint64
}

// BlockFileMagicNumber is the magic number used in block based snapshot files.
var BlockFileMagicNumber = []byte{0x3F, 0x5B, 0xCB, 0xF1, 0xFA, 0xBA, 0x81, 0x9F}

const (
	//
	// RSM
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	//

	// MaxProposalPayloadSize is the max size allowed for a proposal payload.
	MaxProposalPayloadSize uint64 = 32 * 1024 * 1024
	// MaxMessageSize is the max size for a single gRPC message sent between
	// raft nodes. It must be greater than MaxProposalPayloadSize and smaller
	// than the current default of max gRPC send/receive size (4MBytes).
	MaxMessageSize uint64 = 2*MaxProposalPayloadSize + 2*1024*1024
	// SnapshotChunkSize is the snapshot chunk size sent by the gRPC transport
	// module.
	SnapshotChunkSize uint64 = 2 * 1024 * 1024

	//
	// Drummer DB
	//

	// LaunchDeadlineTick defines the number of ticks allowed for the bootstrap
	// process to complete.
	LaunchDeadlineTick uint64 = 24
)

func (h *hard) Hash() uint64 {
	hashstr := fmt.Sprintf("%d-%d-%t-%d-%d",
		h.StepEngineWorkerCount,
		h.LogDBPoolSize,
		false, // was the UseRangeDelete option
		h.LRUMaxSessionCount,
		h.LogDBEntryBatchSize)
	mh := md5.New()
	if _, err := io.WriteString(mh, hashstr); err != nil {
		panic(err)
	}
	md5hash := mh.Sum(nil)
	return binary.LittleEndian.Uint64(md5hash)
}

func getHardSettings() hard {
	org := getDefaultHardSettings()
	overwriteHardSettings(&org)
	return org
}

func getDefaultHardSettings() hard {
	return hard{
		StepEngineWorkerCount: defaultStepEngineWorkerCount,
		LogDBPoolSize:         defaultLogDBPoolSize,
		LRUMaxSessionCount:    4096,
		LogDBEntryBatchSize:   48,
	}
}
