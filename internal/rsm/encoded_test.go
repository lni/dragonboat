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
	"crypto/rand"
	"testing"

	"github.com/lni/dragonboat/v4/internal/utils/dio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

func mustGetPayload(e pb.Entry) []byte {
	p, err := GetPayload(e)
	if err != nil {
		panic(err)
	}
	return p
}

func TestGetEntryPayload(t *testing.T) {
	e1 := pb.Entry{Cmd: []byte{1, 2, 3, 4, 5}}
	e2 := pb.Entry{Cmd: []byte{1, 2, 3}, Type: pb.ConfigChangeEntry}
	e3payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	e3 := pb.Entry{
		Type: pb.EncodedEntry,
		Cmd:  GetEncoded(dio.Snappy, e3payload, make([]byte, 512)),
	}
	require.Equal(t, e1.Cmd, mustGetPayload(e1), "e1 payload changed")
	require.Equal(t, e2.Cmd, mustGetPayload(e2), "e2 payload changed")
	require.Equal(t, e3payload, mustGetPayload(e3), "e3 payload changed")
}

func TestGetV0EncodedPayload(t *testing.T) {
	l1, _ := dio.MaxEncodedLen(dio.Snappy, 16)
	tests := []struct {
		ct  dio.CompressionType
		src uint64
		dst uint64
	}{
		{dio.NoCompression, 16, 0},
		{dio.NoCompression, 16, 1},
		{dio.NoCompression, 16, 16},
		{dio.NoCompression, 16, 17},
		{dio.Snappy, 16, 0},
		{dio.Snappy, 16, 1},
		{dio.Snappy, 16, 16},
		{dio.Snappy, 16, l1},
		{dio.Snappy, 16, l1 - 1},
		{dio.Snappy, 16, l1 + 1},
		{dio.Snappy, 16, 128},
	}
	for idx, tt := range tests {
		plog.Infof("idx: %d", idx)
		var src []byte
		if tt.src == 0 {
			src = nil
		} else {
			src = make([]byte, tt.src)
			_, err := rand.Read(src)
			require.NoError(t, err)
		}
		var dst []byte
		if tt.dst == 0 {
			dst = nil
		} else {
			dst = make([]byte, tt.dst)
		}
		result := GetEncoded(tt.ct, src, dst)
		ver, ct, hasSession := parseEncodedHeader(result)
		require.Equal(t, EEV0, ver, "invalid version number %d", ver)
		require.False(t, hasSession, "unexpectedly has session flag set")
		if tt.ct == dio.NoCompression {
			require.Equal(t, EENoCompression, ct, "unexpected ct")
		}
		if tt.ct == dio.Snappy {
			require.Equal(t, EESnappy, ct, "invalid ct")
		}
		decoded, err := getDecodedPayload(result, nil)
		require.NoError(t, err, "failed to get decoded payload %v", err)
		require.Equal(t, src, decoded, "%d, content changed", idx)
	}
}
