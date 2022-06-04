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

package tan

import (
	"bufio"
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/vfs"
)

func TestEntryAppend(t *testing.T) {
	tests := []struct {
		existing indexEntry
		newEntry indexEntry
		result1  indexEntry
		result2  indexEntry
		merged   bool
	}{
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 30, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 30, 100, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{18, 30, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{18, 30, 100, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 30, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 30, 100, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 110, 10},
			indexEntry{10, 30, 100, 100, 20},
			indexEntry{},
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{22, 30, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{22, 30, 100, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{8, 30, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{8, 30, 100, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 101, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 101, 100, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 100 + indexBlockSize, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 100 + indexBlockSize, 10},
			false,
		},
		{
			indexEntry{10, 20, 100, 0, indexBlockSize - 1},
			indexEntry{21, 30, 100, indexBlockSize - 1, 10},
			indexEntry{10, 30, 100, 0, indexBlockSize + 9},
			indexEntry{},
			true,
		},
	}
	for idx, tt := range tests {
		e1, e2, merged := tt.existing.merge(tt.newEntry)
		require.Equalf(t, tt.result1, e1, "idx: %d", idx)
		require.Equalf(t, tt.result2, e2, "idx: %d", idx)
		require.Equalf(t, tt.merged, merged, "idx: %d", idx)
	}
}

func TestEntryAndSingleEntryIndexUpdate(t *testing.T) {
	tests := []struct {
		existing indexEntry
		newEntry indexEntry
		result1  indexEntry
		result2  indexEntry
		merged   bool
		more     bool
	}{
		// #0
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 110, 10},
			indexEntry{10, 30, 100, 100, 20},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 101, 105, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 101, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 30, 101, 105, 10},
			indexEntry{10, 19, 100, 100, 10},
			indexEntry{20, 30, 101, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 30, 100, 105, 10},
			indexEntry{10, 19, 100, 100, 10},
			indexEntry{20, 30, 100, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{15, 30, 100, 105, 10},
			indexEntry{10, 14, 100, 100, 10},
			indexEntry{15, 30, 100, 105, 10},
			false,
			false,
		},
		// #5
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{15, 30, 101, 105, 10},
			indexEntry{10, 14, 100, 100, 10},
			indexEntry{15, 30, 101, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{15, 16, 100, 105, 10},
			indexEntry{10, 14, 100, 100, 10},
			indexEntry{15, 16, 100, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 30, 100, 105, 10},
			indexEntry{5, 30, 100, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 30, 101, 105, 10},
			indexEntry{5, 30, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 20, 100, 100, 10},
			indexEntry{5, 20, 100, 100, 10},
			indexEntry{},
			true,
			true,
		},
		// #10
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 20, 101, 105, 10},
			indexEntry{5, 20, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 12, 100, 100, 10},
			indexEntry{5, 12, 100, 100, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 12, 101, 105, 10},
			indexEntry{5, 12, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 9, 100, 105, 10},
			indexEntry{5, 9, 100, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 9, 101, 105, 10},
			indexEntry{5, 9, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		// #15
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 20, 101, 105, 10},
			indexEntry{10, 20, 101, 105, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 5, 101, 105, 10},
			indexEntry{5, 5, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 10, 101, 105, 10},
			indexEntry{10, 10, 101, 105, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{15, 15, 101, 105, 10},
			indexEntry{10, 14, 100, 100, 10},
			indexEntry{15, 15, 101, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 20, 101, 105, 10},
			indexEntry{10, 19, 100, 100, 10},
			indexEntry{20, 20, 101, 105, 10},
			false,
			false,
		},
		// #20
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 21, 101, 105, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 21, 101, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{5, 5, 100, 105, 10},
			indexEntry{5, 5, 100, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 10, 100, 105, 10},
			indexEntry{10, 10, 100, 105, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{15, 15, 100, 105, 10},
			indexEntry{10, 14, 100, 100, 10},
			indexEntry{15, 15, 100, 105, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 20, 100, 105, 10},
			indexEntry{10, 19, 100, 100, 10},
			indexEntry{20, 20, 100, 105, 10},
			false,
			false,
		},
		// #25
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 21, 100, 110, 10},
			indexEntry{10, 21, 100, 100, 20},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{5, 8, 101, 105, 10},
			indexEntry{5, 8, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{5, 10, 101, 105, 10},
			indexEntry{5, 10, 101, 105, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{10, 15, 101, 105, 10},
			indexEntry{10, 15, 101, 105, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{11, 15, 101, 105, 10},
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{11, 15, 101, 105, 10},
			false,
			false,
		},
		// #30
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{5, 10, 101, 105 + indexBlockSize, 10},
			indexEntry{5, 10, 101, 105 + indexBlockSize, 10},
			indexEntry{},
			true,
			true,
		},
		{
			indexEntry{10, 10, 100, 100, 10},
			indexEntry{10, 15, 101, 105 + indexBlockSize, 10},
			indexEntry{10, 15, 101, 105 + indexBlockSize, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{20, 20, 100, 100 + indexBlockSize, 10},
			indexEntry{10, 19, 100, 100, 10},
			indexEntry{20, 20, 100, 100 + indexBlockSize, 10},
			false,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{10, 10, 100, 100 + indexBlockSize, 10},
			indexEntry{10, 10, 100, 100 + indexBlockSize, 10},
			indexEntry{},
			true,
			false,
		},
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 100 + indexBlockSize, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{21, 30, 100, 100 + indexBlockSize, 10},
			false,
			false,
		},
		// #35
		{
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{30, 35, 100, 100, 10},
			indexEntry{10, 20, 100, 100, 10},
			indexEntry{30, 35, 100, 100, 10},
			false,
			false,
		},
		{
			indexEntry{27, 27, 3, 47, 10},
			indexEntry{27, 40, 5, 23, 10},
			indexEntry{27, 40, 5, 23, 10},
			indexEntry{},
			true,
			false,
		},
	}

	// entry update
	for idx, tt := range tests {
		e1, e2, merged, more := tt.existing.update(tt.newEntry)
		require.Equalf(t, tt.result1, e1, "idx: %d", idx)
		require.Equalf(t, tt.result2, e2, "idx: %d", idx)
		require.Equalf(t, tt.merged, merged, "idx: %d", idx)
		require.Equalf(t, tt.more, more, "idx: %d", idx)
	}
	// single entry index update
	for idx, tt := range tests {
		testIndex := index{entries: []indexEntry{tt.existing}}
		testIndex.update(tt.newEntry)
		if len(testIndex.entries) == 1 {
			require.Equalf(t, tt.result1, testIndex.entries[0], "idx: %d", idx)
			require.Truef(t, tt.merged, "idx: %d", idx)
		} else if len(testIndex.entries) == 2 {
			require.Equalf(t, tt.result1, testIndex.entries[0], "idx: %d", idx)
			require.Equalf(t, tt.result2, testIndex.entries[1], "idx: %d", idx)
			require.Falsef(t, tt.merged, "idx: %d", idx)
		} else {
			t.Errorf("%d unexpected entry count", idx)
		}
	}
}

func TestIndexUpdate(t *testing.T) {
	tests := []struct {
		existing []indexEntry
		newEntry indexEntry
		result   []indexEntry
	}{
		// #0
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{21, 30, 100, 110, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 30, 100, 100, 20}},
		},

		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{21, 30, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}, {21, 30, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{20, 30, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 19, 100, 100, 10}, {20, 30, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{20, 30, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 19, 100, 100, 10}, {20, 30, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{15, 30, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 14, 100, 100, 10}, {15, 30, 100, 105, 10}},
		},
		// #5
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{15, 30, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 14, 100, 100, 10}, {15, 30, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{15, 16, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 14, 100, 100, 10}, {15, 16, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 30, 100, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 30, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 30, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 30, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 20, 100, 100, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 20, 100, 100, 10}},
		},
		// #10
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 20, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 20, 101, 105, 10}},
		},

		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 12, 100, 100, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 12, 100, 100, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 12, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 12, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 9, 100, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 9, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 9, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 9, 101, 105, 10}},
		},
		// #15
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{10, 20, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 5, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 5, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{10, 10, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{15, 15, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 14, 100, 100, 10}, {15, 15, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{20, 20, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 19, 100, 100, 10}, {20, 20, 101, 105, 10}},
		},
		// #20
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{21, 21, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}, {21, 21, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{5, 5, 100, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 5, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{10, 10, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{15, 15, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 14, 100, 100, 10}, {15, 15, 100, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{20, 20, 100, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 19, 100, 100, 10}, {20, 20, 100, 105, 10}},
		},
		// #25
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 20, 100, 100, 10}},
			indexEntry{21, 21, 100, 110, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 21, 100, 100, 20}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{5, 8, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 8, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{5, 10, 101, 105, 10},
			[]indexEntry{{1, 4, 99, 50, 10}, {5, 10, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{10, 15, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 15, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{11, 15, 101, 105, 10},
			[]indexEntry{{1, 9, 99, 50, 10}, {10, 10, 100, 100, 10}, {11, 15, 101, 105, 10}},
		},
		// #30
		{
			[]indexEntry{{1, 4, 98, 40, 10}, {5, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{3, 15, 101, 105, 10},
			[]indexEntry{{1, 2, 98, 40, 10}, {3, 15, 101, 105, 10}},
		},
		{
			[]indexEntry{{1, 4, 98, 40, 10}, {5, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{3, 4, 101, 105, 10},
			[]indexEntry{{1, 2, 98, 40, 10}, {3, 4, 101, 105, 10}},
		},
		{
			[]indexEntry{{4, 4, 98, 40, 10}, {5, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{3, 3, 101, 105, 10},
			[]indexEntry{{3, 3, 101, 105, 10}},
		},
		{
			[]indexEntry{{4, 4, 98, 40, 10}, {5, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{4, 4, 101, 105, 10},
			[]indexEntry{{4, 4, 101, 105, 10}},
		},
		{
			[]indexEntry{{2, 4, 98, 40, 10}, {5, 9, 99, 50, 10}, {10, 10, 100, 100, 10}},
			indexEntry{1, 15, 101, 105, 10},
			[]indexEntry{{1, 15, 101, 105, 10}},
		},
	}
	for idx, tt := range tests {
		testIndex := index{tt.existing, 0}
		testIndex.update(tt.newEntry)
		require.Equalf(t, tt.result, testIndex.entries, "idx: %d", idx)
	}
}

func TestIndexQuery(t *testing.T) {
	tests := []struct {
		entries []indexEntry
		start   uint64
		end     uint64
		result  []indexEntry
		ok      bool
	}{
		// #0
		{
			[]indexEntry{{1, 5, 100, 100, 10}},
			2, 3,
			[]indexEntry{{1, 5, 100, 100, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}},
			3, 5,
			[]indexEntry{{1, 5, 100, 100, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}},
			1, 4,
			[]indexEntry{{1, 5, 100, 100, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}},
			2, 6,
			[]indexEntry{{1, 5, 100, 100, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}},
			6, 7,
			[]indexEntry{},
			false,
		},
		// #5
		{
			[]indexEntry{{5, 10, 100, 100, 10}},
			1, 4,
			[]indexEntry{},
			false,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			2, 8,
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			2, 12,
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			2, 4,
			[]indexEntry{{1, 5, 100, 100, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			6, 8,
			[]indexEntry{{6, 10, 101, 50, 10}},
			true,
		},
		// #10
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			7, 12,
			[]indexEntry{{6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			true,
		},
		{
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			2, 12,
			[]indexEntry{{1, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			true,
		},
		{
			[]indexEntry{{3, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			2, 6,
			[]indexEntry{},
			false,
		},
		{
			[]indexEntry{{3, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			16, 18,
			[]indexEntry{},
			false,
		},
		{
			[]indexEntry{{3, 5, 100, 100, 10}, {6, 10, 101, 50, 10}, {11, 15, 102, 90, 10}},
			6, 7,
			[]indexEntry{{6, 10, 101, 50, 10}},
			true,
		},
		// #15
		{
			[]indexEntry{},
			6, 7,
			[]indexEntry{},
			false,
		},
		{
			[]indexEntry{{1, 10, 10, 10, 10}, {20, 30, 10, 20, 10}},
			2, 11,
			[]indexEntry{{1, 10, 10, 10, 10}},
			true,
		},
		{
			[]indexEntry{{1, 10, 10, 10, 10}, {20, 30, 10, 20, 10}},
			2, 21,
			[]indexEntry{{1, 10, 10, 10, 10}},
			true,
		},
		{
			[]indexEntry{{1, 10, 10, 10, 10}, {20, 30, 10, 20, 10}},
			11, 15,
			[]indexEntry{},
			false,
		},
		{
			[]indexEntry{{1, 10, 10, 10, 10}, {20, 30, 10, 20, 10}},
			11, 21,
			[]indexEntry{},
			false,
		},
		// #20
		{
			[]indexEntry{{1, 10, 10, 10, 10}, {20, 30, 10, 20, 10}},
			20, 21,
			[]indexEntry{{20, 30, 10, 20, 10}},
			true,
		},
	}
	for idx, tt := range tests {
		testIndex := index{entries: tt.entries}
		result, ok := testIndex.query(tt.start, tt.end)
		require.Equalf(t, tt.ok, ok, "idx: %d", idx)
		require.Equalf(t, tt.result, result, "idx: %d", idx)
	}
}

func TestIndexEncodeDecode(t *testing.T) {
	tests := []struct {
		entries     []indexEntry
		compactedTo uint64
	}{
		{
			nil, 0,
		},
		{
			nil, 10,
		},
		{
			[]indexEntry{}, 0,
		},
		{
			[]indexEntry{{2, 2, 100, 100, 10}}, 0,
		},
		{
			[]indexEntry{{2, 2, 100, 100, 10}, {5, 5, 101, 10, 10}, {10, 10, 102, 10, 10}}, 0,
		},
		{
			[]indexEntry{{2, 2, 100, 100, 10}, {5, 5, 101, 10, 10}, {10, 10, 102, 10, 10}}, 10,
		},
	}
	for idx, tt := range tests {
		input := &index{tt.entries, tt.compactedTo}
		buf := bytes.NewBuffer(nil)
		require.NoErrorf(t, input.encode(buf), "idx: %d", idx)
		decoded := &index{}
		d := &indexDecoder{bufio.NewReader(buf)}
		require.NoErrorf(t, decoded.decode(d), "idx: %d", idx)
		if len(decoded.entries) == 0 {
			require.Truef(t, len(input.entries) == 0, "idx: %d", idx)
		} else {
			require.Equalf(t, input, decoded, "idx: %d", idx)
		}
	}
}

func TestIndexSaveLoad(t *testing.T) {
	i1 := &nodeIndex{
		shardID:   2,
		replicaID: 3,
		currEntries: index{
			entries: []indexEntry{{1, 100, 5, 100, 10}},
		},
		snapshot: indexEntry{30, snapshotFlag, 5, 110, 10},
		state:    indexEntry{6, stateFlag, 5, 20, 10},
	}
	i2 := &nodeIndex{
		shardID:   3,
		replicaID: 4,
		currEntries: index{
			entries: []indexEntry{{1, 100, 5, 100, 10}, {101, 102, 6, 100, 10}},
		},
		snapshot: indexEntry{30, snapshotFlag, 5, 110, 10},
		state:    indexEntry{6, stateFlag, 5, 20, 10},
	}
	ni1 := raftio.NodeInfo{ShardID: 2, ReplicaID: 3}
	ni2 := raftio.NodeInfo{ShardID: 3, ReplicaID: 4}
	nodeStates := newNodeStates()
	nodeStates.indexes[ni1] = i1
	nodeStates.indexes[ni2] = i2
	fs := vfs.NewMem()
	dirname := "db-dir"
	require.NoError(t, fs.MkdirAll(dirname, 0755))
	dir, err := fs.OpenDir(dirname)
	require.NoError(t, err)
	defer dir.Close()
	i1entries := i1.currEntries
	i2entries := i2.currEntries
	require.NoError(t, nodeStates.save(dirname, dir, fileNum(1), fs))

	loaded := newNodeStates()
	require.NoError(t, loaded.load(dirname, fileNum(1), fs))
	require.Equal(t, 2, len(loaded.indexes))
	require.Equal(t, i1.shardID, loaded.indexes[ni1].shardID)
	require.Equal(t, i1.replicaID, loaded.indexes[ni1].replicaID)
	require.Equal(t, i1entries, loaded.indexes[ni1].entries)
	require.Equal(t, i2entries, loaded.indexes[ni2].entries)
	require.Equal(t, i1.snapshot, loaded.indexes[ni1].snapshot)
	require.Equal(t, i2.snapshot, loaded.indexes[ni2].snapshot)
	require.Equal(t, i1.state, loaded.indexes[ni1].state)
	require.Equal(t, i2.state, loaded.indexes[ni2].state)
}

func TestIndexLoadIsAppendOnly(t *testing.T) {
	i := &nodeIndex{
		shardID:   2,
		replicaID: 3,
		currEntries: index{
			entries: []indexEntry{{101, 102, 2, 0, 10}, {102, 104, 2, 58, 10}},
		},
	}
	ni1 := raftio.NodeInfo{ShardID: 2, ReplicaID: 3}
	nodeStates := newNodeStates()
	nodeStates.indexes[ni1] = i
	currEntries := i.currEntries
	fs := vfs.NewMem()
	dirname := "db-dir"
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	dir, err := fs.OpenDir(dirname)
	require.NoError(t, err)
	defer dir.Close()
	require.NoError(t, nodeStates.save(dirname, dir, fileNum(5), fs))
	loaded := newNodeStates()
	require.NoError(t, loaded.load(dirname, fileNum(5), fs))
	require.Equal(t, currEntries, loaded.indexes[ni1].entries)
}

func TestIndexCompaction(t *testing.T) {
	tests := []struct {
		entries     []indexEntry
		compactedTo uint64
		obsolete    fileNum
	}{
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 21, 3, 0, 10}},
			21,
			fileNum(2),
		},
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 21, 3, 0, 10}},
			9,
			fileNum(0),
		},
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 21, 3, 0, 10}},
			17,
			fileNum(2),
		},
	}
	for idx, tt := range tests {
		entries := index{
			entries:     tt.entries,
			compactedTo: tt.compactedTo,
		}
		fn := entries.compaction()
		require.Equalf(t, tt.obsolete, fn, "idx: %d", idx)
	}
}

func TestIndexRemoveObsolete(t *testing.T) {
	tests := []struct {
		entries            []indexEntry
		maxObsoleteFileNum fileNum
		result             []fileNum
	}{
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 21, 3, 0, 10}},
			2,
			[]fileNum{1, 2},
		},
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 21, 3, 0, 10}},
			1,
			[]fileNum{1},
		},
		{
			[]indexEntry{{10, 15, 1, 0, 10}, {16, 17, 2, 0, 10}, {18, 20, 2, 10, 10}, {21, 25, 3, 0, 10}},
			2,
			[]fileNum{1, 2},
		},
		{
			[]indexEntry{{10, 15, 3, 0, 10}, {16, 17, 4, 0, 10}, {18, 21, 5, 0, 10}},
			2,
			nil,
		},
	}
	for idx, tt := range tests {
		entries := index{
			entries: tt.entries,
		}
		result := entries.removeObsolete(tt.maxObsoleteFileNum)
		require.Equalf(t, tt.result, result, "idx: %d", idx)
	}
}

func TestNodeIndexUpdate(t *testing.T) {
	n := nodeIndex{
		entries: index{
			entries: []indexEntry{{1, 5, 5, 10, 10}, {6, 10, 6, 10, 10}},
		},
		snapshot: indexEntry{3, snapshotFlag, 5, 6, 10},
		currEntries: index{
			entries: []indexEntry{{6, 10, 6, 10, 10}},
		},
		state: indexEntry{10, stateFlag, 6, 10, 10},
	}
	exp := nodeIndex{
		entries: index{
			entries: []indexEntry{{1, 5, 5, 10, 10}, {6, 10, 6, 10, 10}, {12, 12, 6, 12, 10}},
		},
		snapshot: indexEntry{11, snapshotFlag, 6, 11, 10},
		currEntries: index{
			entries: []indexEntry{{6, 10, 6, 10, 10}, {12, 12, 6, 12, 10}},
		},
		state: indexEntry{11, stateFlag, 6, 13, 10},
	}
	n.update(indexEntry{12, 12, 6, 12, 10},
		indexEntry{11, snapshotFlag, 6, 11, 10}, indexEntry{11, stateFlag, 6, 13, 10})
	require.Equal(t, exp, n)
}

func TestStateCompaction(t *testing.T) {
	st := nodeIndex{}
	require.Equal(t, fileNum(math.MaxUint64), st.stateCompaction())
	st = nodeIndex{state: indexEntry{10, stateFlag, 20, 10, 10}}
	require.Equal(t, fileNum(19), st.stateCompaction())
}

func TestSnapshotCompaction(t *testing.T) {
	st := nodeIndex{}
	require.Equal(t, fileNum(math.MaxUint64), st.snapshotCompaction())
	st = nodeIndex{snapshot: indexEntry{10, snapshotFlag, 20, 10, 10}}
	require.Equal(t, fileNum(19), st.snapshotCompaction())
}

func TestStateGetObsolete(t *testing.T) {
	nodeStates := newNodeStates()
	ni1 := raftio.NodeInfo{ShardID: 1, ReplicaID: 1}
	ni2 := raftio.NodeInfo{ShardID: 2, ReplicaID: 1}
	index1 := &nodeIndex{}
	index2 := &nodeIndex{}
	index1.entries.append(indexEntry{1, 100, 5, 10, 10})
	index1.entries.append(indexEntry{101, 102, 10, 10, 10})
	index2.entries.append(indexEntry{1, 100, 20, 10, 10})
	nodeStates.indexes[ni1] = index1
	nodeStates.indexes[ni2] = index2
	input := []fileNum{1, 2, 5, 6, 10, 20, 25}
	expected := []fileNum{1, 2, 6, 25}
	result := nodeStates.getObsolete(input)
	require.Equal(t, expected, result)
}
