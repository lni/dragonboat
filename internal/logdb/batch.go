// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

package logdb

import (
	pb "github.com/lni/dragonboat/raftpb"
)

//
// Rather than storing each raft entry as an individual record, dragonboat
// batches entries first, each persisted record can thus contain many raft
// entries. This idea is based on the observations that:
//  * entries are usually saved together
//  * entries are usually read and consumed together
// This idea helped to achieve better latency and throughput performance.
//
// We also compact redundent index/term values whenever possible. In such
// approach, rather than repeatedly storing the continously increamental
// index values and the identifical term values, we represent them in the
// way implemented in the compactBatchFields function below.
//
// To the maximum of our knowledge, dragonboat is the original inventor
// of the ideas above, they were publicly disclosed on github.com when
// dragnoboat made its first public release.
//
// To use/implement the above ideas in your software, please include the
// above copyright notice in your source file, dragonboat's Apache2 license
// also requires to include dragonboat's NOTICE file.
//

func getBatchID(index uint64) uint64 {
	return index / batchSize
}

func entriesSize(ents []pb.Entry) uint64 {
	sz := uint64(0)
	for _, e := range ents {
		sz += uint64(e.SizeUpperLimit())
	}
	return sz
}

func getBatchIDRange(low uint64, high uint64) (uint64, uint64) {
	lowBatchID := getBatchID(low)
	highBatchID := getBatchID(high)
	if high%batchSize == 0 {
		return lowBatchID, highBatchID
	}
	return lowBatchID, highBatchID + 1
}

func restoreBatchFields(eb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) <= 1 {
		panic("restore called on small batch")
	}
	if eb.Entries[len(eb.Entries)-1].Term == 0 {
		term := eb.Entries[0].Term
		idx := eb.Entries[0].Index
		for i := 1; i < len(eb.Entries); i++ {
			eb.Entries[i].Term = term
			eb.Entries[i].Index = idx + uint64(i)
		}
	}
	return eb
}

func compactBatchFields(eb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) <= 1 {
		panic("compact called on small batch")
	}
	expLastIdx := eb.Entries[0].Index + uint64(len(eb.Entries)-1)
	if eb.Entries[0].Term == eb.Entries[len(eb.Entries)-1].Term &&
		expLastIdx == eb.Entries[len(eb.Entries)-1].Index {
		for i := 1; i < len(eb.Entries); i++ {
			eb.Entries[i].Term = 0
			eb.Entries[i].Index = 0
		}
	}
	return eb
}

func getMergedFirstBatch(eb pb.EntryBatch, lb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) == 0 || len(lb.Entries) == 0 {
		panic("getMergedFirstBatch called on empty batch")
	}
	batchID := getBatchID(eb.Entries[0].Index)
	if batchID < getBatchID(lb.Entries[0].Index) {
		panic("eb batch < lb batch")
	}
	if batchID > getBatchID(lb.Entries[0].Index) {
		return eb
	}
	if eb.Entries[0].Index > lb.Entries[0].Index {
		if eb.Entries[0].Index <= lb.Entries[len(lb.Entries)-1].Index {
			firstIndex := eb.Entries[0].Index
			i := 0
			for ; i < len(lb.Entries); i++ {
				if lb.Entries[i].Index >= firstIndex {
					break
				}
			}
			lb.Entries = append(lb.Entries[:i], eb.Entries...)
		} else {
			lb.Entries = append(lb.Entries, eb.Entries...)
		}
		return lb
	}
	return eb
}
