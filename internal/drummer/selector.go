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

package drummer

import (
	"github.com/lni/goutils/random"
)

type randomSelector struct {
	filter       nodeHostFilter
	randomSource random.Source
}

func newRandomSelector(clusterID uint64, currentTick uint64,
	allowedTickGap uint64, randomSource random.Source) *randomSelector {
	if randomSource == nil {
		panic("random source is nil")
	}
	return &randomSelector{
		filter:       newDrummerFilter(clusterID, currentTick, allowedTickGap),
		randomSource: randomSource,
	}
}

func newRandomRegionSelector(region string, clusterID uint64,
	currentTick uint64, allowedTickGap uint64,
	randomSource random.Source) *randomSelector {
	if randomSource == nil {
		panic("random source is nil")
	}
	return &randomSelector{
		filter: newDrummerRegionFilter(region,
			clusterID, currentTick, allowedTickGap),
		randomSource: randomSource,
	}
}

func (rs *randomSelector) findSuitableNodeHost(input []*nodeHostSpec,
	count int) []*nodeHostSpec {
	filtered := rs.filter.filter(input)
	if len(filtered) < count {
		return []*nodeHostSpec{}
	}
	selected := make([]int, 0)
	contains := func(input []int, v int) bool {
		for _, x := range input {
			if v == x {
				return true
			}
		}
		return false
	}
	for len(selected) != count {
		rv := rs.randomSource.Int() % len(filtered)
		if !contains(selected, rv) {
			selected = append(selected, rv)
		}
	}
	result := make([]*nodeHostSpec, 0)
	for _, idx := range selected {
		result = append(result, filtered[idx])
	}
	return result
}
