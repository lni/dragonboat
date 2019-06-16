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

type nodeHostFilter interface {
	filter([]*nodeHostSpec) []*nodeHostSpec
}

type basicFilter struct {
	clusterID uint64
}

func newBasicFilter(clusterID uint64) *basicFilter {
	return &basicFilter{clusterID: clusterID}
}

func (f *basicFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	result := make([]*nodeHostSpec, 0)
	for _, v := range input {
		if _, ok := v.Clusters[f.clusterID]; !ok {
			result = append(result, v)
		}
	}
	return result
}

type liveFilter struct {
	currentTick uint64
	gap         uint64
}

func newLiveFilter(ct uint64, gap uint64) *liveFilter {
	return &liveFilter{currentTick: ct, gap: gap}
}

func (f *liveFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	result := make([]*nodeHostSpec, 0)
	for _, v := range input {
		if f.currentTick-v.Tick < f.gap {
			result = append(result, v)
		}
	}
	return result
}

type regionFilter struct {
	region string
}

func newRegionFilter(region string) *regionFilter {
	return &regionFilter{region: region}
}

func (rf *regionFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	result := make([]*nodeHostSpec, 0)
	for _, v := range input {
		if rf.region == v.Region {
			result = append(result, v)
		}
	}
	return result
}

type combinedFilter struct {
	filters []nodeHostFilter
}

func newCombinedFilter(filters ...nodeHostFilter) *combinedFilter {
	f := make([]nodeHostFilter, 0)
	f = append(f, filters...)
	return &combinedFilter{filters: f}
}

func (f *combinedFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	list := input
	for _, currentFilter := range f.filters {
		list = currentFilter.filter(list)
	}
	return list
}

type defaultFilter struct {
	cf *combinedFilter
}

func newDrummerFilter(clusterID uint64, currentTick uint64,
	allowedTickGap uint64) *defaultFilter {
	lf := newLiveFilter(currentTick, allowedTickGap)
	bf := newBasicFilter(clusterID)
	cf := newCombinedFilter(lf, bf)
	return &defaultFilter{cf: cf}
}

func (f *defaultFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	return f.cf.filter(input)
}

type defaultRegionFilter struct {
	cf *combinedFilter
}

func newDrummerRegionFilter(region string, clusterID uint64, currentTick uint64,
	allowedTickGap uint64) *defaultRegionFilter {
	lf := newLiveFilter(currentTick, allowedTickGap)
	bf := newBasicFilter(clusterID)
	rf := newRegionFilter(region)
	cf := newCombinedFilter(lf, bf, rf)
	return &defaultRegionFilter{cf: cf}
}

func (f *defaultRegionFilter) filter(input []*nodeHostSpec) []*nodeHostSpec {
	return f.cf.filter(input)
}
