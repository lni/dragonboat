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

package dragonboat

import (
	"sort"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3/raftpb"
)

const (
	maxSampleCount = 50000
)

type sample struct {
	mu        sync.Mutex
	sampled   bool
	startTime time.Time
	samples   []int64
}

func newSample() *sample {
	return &sample{samples: make([]int64, 0)}
}

func (s *sample) start() {
	if !s.sampled {
		return
	}
	s.startTime = time.Now()
}

func (s *sample) end() {
	if !s.sampled {
		return
	}
	s.addSample(s.startTime)
}

func (s *sample) record(start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addSample(start)
}

func (s *sample) addSample(start time.Time) {
	cost := time.Since(start).Nanoseconds() / 1000
	s.samples = append(s.samples, cost)
	if len(s.samples) >= maxSampleCount {
		s.samples = s.samples[1:]
	}
}

func (s *sample) median() int64 {
	return s.percentile(50.0)
}

func (s *sample) p999() int64 {
	return s.percentile(99.9)
}

func (s *sample) p99() int64 {
	return s.percentile(99.0)
}

func (s *sample) percentile(p float64) int64 {
	if len(s.samples) == 0 {
		return 0
	}
	sort.Slice(s.samples, func(i int, j int) bool {
		return s.samples[i] < s.samples[j]
	})
	index := (p / 100) * float64(len(s.samples))
	if index == float64(int64(index)) {
		i := int(index)
		return s.samples[i-1]
	} else if index > 1 {
		i := int(index)
		return (s.samples[i] + s.samples[i-1]) / 2
	}
	return 0
}

type profiler struct {
	ratio           int64
	sampleCount     int64
	iteration       int64
	commitIteration int64
	propose         *sample
	step            *sample
	save            *sample
	cs              *sample
	ec              *sample
	exec            *sample
}

func newProfiler(sampleRatio int64) *profiler {
	return &profiler{
		ratio:   sampleRatio,
		propose: newSample(),
		step:    newSample(),
		save:    newSample(),
		cs:      newSample(),
		ec:      newSample(),
		exec:    newSample(),
	}
}

func (s *profiler) newIteration() {
	s.iteration++
	if s.ratio > 0 && s.iteration%s.ratio == 0 {
		s.propose.sampled = true
		s.step.sampled = true
		s.save.sampled = true
		s.cs.sampled = true
		s.ec.sampled = true
		s.sampleCount++
	} else {
		s.propose.sampled = false
		s.step.sampled = false
		s.save.sampled = false
		s.cs.sampled = false
		s.ec.sampled = false
	}
}

func (s *profiler) newCommitIteration() {
	s.commitIteration++
	if s.ratio > 0 && s.commitIteration%s.ratio == 0 {
		s.exec.sampled = true
	} else {
		s.exec.sampled = false
	}
}

func (s *profiler) recordEntryCount(updates []raftpb.Update) {
	if !s.ec.sampled {
		return
	}
	c := int64(0)
	for _, ud := range updates {
		c += int64(len(ud.EntriesToSave))
	}
	s.ec.samples = append(s.ec.samples, c)
	if len(s.ec.samples) >= maxSampleCount {
		s.ec.samples = s.ec.samples[1:]
	}
}
