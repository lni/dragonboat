// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 Peter Mattis.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Package leaktest provides tools to detect leaked goroutines in tests.
// To use it, call "defer leaktest.AfterTest(t)()" at the beginning of each
// test that may use goroutines.
//
// original code is from
// https://github.com/cockroachdb/cockroach/blob/master/pkg/util/leaktest/leaktest.go
// https://github.com/petermattis/goid/blob/master/goid.go
//
// see licensing & copyright info above

package leaktest

import (
	"bytes"
	"log"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ExtractGID returns the GID of the goroutine.
// from https://github.com/petermattis/goid/blob/master/goid.go
func ExtractGID(s []byte) int64 {
	s = s[len("goroutine "):]
	s = s[:bytes.IndexByte(s, ' ')]
	gid, _ := strconv.ParseInt(string(s), 10, 64)
	return gid
}

// interestingGoroutines returns all goroutines we care about for the purpose
// of leak checking. It excludes testing or runtime ones.
func interestingGoroutines() map[int64]string {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	gs := make(map[int64]string)
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if strings.HasPrefix(stack, "testing.RunTests") {
			continue
		}

		if stack == "" ||
			// Below are the stacks ignored by the upstream leaktest code.
			strings.Contains(stack, "testing.Main(") ||
			strings.Contains(stack, "testing.tRunner(") ||
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "created by runtime.gc") ||
			strings.Contains(stack, "interestingGoroutines") ||
			strings.Contains(stack, "runtime.MHeap_Scavenger") ||
			strings.Contains(stack, "signal.signal_recv") ||
			strings.Contains(stack, "sigterm.handler") ||
			strings.Contains(stack, "runtime_mcall") ||
			strings.Contains(stack, "goroutine in C code") ||
			strings.Contains(stack, "runtime.CPUProfile") {
			continue
		}
		gs[ExtractGID([]byte(g))] = g
	}
	return gs
}

// AfterTest snapshots the currently-running goroutines and returns a
// function to be run at the end of tests to see whether any
// goroutines leaked.
func AfterTest(t testing.TB) func() {
	orig := interestingGoroutines()
	return func() {
		if t.Failed() {
			return
		}
		if r := recover(); r != nil {
			panic(r)
		}
		count := 0
		for {
			var leaked []string
			for id, stack := range interestingGoroutines() {
				if _, ok := orig[id]; !ok {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if count < 100 {
				count++
				time.Sleep(50 * time.Millisecond)
				continue
			}
			sort.Strings(leaked)
			for _, g := range leaked {
				t.Errorf("Leaked goroutine: %v", g)
			}
			return
		}
	}
}

// GetInterestedGoroutines returns a set of interested goroutines.
func GetInterestedGoroutines() map[int64]string {
	return interestingGoroutines()
}

// AssertNoGoroutineLeak checks and ensures that there is no extra goroutine
// other than the ones included in the init map.
func AssertNoGoroutineLeak(init map[int64]string) {
	count := 0
	for {
		var leaked []string
		for id, stack := range interestingGoroutines() {
			if _, ok := init[id]; !ok {
				leaked = append(leaked, stack)
			}
		}
		if len(leaked) == 0 {
			return
		}
		if count < 100 {
			count++
			time.Sleep(50 * time.Millisecond)
			continue
		}
		sort.Strings(leaked)
		for _, g := range leaked {
			log.Fatalf("Leaked goroutine: %v", g)
		}
		if len(leaked) > 0 {
			panic("goroutine leak identified")
		}
		return
	}
}
