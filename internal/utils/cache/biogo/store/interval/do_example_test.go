// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval_test

import (
	"fmt"

	"github.com/biogo/store/interval"
)

func min(a, b Int) Int {
	if a < b {
		return a
	}
	return b
}

func max(a, b Int) Int {
	if a > b {
		return a
	}
	return b
}

// Flatten all overlapping intervals, storing originals as sub-intervals.
func Flatten(t *interval.Tree) {
	var (
		fi  = true
		ti  []Interval
		mid uintptr
	)

	t.Do(
		func(e interval.Interface) (done bool) {
			iv := e.(Interval)
			if fi || iv.start >= ti[len(ti)-1].end {
				ti = append(ti, Interval{
					start: iv.start,
					end:   iv.end,
				})
				fi = false
			} else {
				ti[len(ti)-1].end = max(ti[len(ti)-1].end, iv.end)
			}
			ti[len(ti)-1].Sub = append(ti[len(ti)-1].Sub, iv)
			if iv.id > mid {
				mid = iv.id
			}

			return
		},
	)

	mid++
	t.Root, t.Count = nil, 0
	for i, iv := range ti {
		iv.id = uintptr(i) + mid
		t.Insert(iv, true)
	}
	t.AdjustRanges()
}

func ExampleTree_Do() {
	t := &interval.Tree{}
	for i, iv := range ivs {
		iv.id = uintptr(i)
		err := t.Insert(iv, false)
		if err != nil {
			fmt.Println(err)
		}
	}

	Flatten(t)
	t.Do(func(e interval.Interface) (done bool) { fmt.Printf("%s: %v\n", e, e.(Interval).Sub); return })

	// Output:
	// [0,8)#10: [[0,2)#0 [1,6)#2 [1,3)#4 [2,4)#1 [3,4)#3 [4,6)#5 [5,8)#6 [5,7)#8 [6,8)#7]
	// [8,9)#11: [[8,9)#9]
}
