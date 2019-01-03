// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval_test

import (
	"fmt"

	"github.com/biogo/store/interval"
)

// Merge an interval into the tree, replacing overlapping intervals, but retaining them as sub intervals.
func Merge(t *interval.Tree, ni Interval) {
	var (
		fi = true
		qi = &Interval{start: ni.start, end: ni.end}
		r  []interval.Interface
	)

	t.DoMatching(
		func(e interval.Interface) (done bool) {
			iv := e.(Interval)
			r = append(r, e)
			ni.Sub = append(ni.Sub, iv)

			// Flatten merge history.
			ni.Sub = append(ni.Sub, iv.Sub...)
			iv.Sub = nil

			if fi {
				ni.start = min(iv.start, ni.start)
				fi = false
			}
			ni.end = max(iv.end, ni.end)

			return
		},
		qi,
	)
	for _, d := range r {
		t.Delete(d, false)
	}
	t.Insert(ni, false)
}

func ExampleTree_DoMatching() {
	t := &interval.Tree{}

	var (
		i  int
		iv Interval
	)

	for i, iv = range ivs {
		iv.id = uintptr(i)
		err := t.Insert(iv, false)
		if err != nil {
			fmt.Println(err)
		}
	}
	i++

	Merge(t, Interval{start: -1, end: 4, id: uintptr(i)})
	t.Do(func(e interval.Interface) (done bool) {
		fmt.Printf("%s: %v\n", e, e.(Interval).Sub)
		return
	})

	// Output:
	// [-1,6)#10: [[0,2)#0 [1,6)#2 [1,3)#4 [2,4)#1 [3,4)#3]
	// [4,6)#5: []
	// [5,8)#6: []
	// [5,7)#8: []
	// [6,8)#7: []
	// [8,9)#9: []
}
