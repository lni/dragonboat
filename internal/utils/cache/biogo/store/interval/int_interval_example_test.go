/// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval_test

import (
	"fmt"

	"github.com/biogo/store/interval"
)

// Integer-specific intervals
type IntInterval struct {
	Start, End int
	UID        uintptr
	Payload    interface{}
}

func (i IntInterval) Overlap(b interval.IntRange) bool {
	// Half-open interval indexing.
	return i.End > b.Start && i.Start < b.End
}
func (i IntInterval) ID() uintptr              { return i.UID }
func (i IntInterval) Range() interval.IntRange { return interval.IntRange{i.Start, i.End} }
func (i IntInterval) String() string           { return fmt.Sprintf("[%d,%d)#%d", i.Start, i.End, i.UID) }

var intIvs = []IntInterval{
	{Start: 0, End: 2},
	{Start: 2, End: 4},
	{Start: 1, End: 6},
	{Start: 3, End: 4},
	{Start: 1, End: 3},
	{Start: 4, End: 6},
	{Start: 5, End: 8},
	{Start: 6, End: 8},
	{Start: 5, End: 7},
	{Start: 8, End: 9},
}

func Example_2() {
	t := &interval.IntTree{}
	for i, iv := range intIvs {
		iv.UID = uintptr(i)
		err := t.Insert(iv, false)
		if err != nil {
			fmt.Println(err)
		}
	}

	fmt.Println("Integer-specific interval tree:")
	fmt.Println(t.Get(IntInterval{Start: 3, End: 6}))

	// Output:
	// Integer-specific interval tree:
	// [[1,6)#2 [2,4)#1 [3,4)#3 [4,6)#5 [5,8)#6 [5,7)#8]
}
