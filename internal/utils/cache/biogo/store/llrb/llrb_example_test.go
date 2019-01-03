// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package llrb_test

import (
	"fmt"

	"github.com/biogo/store/llrb"
)

type (
	Int           int
	IntUpperBound int
)

func (c Int) Compare(b llrb.Comparable) int {
	switch i := b.(type) {
	case Int:
		return int(c - i)
	case IntUpperBound:
		return int(c) - int(i)
	}
	panic("unknown type")
}

func (c IntUpperBound) Compare(b llrb.Comparable) int {
	var d int
	switch i := b.(type) {
	case Int:
		d = int(c) - int(i)
	case IntUpperBound:
		d = int(c - i)
	}
	if d == 0 {
		return 1
	}
	return d
}

func Example() {
	values := []int{0, 1, 2, 3, 4, 2, 3, 5, 5, 65, 32, 3, 23}

	// Insert using a type that reports equality:
	{
		t := &llrb.Tree{}
		for _, v := range values {
			t.Insert(Int(v)) // Insert with replacement.
		}

		results := []int(nil)
		// More efficiently retrieved using Get(Int(3))...
		t.DoMatching(func(c llrb.Comparable) (done bool) {
			results = append(results, int(c.(Int)))
			return
		}, Int(3))

		fmt.Println("With replacement:   ", results)
	}

	// Insert using a type that does not report equality:
	{
		t := &llrb.Tree{}
		for _, v := range values {
			t.Insert(IntUpperBound(v)) // Insert without replacement.
		}

		results := []int(nil)
		t.DoMatching(func(c llrb.Comparable) (done bool) {
			results = append(results, int(c.(IntUpperBound)))
			return
		}, Int(3))

		fmt.Println("Without replacement:", results)
	}

	// Output:
	// With replacement:    [3]
	// Without replacement: [3 3 3]
}
