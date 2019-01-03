// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval_test

import (
	"fmt"

	"github.com/biogo/store/interval"
)

// Generic intervals
type Int int

func (c Int) Compare(b interval.Comparable) int {
	return int(c - b.(Int))
}

type Interval struct {
	start, end Int
	id         uintptr
	Sub        []Interval
	Payload    interface{}
}

func (i Interval) Overlap(b interval.Range) bool {
	var start, end Int
	switch bc := b.(type) {
	case Interval:
		start, end = bc.start, bc.end
	case *Mutable:
		start, end = bc.start, bc.end
	default:
		panic("unknown type")
	}

	// Half-open interval indexing.
	return i.end > start && i.start < end
}
func (i Interval) ID() uintptr                  { return i.id }
func (i Interval) Start() interval.Comparable   { return i.start }
func (i Interval) End() interval.Comparable     { return i.end }
func (i Interval) NewMutable() interval.Mutable { return &Mutable{i.start, i.end} }
func (i Interval) String() string               { return fmt.Sprintf("[%d,%d)#%d", i.start, i.end, i.id) }

type Mutable struct{ start, end Int }

func (m *Mutable) Start() interval.Comparable     { return m.start }
func (m *Mutable) End() interval.Comparable       { return m.end }
func (m *Mutable) SetStart(c interval.Comparable) { m.start = c.(Int) }
func (m *Mutable) SetEnd(c interval.Comparable)   { m.end = c.(Int) }

var ivs = []Interval{
	{start: 0, end: 2},
	{start: 2, end: 4},
	{start: 1, end: 6},
	{start: 3, end: 4},
	{start: 1, end: 3},
	{start: 4, end: 6},
	{start: 5, end: 8},
	{start: 6, end: 8},
	{start: 5, end: 7},
	{start: 8, end: 9},
}

func Example_1() {
	t := &interval.Tree{}
	for i, iv := range ivs {
		iv.id = uintptr(i)
		err := t.Insert(iv, false)
		if err != nil {
			fmt.Println(err)
		}
	}

	fmt.Println("Generic interval tree:")
	fmt.Println(t.Get(Interval{start: 3, end: 6}))

	// Output:
	// Generic interval tree:
	// [[1,6)#2 [2,4)#1 [3,4)#3 [4,6)#5 [5,8)#6 [5,7)#8]
}
