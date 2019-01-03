// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package landscape implements persistence landscape calculation for a set of intervals.
//
// The detail of this representation are described in 'Statistical Topology Using Persistence
// Landscapes.' P. Bubenik http://arxiv.org/pdf/1207.6437v1.pdf
package landscape

import (
	"container/heap"
	"sort"

	"github.com/biogo/store/interval"
)

type endHeap []interval.IntInterface

func (e endHeap) Len() int              { return len(e) }
func (e endHeap) Less(i, j int) bool    { return e[i].Range().End < e[j].Range().End }
func (e endHeap) Swap(i, j int)         { e[i], e[j] = e[j], e[i] }
func (e *endHeap) Push(x interface{})   { *e = append(*e, x.(interval.IntInterface)) }
func (e *endHeap) Pop() (i interface{}) { i, *e = (*e)[len(*e)-1], (*e)[:len(*e)-1]; return i }

type endRangeHeap []interval.IntRange

func (e endRangeHeap) Len() int              { return len(e) }
func (e endRangeHeap) Less(i, j int) bool    { return e[i].End < e[j].End }
func (e endRangeHeap) Swap(i, j int)         { e[i], e[j] = e[j], e[i] }
func (e *endRangeHeap) Push(x interface{})   { *e = append(*e, x.(interval.IntRange)) }
func (e *endRangeHeap) Pop() (i interface{}) { i, *e = (*e)[len(*e)-1], (*e)[:len(*e)-1]; return i }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func reverse(v []int) {
	for i, j := 0, len(v)-1; i < j; i, j = i+1, j-1 {
		v[i], v[j] = v[j], v[i]
	}
}

// DescribeTree calculates the persistence landscape functions λₖ for the interval
// data in the provided interval tree. fn is called for each position t of the span
// of the interval data with the values for t and the k λ functions at t.
// Explicit zero values for a λₖ(t) are included only at the end points of intervals
// in the span. Note that intervals stored in the tree must have unique id values
// within the tree.
func DescribeTree(it *interval.IntTree, fn func(t int, λₜ []int)) {
	if it == nil || it.Len() == 0 {
		return
	}
	var (
		h    endHeap
		t    = it.Root.Range.Start
		end  = it.Root.Range.End
		last = it.Max().ID()
		l    []int
	)
	it.Do(func(iv interval.IntInterface) (done bool) {
		if s := iv.Range().Start; s >= t || iv.ID() == last {
			if iv.ID() == last {
				heap.Push(&h, iv)
				s, iv = iv.Range().End, nil
			}
			for ; t < s; t++ {
				for len(h) > 0 && h[0].Range().End <= t {
					heap.Pop(&h)
					l = append(l, 0)
				}
				for _, iv := range h {
					r := iv.Range()
					if r.Start == t {
						l = append(l, 0)
					}
					if v := max(0, min(t-r.Start, r.End-t)); v > 0 {
						l = append(l, v)
					}
				}
				sort.Ints(l)
				reverse(l)
				fn(t, l)
				l = l[:0]
			}
		}
		if iv != nil {
			heap.Push(&h, iv)
		} else {
			for ; t <= end; t++ {
				for len(h) > 0 && h[0].Range().End <= t {
					heap.Pop(&h)
					l = append(l, 0)
				}
				for _, iv := range h {
					r := iv.Range()
					if r.Start == t {
						l = append(l, 0)
					}
					if v := max(0, min(t-r.Start, r.End-t)); v > 0 {
						l = append(l, v)
					}
				}
				sort.Ints(l)
				reverse(l)
				fn(t, l)
				l = l[:0]
			}
		}
		return
	})
}

// The landscape Interface allows arbitrary collections to be described as a
// persistence landscape.
type Interface interface {
	sort.Interface
	Item(int) interval.IntRange
}

// Describe calculates the persistence landscape functions λₖ for the interval
// data in the provided Interface. fn is called for each position t of the span
// of the interval data with the values for t and the k λ functions at t.
// Explicit zero values for a λₖ(t) are included only at the end points of intervals
// in the span.
func Describe(data Interface, fn func(t int, λₜ []int)) {
	if data == nil || data.Len() == 0 {
		return
	}
	sort.Sort(data)
	var (
		h   endRangeHeap
		iv  = data.Item(0)
		t   = iv.Start
		end = iv.End
		l   []int
	)
	for i := 0; i < data.Len(); i++ {
		iv = data.Item(i)
		if iv.End > end {
			end = iv.End
		}
		if s := iv.Start; s >= t || i == data.Len()-1 {
			if i == data.Len()-1 {
				heap.Push(&h, iv)
				s = iv.End
			}
			for ; t < s; t++ {
				for len(h) > 0 && h[0].End <= t {
					heap.Pop(&h)
					l = append(l, 0)
				}
				for _, iv := range h {
					if iv.Start == t {
						l = append(l, 0)
					}
					if v := max(0, min(t-iv.Start, iv.End-t)); v > 0 {
						l = append(l, v)
					}
				}
				sort.Ints(l)
				reverse(l)
				fn(t, l)
				l = l[:0]
			}
		}
		if i != data.Len()-1 {
			heap.Push(&h, iv)
		} else {
			for ; t <= end; t++ {
				for len(h) > 0 && h[0].End <= t {
					heap.Pop(&h)
					l = append(l, 0)
				}
				for _, iv := range h {
					if iv.Start == t {
						l = append(l, 0)
					}
					if v := max(0, min(t-iv.Start, iv.End-t)); v > 0 {
						l = append(l, v)
					}
				}
				sort.Ints(l)
				reverse(l)
				fn(t, l)
				l = l[:0]
			}
		}
	}
}
