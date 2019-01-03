// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package landscape

import (
	"fmt"
	"testing"

	"gopkg.in/check.v1"

	"github.com/biogo/store/interval"
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

type iv struct {
	Start, End int
	UID        uintptr
}

func (i iv) Overlap(b interval.IntRange) bool { return i.End > b.Start && i.Start < b.End }
func (i iv) ID() uintptr                      { return i.UID }
func (i iv) Range() interval.IntRange         { return interval.IntRange{i.Start, i.End} }
func (i iv) String() string                   { return fmt.Sprintf("[%d,%d)", i.Start, i.End) }

type lr struct {
	t int
	l []int
}

var testData = []struct {
	ivs    []iv
	expect []lr
}{
	{
		ivs:    []iv(nil),
		expect: []lr(nil),
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2}},
			{4, []int{1}},
			{5, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
			{Start: 6, End: 10},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2}},
			{4, []int{1}},
			{5, []int{0}},

			{6, []int{0}},
			{7, []int{1}},
			{8, []int{2}},
			{9, []int{1}},
			{10, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
			{Start: 5, End: 9},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2}},
			{4, []int{1}},
			{5, []int{0, 0}},
			{6, []int{1}},
			{7, []int{2}},
			{8, []int{1}},
			{9, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
			{Start: 16, End: 20},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2}},
			{4, []int{1}},
			{5, []int{0}},

			{16, []int{0}},
			{17, []int{1}},
			{18, []int{2}},
			{19, []int{1}},
			{20, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
			{Start: 4, End: 17},
			{Start: 16, End: 20},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2}},
			{4, []int{1, 0}},
			{5, []int{1, 0}},
			{6, []int{2}},
			{7, []int{3}},
			{8, []int{4}},
			{9, []int{5}},
			{10, []int{6}},
			{11, []int{6}},
			{12, []int{5}},
			{13, []int{4}},
			{14, []int{3}},
			{15, []int{2}},
			{16, []int{1, 0}},
			{17, []int{1, 0}},
			{18, []int{2}},
			{19, []int{1}},
			{20, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 1, End: 5},
			{Start: 3, End: 18},
			{Start: 16, End: 20},
		},
		expect: []lr{
			{1, []int{0}},
			{2, []int{1}},
			{3, []int{2, 0}},
			{4, []int{1, 1}},
			{5, []int{2, 0}},
			{6, []int{3}},
			{7, []int{4}},
			{8, []int{5}},
			{9, []int{6}},
			{10, []int{7}},
			{11, []int{7}},
			{12, []int{6}},
			{13, []int{5}},
			{14, []int{4}},
			{15, []int{3}},
			{16, []int{2, 0}},
			{17, []int{1, 1}},
			{18, []int{2, 0}},
			{19, []int{1}},
			{20, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 0, End: 6},
			{Start: 1, End: 5},
		},
		expect: []lr{
			{0, []int{0}},
			{1, []int{1, 0}},
			{2, []int{2, 1}},
			{3, []int{3, 2}},
			{4, []int{2, 1}},
			{5, []int{1, 0}},
			{6, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 0, End: 10},
			{Start: 1, End: 5},
		},
		expect: []lr{
			{0, []int{0}},
			{1, []int{1, 0}},
			{2, []int{2, 1}},
			{3, []int{3, 2}},
			{4, []int{4, 1}},
			{5, []int{5, 0}},
			{6, []int{4}},
			{7, []int{3}},
			{8, []int{2}},
			{9, []int{1}},
			{10, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 0, End: 10},
			{Start: 1, End: 5},
			{Start: 1, End: 5},
		},
		expect: []lr{
			{0, []int{0}},
			{1, []int{1, 0, 0}},
			{2, []int{2, 1, 1}},
			{3, []int{3, 2, 2}},
			{4, []int{4, 1, 1}},
			{5, []int{5, 0, 0}},
			{6, []int{4}},
			{7, []int{3}},
			{8, []int{2}},
			{9, []int{1}},
			{10, []int{0}},
		},
	},
	{
		ivs: []iv{
			{Start: 5, End: 8},
			{Start: 6, End: 14},
			{Start: 0, End: 10},
		},
		expect: []lr{
			{0, []int{0}},
			{1, []int{1}},
			{2, []int{2}},
			{3, []int{3}},
			{4, []int{4}},
			{5, []int{5, 0}},
			{6, []int{4, 1, 0}},
			{7, []int{3, 1, 1}},
			{8, []int{2, 2, 0}},
			{9, []int{3, 1}},
			{10, []int{4, 0}},
			{11, []int{3}},
			{12, []int{2}},
			{13, []int{1}},
			{14, []int{0}},
		},
	},
}

func (s *S) TestDescribeTree(c *check.C) {
	for i, t := range testData {
		var (
			it interval.IntTree
			r  []lr
		)
		for id, e := range t.ivs {
			e.UID = uintptr(id)
			err := it.Insert(e, false)
			c.Assert(err, check.Equals, nil)
		}
		DescribeTree(&it, func(pos int, l []int) {
			if len(l) > 0 {
				r = append(r, lr{pos, append([]int(nil), l...)})
			}
		})
		c.Check(r, check.DeepEquals, t.expect, check.Commentf("Test %d: %v", i, t.ivs))
	}
}

type ivs []iv

func (s ivs) Len() int                     { return len(s) }
func (s ivs) Less(i, j int) bool           { return s[i].Start < s[j].Start }
func (s ivs) Swap(i, j int)                { s[i], s[j] = s[j], s[i] }
func (s ivs) Item(i int) interval.IntRange { return s[i].Range() }

func (s *S) TestDescribe(c *check.C) {
	for i, t := range testData {
		var (
			data = append(ivs(nil), t.ivs...)
			r    []lr
		)
		Describe(data, func(pos int, l []int) {
			if len(l) > 0 {
				r = append(r, lr{pos, append([]int(nil), l...)})
			}
		})
		c.Check(r, check.DeepEquals, t.expect, check.Commentf("Test %d: %v", i, t.ivs))
	}
}
