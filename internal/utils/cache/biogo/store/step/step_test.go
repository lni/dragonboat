// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package step

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"gopkg.in/check.v1"
)

// Tests
func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

type nilable int

func (n *nilable) Equal(e Equaler) bool {
	return n == e.(*nilable)
}

func (s *S) TestCreate(c *check.C) {
	_, err := New(0, 0, nil)
	c.Check(err, check.ErrorMatches, ErrZeroLength.Error())
	for _, vec := range []struct {
		start, end int
		zero       Equaler
	}{
		{1, 10, (*nilable)(nil)},
		{0, 10, (*nilable)(nil)},
		{-1, 100, (*nilable)(nil)},
		{-100, -10, (*nilable)(nil)},
		{1, 10, Int(0)},
		{0, 10, Int(0)},
		{-1, 100, Int(0)},
		{-100, -10, Int(0)},
	} {
		sv, err := New(vec.start, vec.end, vec.zero)
		c.Assert(err, check.Equals, nil)
		c.Check(sv.Start(), check.Equals, vec.start)
		c.Check(sv.End(), check.Equals, vec.end)
		c.Check(sv.Len(), check.Equals, vec.end-vec.start)
		c.Check(sv.Zero, check.DeepEquals, vec.zero)
		var at Equaler
		for i := vec.start; i < vec.end; i++ {
			at, err = sv.At(i)
			c.Check(at, check.DeepEquals, vec.zero)
			c.Check(err, check.Equals, nil)
		}
		_, err = sv.At(vec.start - 1)
		c.Check(err, check.ErrorMatches, ErrOutOfRange.Error())
		_, err = sv.At(vec.start - 1)
		c.Check(err, check.ErrorMatches, ErrOutOfRange.Error())
	}
}

func (s *S) TestSet_1(c *check.C) {
	for i, t := range []struct {
		start, end int
		zero       Equaler
		sets       []position
		expect     string
	}{
		{1, 10, Int(0),
			[]position{
				{1, Int(2)},
				{2, Int(3)},
				{3, Int(3)},
				{4, Int(3)},
				{5, Int(2)},
			},
			"[1:2 2:3 5:2 6:0 10:<nil>]",
		},
		{1, 10, Int(0),
			[]position{
				{3, Int(3)},
				{4, Int(3)},
				{1, Int(2)},
				{2, Int(3)},
				{5, Int(2)},
			},
			"[1:2 2:3 5:2 6:0 10:<nil>]",
		},
		{1, 10, Int(0),
			[]position{
				{3, Int(3)},
				{4, Int(3)},
				{5, Int(2)},
				{1, Int(2)},
				{2, Int(3)},
				{9, Int(2)},
			},
			"[1:2 2:3 5:2 6:0 9:2 10:<nil>]",
		},
		{1, 10, Float(0),
			[]position{
				{3, Float(math.NaN())},
				{4, Float(math.NaN())},
				{5, Float(2)},
				{1, Float(2)},
				{2, Float(math.NaN())},
				{9, Float(2)},
			},
			"[1:2 2:NaN 5:2 6:0 9:2 10:<nil>]",
		},
		{1, 10, Float(math.NaN()),
			[]position{
				{3, Float(3)},
				{4, Float(3)},
				{5, Float(2)},
				{1, Float(2)},
				{2, Float(3)},
				{9, Float(2)},
			},
			"[1:2 2:3 5:2 6:NaN 9:2 10:<nil>]",
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		c.Check(func() { sv.Set(t.start-1, nil) }, check.Panics, ErrOutOfRange)
		c.Check(func() { sv.Set(t.end, nil) }, check.Panics, ErrOutOfRange)
		for _, v := range t.sets {
			sv.Set(v.pos, v.val)
			c.Check(sv.min.pos, check.Equals, t.start)
			c.Check(sv.max.pos, check.Equals, t.end)
			c.Check(sv.Len(), check.Equals, t.end-t.start)
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
		sv.Relaxed = true
		sv.Set(t.start-1, sv.Zero)
		sv.Set(t.end, sv.Zero)
		c.Check(sv.Len(), check.Equals, t.end-t.start+2)
		for _, v := range t.sets {
			sv.Set(v.pos, t.zero)
		}
		sv.Set(t.start-1, t.zero)
		sv.Set(t.end, t.zero)
		c.Check(sv.t.Len(), check.Equals, 2)
		c.Check(sv.String(), check.Equals, fmt.Sprintf("[%d:%v %d:%v]", t.start-1, t.zero, t.end+1, nil))
	}
}

func (s *S) TestSet_2(c *check.C) {
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []position
		expect     string
		count      int
	}{
		{1, 2, 0,
			[]position{
				{1, Int(2)},
				{2, Int(3)},
				{3, Int(3)},
				{4, Int(3)},
				{5, Int(2)},
				{-1, Int(5)},
				{10, Int(23)},
			},
			"[-1:5 0:0 1:2 2:3 5:2 6:0 10:23 11:<nil>]",
			7,
		},
		{1, 10, 0,
			[]position{
				{0, Int(0)},
			},
			"[0:0 10:<nil>]",
			1,
		},
		{1, 10, 0,
			[]position{
				{-1, Int(0)},
			},
			"[-1:0 10:<nil>]",
			1,
		},
		{1, 10, 0,
			[]position{
				{11, Int(0)},
			},
			"[1:0 12:<nil>]",
			1,
		},
		{1, 10, 0,
			[]position{
				{2, Int(1)},
				{3, Int(1)},
				{4, Int(1)},
				{5, Int(1)},
				{6, Int(1)},
				{7, Int(1)},
				{8, Int(1)},
				{5, Int(1)},
			},
			"[1:0 2:1 9:0 10:<nil>]",
			3,
		},
		{1, 10, 0,
			[]position{
				{3, Int(1)},
				{2, Int(1)},
			},
			"[1:0 2:1 4:0 10:<nil>]",
			3,
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		sv.Relaxed = true
		for _, v := range t.sets {
			sv.Set(v.pos, v.val)
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
		c.Check(sv.Count(), check.Equals, t.count)
	}
}

func (s *S) TestSetRange_0(c *check.C) {
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []posRange
		expect     string
		count      int
	}{
		// Left overhang
		{1, 10, 0,
			[]posRange{
				{-2, 0, 1},
			},
			"[-2:1 0:0 10:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{-2, 0, 0},
			},
			"[-2:0 10:<nil>]",
			1,
		},
		{1, 10, 0,
			[]posRange{
				{-1, 1, 1},
			},
			"[-1:1 1:0 10:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{-1, 1, 0},
			},
			"[-1:0 10:<nil>]",
			1,
		},
		{1, 10, 0,
			[]posRange{
				{-1, 2, 1},
			},
			"[-1:1 2:0 10:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{-1, 2, 0},
			},
			"[-1:0 10:<nil>]",
			1,
		},

		// Right overhang
		{1, 10, 0,
			[]posRange{
				{11, 12, 1},
			},
			"[1:0 11:1 12:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{11, 12, 0},
			},
			"[1:0 12:<nil>]",
			1,
		},
		{1, 10, 0,
			[]posRange{
				{11, 13, 1},
			},
			"[1:0 11:1 13:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{11, 13, 0},
			},
			"[1:0 13:<nil>]",
			1,
		},
		{1, 10, 0,
			[]posRange{
				{1, 10, 1},
				{11, 13, 1},
			},
			"[1:1 10:0 11:1 13:<nil>]",
			3,
		},
		{1, 10, 0,
			[]posRange{
				{1, 10, 1},
				{11, 13, 0},
			},
			"[1:1 10:0 13:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{10, 11, 1},
			},
			"[1:0 10:1 11:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{10, 11, 0},
			},
			"[1:0 11:<nil>]",
			1,
		},
		{1, 10, 0,
			[]posRange{
				{9, 11, 1},
			},
			"[1:0 9:1 11:<nil>]",
			2,
		},
		{1, 10, 0,
			[]posRange{
				{9, 11, 0},
			},
			"[1:0 11:<nil>]",
			1,
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		sv.Relaxed = true
		c.Logf("subtest %d process", i)
		for _, v := range t.sets {
			in := fmt.Sprint(sv)
			sv.SetRange(v.start, v.end, v.val)
			c.Logf(" %s --%+v-> %s min:%+v max:%+v", in, v, sv, *sv.min, *sv.max)
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
		c.Check(sv.Count(), check.Equals, t.count)
	}
}

func (s *S) TestSetRange_1(c *check.C) {
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []posRange
		expect     string
		count      int
	}{
		{1, 10, 0,
			[]posRange{
				{1, 2, 2},
				{2, 3, 3},
				{3, 4, 3},
				{4, 5, 3},
				{5, 6, 2},
			},
			"[1:2 2:3 5:2 6:0 10:<nil>]",
			4,
		},
		{1, 10, 0,
			[]posRange{
				{3, 4, 3},
				{4, 5, 3},
				{1, 2, 2},
				{2, 3, 3},
				{5, 6, 2},
			},
			"[1:2 2:3 5:2 6:0 10:<nil>]",
			4,
		},
		{1, 10, 0,
			[]posRange{
				{3, 4, 3},
				{4, 5, 3},
				{5, 6, 2},
				{1, 2, 2},
				{2, 3, 3},
				{9, 10, 2},
			},
			"[1:2 2:3 5:2 6:0 9:2 10:<nil>]",
			5,
		},
		{1, 10, 0,
			[]posRange{
				{3, 6, 3},
				{4, 5, 1},
				{5, 7, 2},
				{1, 3, 2},
				{9, 10, 2},
			},
			"[1:2 3:3 4:1 5:2 7:0 9:2 10:<nil>]",
			6,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			"[1:3 3:0 4:1 5:0 7:2 8:0 9:4 10:<nil>]",
			7,
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		c.Check(func() { sv.SetRange(t.start-2, t.start, nil) }, check.Panics, ErrOutOfRange)
		c.Check(func() { sv.SetRange(t.end, t.end+2, nil) }, check.Panics, ErrOutOfRange)
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
			c.Check(sv.min.pos, check.Equals, t.start)
			c.Check(sv.max.pos, check.Equals, t.end)
			c.Check(sv.Len(), check.Equals, t.end-t.start)
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
		c.Check(sv.Count(), check.Equals, t.count)
		sv.Relaxed = true
		sv.SetRange(t.start-1, t.start, sv.Zero)
		sv.SetRange(t.end, t.end+1, sv.Zero)
		c.Check(sv.Len(), check.Equals, t.end-t.start+2)
		sv.SetRange(t.start-1, t.end+1, t.zero)
		c.Check(sv.t.Len(), check.Equals, 2)
		c.Check(sv.String(), check.Equals, fmt.Sprintf("[%d:%v %d:%v]", t.start-1, t.zero, t.end+1, nil))
	}
}

func (s *S) TestSetRange_2(c *check.C) {
	sv, _ := New(0, 1, nil)
	c.Check(func() { sv.SetRange(1, 0, nil) }, check.Panics, ErrInvertedRange)
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []posRange
		expect     string
	}{
		{1, 10, 0,
			[]posRange{
				{1, 2, 2},
				{2, 3, 3},
				{3, 4, 3},
				{4, 5, 3},
				{5, 6, 2},
				{-10, -1, 4},
				{23, 35, 10},
			},
			"[-10:4 -1:0 1:2 2:3 5:2 6:0 23:10 35:<nil>]",
		},
		{1, 2, 0,
			[]posRange{
				{1, 1, 2},
			},
			"[1:0 2:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{-10, 1, 0},
			},
			"[-10:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{-10, 1, 1},
			},
			"[-10:1 1:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{-10, 0, 1},
			},
			"[-10:1 0:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{-10, 0, 0},
			},
			"[-10:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{10, 20, 0},
			},
			"[1:0 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{10, 20, 1},
			},
			"[1:0 10:1 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{11, 20, 0},
			},
			"[1:0 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{11, 20, 1},
			},
			"[1:0 11:1 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{1, 10, 1},
				{11, 20, 1},
			},
			"[1:1 10:0 11:1 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 5, 1},
				{2, 5, 0},
			},
			"[1:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 6, 1},
				{2, 5, 0},
			},
			"[1:0 5:1 6:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 1},
				{5, 7, 2},
				{3, 5, 1},
			},
			"[1:1 5:2 7:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 1},
				{5, 7, 2},
				{3, 5, 2},
			},
			"[1:1 3:2 7:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 5, 1},
				{2, 6, 0},
			},
			"[1:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 6, 1},
				{2, 5, 0},
			},
			"[1:0 5:1 6:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 5, 1},
				{2, 5, 2},
			},
			"[1:0 2:2 5:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 5, 1},
				{3, 5, 2},
			},
			"[1:0 2:1 3:2 5:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{2, 5, 1},
				{3, 5, 0},
			},
			"[1:0 2:1 3:0 10:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{5, 20, 1},
			},
			"[1:0 5:1 20:<nil>]",
		},
		{1, 10, 0,
			[]posRange{
				{5, 10, 1},
			},
			"[1:0 5:1 10:<nil>]",
		},
		{5, 10, 0,
			[]posRange{
				{5, 10, 1},
				{1, 4, 1},
			},
			"[1:1 4:0 5:1 10:<nil>]",
		},
		{1, 4, 0,
			[]posRange{
				{1, 4, 1},
				{5, 10, 1},
			},
			"[1:1 4:0 5:1 10:<nil>]",
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		sv.Relaxed = true
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
	}
}

func (s *S) TestStepAt(c *check.C) {
	type posRange struct {
		start, end int
		val        Int
	}
	t := struct {
		start, end int
		zero       Int
		sets       []posRange
		expect     string
	}{1, 10, 0,
		[]posRange{
			{1, 3, 3},
			{4, 5, 1},
			{7, 8, 2},
			{9, 10, 4},
		},
		"[1:3 3:0 4:1 5:0 7:2 8:0 9:4 10:<nil>]",
	}

	sv, err := New(t.start, t.end, t.zero)
	c.Assert(err, check.Equals, nil)
	for _, v := range t.sets {
		sv.SetRange(v.start, v.end, v.val)
	}
	c.Check(sv.String(), check.Equals, t.expect)
	for i, v := range t.sets {
		for j := v.start; j < v.end; j++ {
			st, en, at, err := sv.StepAt(v.start)
			c.Check(err, check.Equals, nil)
			c.Check(at, check.DeepEquals, v.val)
			c.Check(st, check.Equals, v.start)
			c.Check(en, check.Equals, v.end)
		}
		st, en, at, err := sv.StepAt(v.end)
		if v.end < sv.End() {
			c.Check(err, check.Equals, nil)
			c.Check(at, check.DeepEquals, sv.Zero)
			c.Check(st, check.Equals, v.end)
			c.Check(en, check.Equals, t.sets[i+1].start)
		} else {
			c.Check(err, check.ErrorMatches, ErrOutOfRange.Error())
		}
	}
	_, _, _, err = sv.StepAt(t.start - 1)
	c.Check(err, check.ErrorMatches, ErrOutOfRange.Error())
}

func (s *S) TestDo(c *check.C) {
	var data interface{}
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		relaxed    bool
		sets       []posRange
		setup      func()
		fn         Operation
		expect     interface{}
	}{
		{1, 10, 0, false,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			func() { data = []Int(nil) },
			func(start, end int, vi Equaler) {
				sl := data.([]Int)
				v := vi.(Int)
				for i := start; i < end; i++ {
					sl = append(sl, v)
				}
				data = sl
			},
			[]Int{3, 3, 0, 1, 0, 0, 2, 0, 4},
		},
		{5, 10, 0, true,
			[]posRange{
				{5, 10, 1},
				{1, 7, 1},
			},
			func() { data = []Int(nil) },
			func(start, end int, vi Equaler) {
				sl := data.([]Int)
				v := vi.(Int)
				for i := start; i < end; i++ {
					sl = append(sl, v)
				}
				data = sl
			},
			[]Int{1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{5, 10, 0, true,
			[]posRange{
				{5, 10, 1},
				{3, 7, 1},
				{1, 3, 1},
			},
			func() { data = []Int(nil) },
			func(start, end int, vi Equaler) {
				sl := data.([]Int)
				v := vi.(Int)
				for i := start; i < end; i++ {
					sl = append(sl, v)
				}
				data = sl
			},
			[]Int{1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
	} {
		t.setup()
		sv, err := New(t.start, t.end, t.zero)
		sv.Relaxed = t.relaxed
		c.Assert(err, check.Equals, nil)
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}
		sv.Do(t.fn)
		c.Check(data, check.DeepEquals, t.expect, check.Commentf("subtest %d", i))
		c.Check(reflect.ValueOf(data).Len(), check.Equals, sv.Len())
	}
}

func (s *S) TestDoRange(c *check.C) {
	var data interface{}
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []posRange
		setup      func()
		fn         Operation
		from, to   int
		expect     interface{}
		err        error
	}{
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			func() { data = []Int(nil) },
			func(start, end int, vi Equaler) {
				sl := data.([]Int)
				v := vi.(Int)
				for i := start; i < end; i++ {
					sl = append(sl, v)
				}
				data = sl
			},
			2, 8,
			[]Int{3, 0, 1, 0, 0, 2},
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			func() { data = []Int(nil) },
			func(_, _ int, _ Equaler) {},
			-2, -1,
			[]Int(nil),
			ErrOutOfRange,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			func() { data = []Int(nil) },
			func(_, _ int, _ Equaler) {},
			10, 1,
			[]Int(nil),
			ErrInvertedRange,
		},
	} {
		t.setup()
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}
		c.Check(sv.DoRange(t.from, t.to, t.fn), check.DeepEquals, t.err)
		c.Check(data, check.DeepEquals, t.expect, check.Commentf("subtest %d", i))
		if t.from <= t.to && t.from < sv.End() && t.to > sv.Start() {
			c.Check(reflect.ValueOf(data).Len(), check.Equals, t.to-t.from)
		}
	}
}

func (s *S) TestApply(c *check.C) {
	type posRange struct {
		start, end int
		val        Equaler
	}
	for i, t := range []struct {
		start, end int
		zero       Equaler
		sets       []posRange
		mutate     Mutator
		expect     string
	}{
		{1, 10, Int(0),
			[]posRange{
				{1, 3, Int(3)},
				{4, 5, Int(1)},
				{7, 8, Int(2)},
				{9, 10, Int(4)},
			},
			IncInt,
			"[1:4 3:1 4:2 5:1 7:3 8:1 9:5 10:<nil>]",
		},
		{1, 10, Int(0),
			[]posRange{
				{1, 3, Int(3)},
				{4, 5, Int(1)},
				{7, 8, Int(2)},
				{9, 10, Int(4)},
			},
			DecInt,
			"[1:2 3:-1 4:0 5:-1 7:1 8:-1 9:3 10:<nil>]",
		},
		{1, 10, Float(0),
			[]posRange{
				{1, 3, Float(3)},
				{4, 5, Float(1)},
				{7, 8, Float(2)},
				{9, 10, Float(4)},
			},
			IncFloat,
			"[1:4 3:1 4:2 5:1 7:3 8:1 9:5 10:<nil>]",
		},
		{1, 10, Float(0),
			[]posRange{
				{1, 3, Float(3)},
				{4, 5, Float(1)},
				{7, 8, Float(2)},
				{9, 10, Float(4)},
			},
			DecFloat,
			"[1:2 3:-1 4:0 5:-1 7:1 8:-1 9:3 10:<nil>]",
		},
		{1, 10, Int(0),
			[]posRange{
				{1, 3, Int(3)},
				{4, 5, Int(1)},
				{7, 8, Int(2)},
				{9, 10, Int(4)},
			},
			func(_ Equaler) Equaler { return Int(0) },
			"[1:0 10:<nil>]",
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}
		sv.Apply(t.mutate)
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
	}
}

func (s *S) TestMutateRange(c *check.C) {
	type posRange struct {
		start, end int
		val        Int
	}
	for i, t := range []struct {
		start, end int
		zero       Int
		sets       []posRange
		mutate     Mutator
		from, to   int
		expect     string
		err        error
	}{
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			IncInt,
			2, 8,
			"[1:3 2:4 3:1 4:2 5:1 7:3 8:0 9:4 10:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{7, 8, 2},
				{9, 10, 4},
			},
			IncInt,
			4, 6,
			"[1:3 3:0 4:1 6:0 7:2 8:0 9:4 10:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{7, 8, 1},
				{9, 10, 4},
			},
			IncInt,
			4, 7,
			"[1:3 3:0 4:1 8:0 9:4 10:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			func(_ Equaler) Equaler { return Int(0) },
			2, 8,
			"[1:3 2:0 9:4 10:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 9, 2},
				{9, 10, 4},
			},
			func(_ Equaler) Equaler { return Int(0) },
			2, 8,
			"[1:3 2:0 8:2 9:4 10:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{7, 8, 1},
				{9, 10, 4},
			},
			IncInt,
			4, 8,
			"[1:3 3:0 4:1 7:2 8:0 9:4 10:<nil>]",
			nil,
		},
		{1, 20, 0,
			[]posRange{
				{5, 10, 1},
				{10, 15, 2},
				{15, 20, 3},
			},
			func(v Equaler) Equaler {
				if v.Equal(Int(3)) {
					return Int(1)
				}
				return v
			},
			8, 18,
			"[1:0 5:1 10:2 15:1 18:3 20:<nil>]",
			nil,
		},
		{1, 20, 0,
			[]posRange{
				{1, 6, 1},
				{6, 15, 2},
				{15, 20, 1},
			},
			func(v Equaler) Equaler {
				return Int(2)
			},
			4, 12,
			"[1:1 4:2 15:1 20:<nil>]",
			nil,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			IncInt,
			-1, 0,
			"[1:3 3:0 4:1 5:0 7:2 8:0 9:4 10:<nil>]",
			ErrOutOfRange,
		},
		{1, 10, 0,
			[]posRange{
				{1, 3, 3},
				{4, 5, 1},
				{7, 8, 2},
				{9, 10, 4},
			},
			IncInt,
			10, 1,
			"[1:3 3:0 4:1 5:0 7:2 8:0 9:4 10:<nil>]",
			ErrInvertedRange,
		},
	} {
		sv, err := New(t.start, t.end, t.zero)
		c.Assert(err, check.Equals, nil)
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}
		c.Check(sv.ApplyRange(t.from, t.to, t.mutate), check.DeepEquals, t.err)
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
	}
}

// pair is a [2]bool type satisfying the step.Equaler interface.
type pair [2]bool

// Equal returns whether p equals e. Equal assumes the underlying type of e is pair.
func (p pair) Equal(e Equaler) bool {
	return p == e.(pair)
}

func (s *S) TestMutateRangePartial(c *check.C) {
	type posRange struct {
		start, end int
		val        pair
	}
	for i, t := range []struct {
		start, end int
		zero       pair
		sets       []posRange
		mutate     Mutator
		from, to   int
		expect     string
		err        error
	}{
		{94, 301, pair{},
			[]posRange{
				{94, 120, pair{false, false}},
				{120, 134, pair{false, true}},
				{134, 301, pair{false, false}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = true
				return p
			},
			113, 130,
			"[94:[false false] 113:[false true] 134:[false false] 301:<nil>]",
			nil,
		},
		{253121, 253565, pair{},
			[]posRange{
				{253121, 253565, pair{false, true}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = true
				return p
			},
			253115, 253565,
			"[253115:[false true] 253565:<nil>]",
			nil,
		},
		{253121, 253565, pair{},
			[]posRange{
				{253121, 253565, pair{false, true}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = true
				return p
			},
			253121, 253575,
			"[253121:[false true] 253575:<nil>]",
			nil,
		},
		{253121, 253565, pair{},
			[]posRange{
				{253121, 253565, pair{false, true}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = true
				return p
			},
			253115, 253575,
			"[253115:[false true] 253575:<nil>]",
			nil,
		},
		{0, 13, pair{},
			[]posRange{
				{0, 1, pair{false, false}},
				{1, 2, pair{false, true}},
				{2, 4, pair{true, true}},
				{4, 6, pair{true, false}},
				{6, 7, pair{false, false}},
				{7, 9, pair{false, true}},
				{9, 13, pair{false, false}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = false
				return p
			},
			8, 11,
			"[0:[false false] 1:[false true] 2:[true true] 4:[true false] 6:[false false] 7:[false true] 8:[false false] 13:<nil>]",
			nil,
		},
		{0, 13, pair{},
			[]posRange{
				{0, 1, pair{false, false}},
				{1, 2, pair{false, true}},
				{2, 3, pair{true, false}},
				{3, 6, pair{true, false}},
				{6, 7, pair{false, true}},
				{7, 9, pair{true, true}},
				{9, 10, pair{true, false}},
				{10, 13, pair{false, false}},
			},
			func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = false
				return p
			},
			1, 5,
			"[0:[false false] 2:[true false] 6:[false true] 7:[true true] 9:[true false] 10:[false false] 13:<nil>]",
			nil,
		},
	} {
		sv, err := New(t.start, t.end, pair{})
		c.Assert(err, check.Equals, nil)
		sv.Relaxed = true
		for _, v := range t.sets {
			sv.SetRange(v.start, v.end, v.val)
		}

		c.Check(sv.ApplyRange(t.from, t.to, t.mutate), check.DeepEquals, t.err)

		var (
			last      Equaler
			failed    = false
			failedEnd int
		)
		sv.Do(func(start, end int, e Equaler) {
			if e == last {
				failed = true
				failedEnd = start
			}
			last = e
		})
		if failed {
			c.Errorf("setting pair[1]=true over [%d,%d) gives invalid vector near %d:\n%s",
				t.start, t.end, failedEnd, sv.String())
		}
		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
	}
}

func (s *S) TestMutatorSetting(c *check.C) {
	type posRange struct {
		start, end int
	}
	for i, t := range []struct {
		sets   []posRange
		max    int
		expect string
		err    error
	}{
		{
			[]posRange{
				{30, 70},
				{10, 50},
			},
			70,
			"[10:[false true] 70:<nil>]",
			nil,
		},
		{
			[]posRange{
				{10, 50},
				{30, 70},
			},
			70,
			"[10:[false true] 70:<nil>]",
			nil,
		},
		{
			[]posRange{
				{30, 50},
				{10, 70},
			},
			70,
			"[10:[false true] 70:<nil>]",
			nil,
		},
		{
			[]posRange{
				{10, 70},
				{30, 50},
			},
			70,
			"[10:[false true] 70:<nil>]",
			nil,
		},
	} {
		sv, err := New(t.sets[0].start, t.sets[0].end, pair{})
		c.Assert(err, check.Equals, nil)
		sv.Relaxed = true
		av := newVector(t.sets[0].start, t.sets[0].end, t.max, pair{})
		for _, v := range t.sets {
			m := func(e Equaler) Equaler {
				p := e.(pair)
				p[1] = true
				return p
			}
			c.Check(sv.ApplyRange(v.start, v.end, m), check.DeepEquals, t.err)
			av.applyRange(v.start, v.end, m)
		}

		c.Check(sv.String(), check.Equals, t.expect, check.Commentf("subtest %d", i))
		c.Check(av.aggreesWith(sv), check.Equals, true)
	}
}

type vector struct {
	min, max int
	data     []Equaler
}

func newVector(start, end, cap int, zero Equaler) *vector {
	data := make([]Equaler, cap)
	for i := range data {
		data[i] = zero
	}
	return &vector{
		min:  start,
		max:  end,
		data: data,
	}
}

func (v *vector) setRange(start, end int, e Equaler) {
	if start == end {
		return
	}
	if start < v.min {
		v.min = start
	}
	if end > v.max {
		v.max = end
	}
	for i := start; i < end; i++ {
		v.data[i] = e
	}
}

func (v *vector) applyRange(start, end int, m Mutator) {
	if start == end {
		return
	}
	if start < v.min {
		v.min = start
	}
	if end > v.max {
		v.max = end
	}
	for i := start; i < end; i++ {
		v.data[i] = m(v.data[i])
	}
}

func (v *vector) aggreesWith(sv *Vector) bool {
	if v.min != sv.Start() || v.max != sv.End() {
		return false
	}
	ok := true
	sv.Do(func(start, end int, e Equaler) {
		for _, ve := range v.data[start:end] {
			if e != ve {
				ok = false
			}
		}
	})
	return ok
}

func (v *vector) String() string {
	var (
		buf  bytes.Buffer
		last Equaler
	)
	fmt.Fprint(&buf, "[")
	for i := v.min; i < v.max; i++ {
		if v.data[i] != last {
			fmt.Fprintf(&buf, "%d:%v ", i, v.data[i])
		}
		last = v.data[i]
	}
	fmt.Fprintf(&buf, "%d:<nil>]", v.max)
	return buf.String()
}

func (s *S) TestMutateRangePartialFuzzing(c *check.C) {
	rand.Seed(1)
	sv, err := New(0, 1, pair{})
	c.Assert(err, check.Equals, nil)
	sv.Relaxed = true
	v := newVector(0, 1, 15, pair{})
	var prev, prevArray string
	for i := 0; i < 100000; i++ {
		s := rand.Intn(10)
		l := rand.Intn(5)
		j := rand.Intn(2)
		f := rand.Intn(2) < 1
		c.Check(sv.ApplyRange(s, s+l, func(e Equaler) Equaler {
			p := e.(pair)
			p[j] = f
			return p
		}), check.DeepEquals, nil)
		v.applyRange(s, s+l, func(e Equaler) Equaler {
			p := e.(pair)
			p[j] = f
			return p
		})
		now := sv.String()
		array := v.String()
		c.Assert(sv.min, check.DeepEquals, sv.t.Min(),
			check.Commentf("invalid tree after iteration %d: set [%d,%d) pair[%d]=%t:\nwas: %v\nis:  %s", i, s, s+l, j, f, prev, now),
		)
		c.Assert(sv.max, check.DeepEquals, sv.t.Max(),
			check.Commentf("invalid tree after iteration %d: set [%d,%d) pair[%d]=%t:\nwas: %v\nis:  %s", i, s, s+l, j, f, prev, now),
		)
		c.Assert(v.aggreesWith(sv), check.Equals, true,
			check.Commentf("vector disagreement after iteration %d: set [%d,%d) pair[%d]=%t:\nwas:   %s\narray: %v\nwas:   %s\nstep:  %s",
				i, s, s+l, j, f, prevArray, array, prev, now),
		)
		var last Equaler
		sv.Do(func(start, end int, e Equaler) {
			if e == last {
				c.Fatalf("iteration %d: setting pair[%d]=%t over [%d,%d) gives invalid vector near %d:\nwas: %s\nis:  %s\nwant:%s",
					i, j, f, s, s+l, start, prev, now, array)
			}
			last = e
		})
		prev = now
		prevArray = array
	}
}

func (s *S) TestSetRangeFuzzing(c *check.C) {
	rand.Seed(2)
	sv, err := New(0, 1, pair{})
	c.Assert(err, check.Equals, nil)
	sv.Relaxed = true
	v := newVector(0, 1, 15, pair{})
	var prev, prevArray string
	for i := 0; i < 100000; i++ {
		s := rand.Intn(10)
		l := rand.Intn(5)
		f := pair{rand.Intn(2) < 1, rand.Intn(2) < 1}
		sv.SetRange(s, s+l, f)
		v.setRange(s, s+l, f)
		now := sv.String()
		array := v.String()
		c.Assert(sv.min, check.DeepEquals, sv.t.Min(),
			check.Commentf("invalid tree after iteration %d: set [%d,%d) to %#v:\nwas: %v\nis:  %s", i, s, s+l, f, prev, now),
		)
		c.Assert(sv.max, check.DeepEquals, sv.t.Max(),
			check.Commentf("invalid tree after iteration %d: set [%d,%d) to %#v:\nwas: %v\nis:  %s", i, s, s+l, f, prev, now),
		)
		c.Assert(v.aggreesWith(sv), check.Equals, true,
			check.Commentf("vector disagreement after iteration %d: set [%d,%d) to %#v:\nwas:   %s\narray: %v\nwas:   %s\nstep:  %s",
				i, s, s+l, f, prevArray, array, prev, now),
		)
		var last Equaler
		sv.Do(func(start, end int, e Equaler) {
			if e == last {
				c.Fatalf("iteration %d: setting pair=%#v over [%d,%d) gives invalid vector near %d:\nwas: %s\nis:  %s\nwant:%s",
					i, f, s, s+l, start, prev, now, array)
			}
			last = e
		})
		prev = now
		prevArray = array
	}
}

func (s *S) TestAgreementFuzzing(c *check.C) {
	rand.Seed(1)
	mutV, err := New(0, 1, pair{})
	c.Assert(err, check.Equals, nil)
	mutV.Relaxed = true
	setV, err := New(0, 1, pair{})
	c.Assert(err, check.Equals, nil)
	setV.Relaxed = true
	var (
		prevSet, prevMut string
		setLen           int
	)
	// Set up agreeing representations of intervals.
	for i := 0; i < 100000; i++ {
		s := rand.Intn(1000)
		l := rand.Intn(50)
		setV.SetRange(s, s+l, pair{true, false})
		c.Assert(mutV.ApplyRange(s, s+l, func(e Equaler) Equaler {
			p := e.(pair)
			p[0] = true
			return p
		}), check.Equals, nil)

		setNow := setV.String()
		mutNow := mutV.String()
		var mutLen int
		setLen = 0
		setV.Do(func(start, end int, e Equaler) {
			p := e.(pair)
			if p[0] {
				setLen += end - start
			}
		})
		mutV.Do(func(start, end int, e Equaler) {
			p := e.(pair)
			if p[0] {
				mutLen += end - start
			}
		})
		if setLen != mutLen {
			c.Fatalf("length disagreement after iteration %d: %d != %d\nset was: %s\nmut was: %s\nset now: %s\nmut now: %s",
				i, setLen, mutLen, prevSet, prevMut, setNow, mutNow)
		}

		prevSet = setNow
		prevMut = mutNow
	}

	// Mutate the other element of steps in the mutating vector, checking for changes
	// in position 0 of the steps.
	for i := 0; i < 100000; i++ {
		s := rand.Intn(1000)
		l := rand.Intn(50)
		c.Assert(mutV.ApplyRange(s, s+l, func(e Equaler) Equaler {
			p := e.(pair)
			p[1] = rand.Intn(2) < 1
			return p
		}), check.Equals, nil)

		mutNow := mutV.String()
		var mutLen int
		mutV.Do(func(start, end int, e Equaler) {
			p := e.(pair)
			if p[0] {
				mutLen += end - start
			}
		})
		if setLen != mutLen {
			c.Fatalf("length disagreement after iteration %d: %d != %d\nmut was: %s\nmut now: %s",
				i, setLen, mutLen, prevMut, mutNow)
		}

		prevMut = mutNow
	}
}

// Benchmarks

func applyRange(b *testing.B, coverage float64) {
	b.StopTimer()
	var (
		length = 100
		start  = 0
		end    = int(float64(b.N)/coverage) / length
		zero   = Int(0)
		pool   = make([]int, b.N)
	)
	if end == 0 {
		return
	}
	sv, _ := New(start, end, zero)
	for i := 0; i < b.N; i++ {
		pool[i] = rand.Intn(end)
	}
	b.StartTimer()
	for _, r := range pool {
		sv.ApplyRange(r, r+length, IncInt)
	}
}

func BenchmarkApplyRangeXDense(b *testing.B) {
	applyRange(b, 1000)
}
func BenchmarkApplyRangeVDense(b *testing.B) {
	applyRange(b, 100)
}
func BenchmarkApplyRangeDense(b *testing.B) {
	applyRange(b, 10)
}
func BenchmarkApplyRangeUnity(b *testing.B) {
	applyRange(b, 1)
}
func BenchmarkApplyRangeSparse(b *testing.B) {
	applyRange(b, 0.1)
}
func BenchmarkApplyRangeVSparse(b *testing.B) {
	applyRange(b, 0.01)
}
func BenchmarkApplyRangeXSparse(b *testing.B) {
	applyRange(b, 0.001)
}

func atFunc(b *testing.B, coverage float64) {
	b.StopTimer()
	var (
		length = 100
		start  = 0
		end    = int(float64(b.N)/coverage) / length
		zero   = Int(0)
	)
	if end == 0 {
		return
	}
	sv, _ := New(start, end, zero)
	for i := 0; i < b.N; i++ {
		r := rand.Intn(end)
		sv.ApplyRange(r, r+length, IncInt)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := sv.At(rand.Intn(end))
		if err != nil {
			panic("cannot reach")
		}
	}
}

func BenchmarkAtXDense(b *testing.B) {
	atFunc(b, 1000)
}
func BenchmarkAtVDense(b *testing.B) {
	atFunc(b, 100)
}
func BenchmarkAtDense(b *testing.B) {
	atFunc(b, 10)
}
func BenchmarkAtUnity(b *testing.B) {
	atFunc(b, 1)
}
func BenchmarkAtSparse(b *testing.B) {
	atFunc(b, 0.1)
}
func BenchmarkAtVSparse(b *testing.B) {
	atFunc(b, 0.01)
}
func BenchmarkAtXSparse(b *testing.B) {
	atFunc(b, 0.001)
}
