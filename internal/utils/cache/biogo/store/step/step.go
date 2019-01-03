// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package step implements a step vector type.
//
// A step vector can be used to represent high volume data that would be
// efficiently stored by run-length encoding.
package step

import (
	"errors"
	"fmt"

	"github.com/biogo/store/llrb"
)

var (
	ErrOutOfRange    = errors.New("step: index out of range")
	ErrInvertedRange = errors.New("step: inverted range")
	ErrZeroLength    = errors.New("step: attempt to create zero length vector")
)

type (
	position struct {
		pos int
		val Equaler
	}
	lower int
	query int
	upper int
)

func (p *position) Compare(c llrb.Comparable) int {
	return p.pos - c.(*position).pos
}
func (q lower) Compare(c llrb.Comparable) (d int) {
	d = int(q) - c.(*position).pos
	if d == 0 {
		d = -1
	}
	return
}
func (q query) Compare(c llrb.Comparable) (d int) {
	switch c := c.(type) {
	case *position:
		d = int(q) - c.pos
	case query:
		d = int(q) - int(c)
	}
	return
}
func (q upper) Compare(c llrb.Comparable) (d int) {
	d = int(q) - c.(*position).pos
	if d == 0 {
		d = 1
	}
	return
}

// An Equaler is a type that can return whether it equals another Equaler.
type Equaler interface {
	Equal(Equaler) bool
}

// An Int is an int type satisfying the Equaler interface.
type Int int

// Equal returns whether i equals e. Equal assumes the underlying type of e is Int.
func (i Int) Equal(e Equaler) bool {
	return i == e.(Int)
}

// A Float is a float64 type satisfying the Equaler interface.
type Float float64

// Equal returns whether f equals e. For the purposes of the step package here, NaN == NaN
// evaluates to true. Equal assumes the underlying type of e is Float.
func (f Float) Equal(e Equaler) bool {
	ef := e.(Float)
	if f != f && ef != ef { // For our purposes NaN == NaN.
		return true
	}
	return f == ef
}

// A Vector is type that support the storage of array type data in a run-length
// encoding format.
type Vector struct {
	Zero     Equaler // Ground state for the step vector.
	Relaxed  bool    // If true, dynamic vector resize is allowed.
	t        llrb.Tree
	min, max *position
}

// New returns a new Vector with the extent defined by start and end,
// and the ground state defined by zero. The Vector's extent is mutable
// if the Relaxed field is set to true. If a zero length vector is requested
// an error is returned.
func New(start, end int, zero Equaler) (*Vector, error) {
	if start >= end {
		return nil, ErrZeroLength
	}
	v := &Vector{
		Zero: zero,
		min: &position{
			pos: start,
			val: zero,
		},
		max: &position{
			pos: end,
			val: nil,
		},
	}
	v.t.Insert(v.min)
	v.t.Insert(v.max)

	return v, nil
}

// Start returns the index of minimum position of the Vector.
func (v *Vector) Start() int { return v.min.pos }

// End returns the index of lowest position beyond the end of the Vector.
func (v *Vector) End() int { return v.max.pos }

// Len returns the length of the represented data array, that is the distance
// between the start and end of the vector.
func (v *Vector) Len() int { return v.End() - v.Start() }

// Count returns the number of steps represented in the vector.
func (v *Vector) Count() int { return v.t.Len() - 1 }

// At returns the value of the vector at position i. If i is outside the extent
// of the vector an error is returned.
func (v *Vector) At(i int) (Equaler, error) {
	if i < v.Start() || i >= v.End() {
		return nil, ErrOutOfRange
	}
	st := v.t.Floor(query(i)).(*position)
	return st.val, nil
}

// StepAt returns the value and range of the step at i, where start <= i < end.
// If i is outside the extent of the vector, an error is returned.
func (v *Vector) StepAt(i int) (start, end int, e Equaler, err error) {
	if i < v.Start() || i >= v.End() {
		return 0, 0, nil, ErrOutOfRange
	}
	lo := v.t.Floor(query(i)).(*position)
	hi := v.t.Ceil(upper(i)).(*position)
	return lo.pos, hi.pos, lo.val, nil
}

// Set sets the value of position i to e.
func (v *Vector) Set(i int, e Equaler) {
	if i < v.min.pos || v.max.pos <= i {
		if !v.Relaxed {
			panic(ErrOutOfRange)
		}

		if i < v.min.pos {
			if i == v.min.pos-1 {
				if e.Equal(v.min.val) {
					v.min.pos--
				} else {
					v.min = &position{pos: i, val: e}
					v.t.Insert(v.min)
				}
			} else {
				if v.min.val.Equal(v.Zero) {
					v.min.pos = i + 1
				} else {
					v.min = &position{pos: i + 1, val: v.Zero}
					v.t.Insert(v.min)
				}
				if e.Equal(v.Zero) {
					v.min.pos--
				} else {
					v.min = &position{pos: i, val: e}
					v.t.Insert(v.min)
				}
			}
		} else if i >= v.max.pos {
			if i == v.max.pos {
				v.max.pos++
				prev := v.t.Floor(query(i)).(*position)
				if !e.Equal(prev.val) {
					v.t.Insert(&position{pos: i, val: e})
				}
			} else {
				mpos := v.max.pos
				v.max.pos = i + 1
				prev := v.t.Floor(query(i)).(*position)
				if !prev.val.Equal(v.Zero) {
					v.t.Insert(&position{pos: mpos, val: v.Zero})
				}
				if !e.Equal(v.Zero) {
					v.t.Insert(&position{pos: i, val: e})
				}
			}
		}
		return
	}

	lo := v.t.Floor(query(i)).(*position)
	if e.Equal(lo.val) {
		return
	}
	hi := v.t.Ceil(upper(i)).(*position)

	if lo.pos == i {
		if hi.pos == i+1 {
			if hi != v.max && e.Equal(hi.val) {
				v.t.Delete(query(i))
				hi.pos--
				if v.min.pos == i {
					v.min = hi
				}
			} else {
				lo.val = e
			}
			if i > v.min.pos {
				prev := v.t.Floor(query(i - 1)).(*position)
				if e.Equal(prev.val) {
					v.t.Delete(query(i))
				}
			}
		} else {
			lo.pos = i + 1
			prev := v.t.Floor(query(i))
			if prev == nil {
				v.min = &position{pos: i, val: e}
				v.t.Insert(v.min)
			} else if !e.Equal(prev.(*position).val) {
				v.t.Insert(&position{pos: i, val: e})
			}
		}
	} else {
		if hi.pos == i+1 {
			if hi != v.max && e.Equal(hi.val) {
				hi.pos--
			} else {
				v.t.Insert(&position{pos: i, val: e})
			}
		} else {
			v.t.Insert(&position{pos: i, val: e})
			v.t.Insert(&position{pos: i + 1, val: lo.val})
		}
	}
}

// SetRange sets the value of positions [start, end) to e.
func (v *Vector) SetRange(start, end int, e Equaler) {
	switch l := end - start; {
	case l == 0:
		if !v.Relaxed && (start < v.min.pos || start >= v.max.pos) {
			panic(ErrOutOfRange)
		}
		return
	case l == 1:
		v.Set(start, e)
		return
	case l < 0:
		panic(ErrInvertedRange)
	}

	if !v.Relaxed && (start < v.min.pos || end > v.max.pos || start == v.max.pos) {
		panic(ErrOutOfRange)
	}

	// Do fast path complete vector replacement if possible.
	if start <= v.min.pos && v.max.pos <= end {
		v.t = llrb.Tree{}
		*v.min = position{pos: start, val: e}
		v.t.Insert(v.min)
		v.max.pos = end
		v.t.Insert(v.max)
		return
	}

	// Handle cases where the given range is entirely outside the vector.
	switch {
	case start >= v.max.pos:
		oldEnd := v.max.pos
		v.max.pos = end
		if start != oldEnd {
			prev := v.t.Floor(query(oldEnd)).(*position)
			if !prev.val.Equal(v.Zero) {
				v.t.Insert(&position{pos: oldEnd, val: v.Zero})
			}
		}
		last := v.t.Floor(query(start)).(*position)
		if !e.Equal(last.val) {
			v.t.Insert(&position{pos: start, val: e})
		}
		return
	case end < v.min.pos:
		if v.min.val.Equal(v.Zero) {
			v.min.pos = end
		} else {
			v.min = &position{pos: end, val: v.Zero}
			v.t.Insert(v.min)
		}
		fallthrough
	case end == v.min.pos:
		if e.Equal(v.min.val) {
			v.min.pos = start
		} else {
			v.min = &position{pos: start, val: e}
			v.t.Insert(v.min)
		}
		return
	}

	// Handle cases where the given range
	last := v.t.Floor(query(end)).(*position)
	deleteRangeInclusive(&v.t, start, end)
	switch {
	// is entirely within the existing vector;
	case v.min.pos < start && end <= v.max.pos:
		prev := v.t.Floor(query(start)).(*position)
		if !e.Equal(prev.val) {
			v.t.Insert(&position{pos: start, val: e})
		}
		if last.val == nil {
			v.t.Insert(v.max)
		} else if !e.Equal(last.val) {
			v.t.Insert(&position{pos: end, val: last.val})
		}

	// hangs over the left end and the right end is in the vector; or
	case start <= v.min.pos:
		lastVal := last.val
		*v.min = position{pos: start, val: e}
		v.t.Insert(v.min)

		if !e.Equal(lastVal) {
			v.t.Insert(&position{pos: end, val: lastVal})
		}

	// hangs over the right end and the left end is in the vector.
	case end > v.max.pos:
		v.max.pos = end
		v.t.Insert(v.max)

		prev := v.t.Floor(query(start)).(*position)
		if e.Equal(prev.val) {
			return
		}
		if last.val == nil || !e.Equal(last.val) {
			v.t.Insert(&position{pos: start, val: e})
		}

	default:
		panic("step: unexpected case")
	}
}

// deleteRangeInclusive deletes all steps within the given range.
// Note that llrb.(*Tree).DoRange does not operate on the node matching the end of a range.
func deleteRangeInclusive(t *llrb.Tree, start, end int) {
	var delQ []llrb.Comparable
	t.DoRange(func(c llrb.Comparable) (done bool) {
		delQ = append(delQ, c)
		return
	}, query(start), query(end+1))
	for _, p := range delQ {
		t.Delete(p)
	}
}

// An Operation is a non-mutating function that can be applied to a vector using Do
// and DoRange.
type Operation func(start, end int, e Equaler)

// Do performs the function fn on steps stored in the Vector in ascending sort order
// of start position. fn is passed the start, end and value of the step.
func (v *Vector) Do(fn Operation) {
	var (
		la  *position
		min = v.min.pos
	)

	v.t.Do(func(c llrb.Comparable) (done bool) {
		p := c.(*position)
		if p.pos != min {
			fn(la.pos, p.pos, la.val)
		}
		la = p
		return
	})
}

// Do performs the function fn on steps stored in the Vector over the range [from, to)
// in ascending sort order of start position. fn is passed the start, end and value of
// the step.
func (v *Vector) DoRange(from, to int, fn Operation) error {
	if to < from {
		return ErrInvertedRange
	}
	var (
		la  *position
		min = v.min.pos
		max = v.max.pos
	)
	if to <= min || from >= max {
		return ErrOutOfRange
	}

	_, end, e, _ := v.StepAt(from)
	if end > to {
		end = to
	}
	fn(from, end, e)
	if end == to {
		return nil
	}
	v.t.DoRange(func(c llrb.Comparable) (done bool) {
		p := c.(*position)
		if p.pos != end {
			fn(la.pos, p.pos, la.val)
		}
		la = p
		return
	}, query(end), query(to))
	if to > la.pos {
		fn(la.pos, to, la.val)
	}

	return nil
}

// A Mutator is a function that is used by Apply and ApplyRange to alter values within
// a Vector.
type Mutator func(Equaler) Equaler

// Convenience mutator functions. Mutator functions are used by Apply and ApplyRange
// to alter step values in a value-dependent manner. These mutators assume the stored
// type matches the function and will panic is this is not true.
var (
	IncInt   Mutator = incInt   // Increment an int value.
	DecInt   Mutator = decInt   // Decrement an int value.
	IncFloat Mutator = incFloat // Increment a float64 value.
	DecFloat Mutator = decFloat // Decrement a float64 value.
)

func incInt(e Equaler) Equaler   { return e.(Int) + 1 }
func decInt(e Equaler) Equaler   { return e.(Int) - 1 }
func incFloat(e Equaler) Equaler { return e.(Float) + 1 }
func decFloat(e Equaler) Equaler { return e.(Float) - 1 }

// Apply applies the mutator function m to steps stored in the Vector in ascending sort order
// of start position. Redundant steps resulting from changes in step values are erased.
func (v *Vector) Apply(m Mutator) {
	var (
		la   Equaler
		min  = v.min.pos
		max  = v.max.pos
		delQ []query
	)

	v.t.Do(func(c llrb.Comparable) (done bool) {
		p := c.(*position)
		if p.pos == max {
			return true
		}
		p.val = m(p.val)
		if p.pos != min && p.pos != max && p.val.Equal(la) {
			delQ = append(delQ, query(p.pos))
		}
		la = p.val
		return
	})

	for _, d := range delQ {
		v.t.Delete(d)
	}
}

// Apply applies the mutator function m to steps stored in the Vector in over the range
// [from, to) in ascending sort order of start position. Redundant steps resulting from
// changes in step values are erased.
func (v *Vector) ApplyRange(from, to int, m Mutator) error {
	if to < from {
		return ErrInvertedRange
	}
	if from == to {
		return nil
	}
	var (
		la   Equaler
		old  position
		min  = v.min.pos
		max  = v.max.pos
		delQ []query
	)
	if !v.Relaxed && (to <= min || from >= max) {
		return ErrOutOfRange
	}
	if v.Relaxed {
		if from < min {
			v.SetRange(from, min, v.Zero)
		}
		if max < to {
			v.SetRange(max, to, v.Zero)
		}
	}

	var end int
	old.pos, end, old.val, _ = v.StepAt(from)
	la = old.val
	la = m(la)
	if to <= end {
		v.SetRange(from, to, la)
		return nil
	}
	if !la.Equal(old.val) {
		switch {
		case from > min:
			if !la.Equal(v.t.Floor(lower(from)).(*position).val) {
				v.t.Insert(&position{from, la})
			} else {
				v.t.Delete(query(from))
			}
		case from < min:
			v.SetRange(from, min, la)
		default:
			*v.min = position{from, la}
		}
	}

	var tail *position
	v.t.DoRange(func(c llrb.Comparable) (done bool) {
		p := c.(*position)
		if p.pos == max {
			// We should be at v.t.Max(), but don't stop
			// just in case there is more. We want to fail
			// noisily if max < v.t.Max().
			return
		}
		if p.pos == to {
			tail = p
			return
		}
		old = *p // Needed for fix-up of last step if to is not at a step boundary.
		p.val = m(p.val)
		if p.pos != min && p.val.Equal(la) {
			delQ = append(delQ, query(p.pos))
		}
		la = p.val
		return
	}, query(end), upper(to))
	for _, d := range delQ {
		v.t.Delete(d)
	}
	if to < max {
		if tail == nil {
			prev := v.t.Floor(lower(to)).(*position)
			if old.pos != from && !old.val.Equal(prev.val) {
				v.t.Insert(&position{to, old.val})
			}
		} else {
			prev := v.t.Floor(lower(tail.pos)).(*position)
			if tail.val != nil && tail.val.Equal(prev.val) {
				v.t.Delete(query(tail.pos))
			}
		}
		return nil
	}

	if v.Relaxed && to > max {
		v.SetRange(max, to, m(v.Zero))
	}

	return nil
}

// String returns a string representation a Vector, displaying step start
// positions and values. The last step indicates the end of the vector and
// always has an associated value of nil.
func (v *Vector) String() string {
	sb := make([]string, 0, v.t.Len())
	v.t.Do(func(c llrb.Comparable) (done bool) {
		p := c.(*position)
		sb = append(sb, fmt.Sprintf("%d:%v", p.pos, p.val))
		return
	})
	return fmt.Sprintf("%v", sb)
}
