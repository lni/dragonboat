// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kdtree

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"gopkg.in/check.v1"
)

var (
	genDot   = flag.Bool("dot", false, "Generate dot code for failing trees.")
	dotLimit = flag.Int("dotmax", 100, "Maximum size for tree output for dot format.")
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

var (
	// Using example from WP article.
	wpData   = Points{{2, 3}, {5, 4}, {9, 6}, {4, 7}, {8, 1}, {7, 2}}
	nbWpData = nbPoints{{2, 3}, {5, 4}, {9, 6}, {4, 7}, {8, 1}, {7, 2}}
	wpBound  = &Bounding{Point{2, 1}, Point{9, 7}}
	bData    = func(i int) Points {
		p := make(Points, i)
		for i := range p {
			p[i] = Point{rand.Float64(), rand.Float64(), rand.Float64()}
		}
		return p
	}(1e2)
	bTree = New(bData, true)
)

func (s *S) TestNew(c *check.C) {
	for i, test := range []struct {
		data     Interface
		bounding bool
		bounds   *Bounding
	}{
		{wpData, false, nil},
		{nbWpData, false, nil},
		{wpData, true, wpBound},
		{nbWpData, true, nil},
	} {
		var t *Tree
		NewTreePanics := func() (panicked bool) {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()
			t = New(test.data, test.bounding)
			return
		}
		c.Check(NewTreePanics(), check.Equals, false)
		c.Check(t.Root.isKDTree(), check.Equals, true)
		switch data := test.data.(type) {
		case Points:
			for _, p := range data {
				c.Check(t.Contains(p), check.Equals, true)
			}
		case nbPoints:
			for _, p := range data {
				c.Check(t.Contains(p), check.Equals, true)
			}
		}
		c.Check(t.Root.Bounding, check.DeepEquals, test.bounds,
			check.Commentf("Test %d. %T %v", i, test.data, test.bounding))
		if c.Failed() && *genDot && t.Len() <= *dotLimit {
			err := dotFile(t, fmt.Sprintf("TestNew%T", test.data), "")
			if err != nil {
				c.Errorf("Dot file write failed: %v", err)
			}
		}
	}
}

func (s *S) TestInsert(c *check.C) {
	for i, test := range []struct {
		data   Interface
		insert []Comparable
		bounds *Bounding
	}{
		{
			wpData,
			[]Comparable{Point{0, 0}, Point{10, 10}},
			&Bounding{Point{0, 0}, Point{10, 10}},
		},
		{
			nbWpData,
			[]Comparable{nbPoint{0, 0}, nbPoint{10, 10}},
			nil,
		},
	} {
		t := New(test.data, true)
		for _, v := range test.insert {
			t.Insert(v, true)
		}
		c.Check(t.Root.isKDTree(), check.Equals, true)
		c.Check(t.Root.Bounding, check.DeepEquals, test.bounds,
			check.Commentf("Test %d. %T", i, test.data))
		if c.Failed() && *genDot && t.Len() <= *dotLimit {
			err := dotFile(t, fmt.Sprintf("TestInsert%T", test.data), "")
			if err != nil {
				c.Errorf("Dot file write failed: %v", err)
			}
		}
	}
}

type compFn func(float64) bool

func left(v float64) bool  { return v <= 0 }
func right(v float64) bool { return !left(v) }

func (n *Node) isKDTree() bool {
	if n == nil {
		return true
	}
	d := n.Point.Dims()
	// Together these define the property of minimal orthogonal bounding.
	if !(n.isContainedBy(n.Bounding) && n.Bounding.planesHaveCoincidentPointsIn(n, [2][]bool{make([]bool, d), make([]bool, d)})) {
		return false
	}
	if !n.Left.isPartitioned(n.Point, left, n.Plane) {
		return false
	}
	if !n.Right.isPartitioned(n.Point, right, n.Plane) {
		return false
	}
	return n.Left.isKDTree() && n.Right.isKDTree()
}

func (n *Node) isPartitioned(pivot Comparable, fn compFn, plane Dim) bool {
	if n == nil {
		return true
	}
	if n.Left != nil && fn(pivot.Compare(n.Left.Point, plane)) {
		return false
	}
	if n.Right != nil && fn(pivot.Compare(n.Right.Point, plane)) {
		return false
	}
	return n.Left.isPartitioned(pivot, fn, plane) && n.Right.isPartitioned(pivot, fn, plane)
}

func (n *Node) isContainedBy(b *Bounding) bool {
	if n == nil {
		return true
	}
	if !b.Contains(n.Point) {
		return false
	}
	return n.Left.isContainedBy(b) && n.Right.isContainedBy(b)
}

func (b *Bounding) planesHaveCoincidentPointsIn(n *Node, tight [2][]bool) bool {
	if b == nil {
		return true
	}
	if n == nil {
		return true
	}

	b.planesHaveCoincidentPointsIn(n.Left, tight)
	b.planesHaveCoincidentPointsIn(n.Right, tight)

	var ok = true
	for i := range tight {
		for d := 0; d < n.Point.Dims(); d++ {
			if c := n.Point.Compare(b[0], Dim(d)); c == 0 {
				tight[i][d] = true
			}
			ok = ok && tight[i][d]
		}
	}
	return ok
}

func nearest(q Point, p Points) (Point, float64) {
	min := q.Distance(p[0])
	var r int
	for i := 1; i < p.Len(); i++ {
		d := q.Distance(p[i])
		if d < min {
			min = d
			r = i
		}
	}
	return p[r], min
}

func (s *S) TestNearestRandom(c *check.C) {
	const (
		min = 0.
		max = 1000.

		dims    = 4
		setSize = 10000
	)

	var randData Points
	for i := 0; i < setSize; i++ {
		p := make(Point, dims)
		for j := 0; j < dims; j++ {
			p[j] = (max-min)*rand.Float64() + min
		}
		randData = append(randData, p)
	}
	t := New(randData, false)

	for i := 0; i < setSize; i++ {
		q := make(Point, dims)
		for j := 0; j < dims; j++ {
			q[j] = (max-min)*rand.Float64() + min
		}

		p, _ := t.Nearest(q)
		ep, _ := nearest(q, randData)
		c.Assert(p, check.DeepEquals, ep, check.Commentf("Test %d: query %.3f expects %.3f", i, q, ep))
	}
}

func (s *S) TestNearest(c *check.C) {
	t := New(wpData, false)
	for i, q := range append([]Point{
		{4, 6},
		{7, 5},
		{8, 7},
		{6, -5},
		{1e5, 1e5},
		{1e5, -1e5},
		{-1e5, 1e5},
		{-1e5, -1e5},
		{1e5, 0},
		{0, -1e5},
		{0, 1e5},
		{-1e5, 0},
	}, wpData...) {
		p, d := t.Nearest(q)
		ep, ed := nearest(q, wpData)
		c.Check(p, check.DeepEquals, ep, check.Commentf("Test %d: query %.3f expects %.3f", i, q, ep))
		c.Check(d, check.Equals, ed)
	}
}

func nearestN(n int, q Point, p Points) []ComparableDist {
	nk := NewNKeeper(n)
	for i := 0; i < p.Len(); i++ {
		nk.Keep(ComparableDist{Comparable: p[i], Dist: q.Distance(p[i])})
	}
	if len(nk.Heap) == 1 {
		return nk.Heap
	}
	sort.Sort(nk)
	for i, j := 0, len(nk.Heap)-1; i < j; i, j = i+1, j-1 {
		nk.Heap[i], nk.Heap[j] = nk.Heap[j], nk.Heap[i]
	}
	return nk.Heap
}

func (s *S) TestNearestSetN(c *check.C) {
	t := New(wpData, false)
	in := append([]Point{
		{4, 6},
		{7, 5},
		{8, 7},
		{6, -5},
		{1e5, 1e5},
		{1e5, -1e5},
		{-1e5, 1e5},
		{-1e5, -1e5},
		{1e5, 0},
		{0, -1e5},
		{0, 1e5},
		{-1e5, 0}}, wpData[:len(wpData)-1]...)
	for k := 1; k <= len(wpData); k++ {
		for i, q := range in {
			ep := nearestN(k, q, wpData)
			nk := NewNKeeper(k)
			t.NearestSet(nk, q)

			var max float64
			ed := make(map[float64]map[string]struct{})
			for _, p := range ep {
				if p.Dist > max {
					max = p.Dist
				}
				d, ok := ed[p.Dist]
				if !ok {
					d = make(map[string]struct{})
				}
				d[fmt.Sprint(p.Comparable)] = struct{}{}
				ed[p.Dist] = d
			}
			kd := make(map[float64]map[string]struct{})
			for _, p := range nk.Heap {
				c.Check(max >= p.Dist, check.Equals, true)
				d, ok := kd[p.Dist]
				if !ok {
					d = make(map[string]struct{})
				}
				d[fmt.Sprint(p.Comparable)] = struct{}{}
				kd[p.Dist] = d
			}

			// If the available number of slots does not fit all the coequal furthest points
			// we will fail the check. So remove, but check them minimally here.
			if !reflect.DeepEqual(ed[max], kd[max]) {
				// The best we can do at this stage is confirm that there are an equal number of matches at this distance.
				c.Check(len(ed[max]), check.Equals, len(kd[max]))
				delete(ed, max)
				delete(kd, max)
			}

			c.Check(kd, check.DeepEquals, ed, check.Commentf("Test k=%d %d: query %.3f expects %.3f", k, i, q, ep))
		}
	}
}

func (s *S) TestNearestSetDist(c *check.C) {
	t := New(wpData, false)
	for i, q := range []Point{
		{4, 6},
		{7, 5},
		{8, 7},
		{6, -5},
	} {
		for d := 1.; d < 100; d += 0.1 {
			dk := NewDistKeeper(d)
			t.NearestSet(dk, q)

			hits := make(map[string]float64)
			for _, p := range wpData {
				hits[fmt.Sprint(p)] = p.Distance(q)
			}

			for _, p := range dk.Heap {
				var finished bool
				if p.Comparable != nil {
					delete(hits, fmt.Sprint(p.Comparable))
					c.Check(finished, check.Equals, false)
					dist := p.Comparable.Distance(q)
					c.Check(dist <= d, check.Equals, true, check.Commentf("Test %d: query %v found %v expect %.3f <= %.3f", i, q, p, dist, d))
				} else {
					finished = true
				}
			}

			for p, dist := range hits {
				c.Check(dist > d, check.Equals, true, check.Commentf("Test %d: query %v missed %v expect %.3f > %.3f", i, q, p, dist, d))
			}
		}
	}
}

func (s *S) TestDo(c *check.C) {
	var result Points
	t := New(wpData, false)
	f := func(c Comparable, _ *Bounding, _ int) (done bool) {
		result = append(result, c.(Point))
		return
	}
	killed := t.Do(f)
	c.Check(result, check.DeepEquals, wpData)
	c.Check(killed, check.Equals, false)
}

func (s *S) TestDoBounded(c *check.C) {
	for _, test := range []struct {
		bounds *Bounding
		result Points
	}{
		{
			nil,
			wpData,
		},
		{
			&Bounding{Point{0, 0}, Point{10, 10}},
			wpData,
		},
		{
			&Bounding{Point{3, 4}, Point{10, 10}},
			Points{Point{5, 4}, Point{4, 7}, Point{9, 6}},
		},
		{
			&Bounding{Point{3, 3}, Point{10, 10}},
			Points{Point{5, 4}, Point{4, 7}, Point{9, 6}},
		},
		{
			&Bounding{Point{0, 0}, Point{6, 5}},
			Points{Point{2, 3}, Point{5, 4}},
		},
		{
			&Bounding{Point{5, 2}, Point{7, 4}},
			Points{Point{5, 4}, Point{7, 2}},
		},
		{
			&Bounding{Point{2, 2}, Point{7, 4}},
			Points{Point{2, 3}, Point{5, 4}, Point{7, 2}},
		},
		{
			&Bounding{Point{2, 3}, Point{9, 6}},
			Points{Point{2, 3}, Point{5, 4}, Point{9, 6}},
		},
		{
			&Bounding{Point{7, 2}, Point{7, 2}},
			Points{Point{7, 2}},
		},
	} {
		var result Points
		t := New(wpData, false)
		f := func(c Comparable, _ *Bounding, _ int) (done bool) {
			result = append(result, c.(Point))
			return
		}
		killed := t.DoBounded(f, test.bounds)
		c.Check(result, check.DeepEquals, test.result)
		c.Check(killed, check.Equals, false)
	}
}

func BenchmarkNew(b *testing.B) {
	b.StopTimer()
	p := make(Points, 1e5)
	for i := range p {
		p[i] = Point{rand.Float64(), rand.Float64(), rand.Float64()}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = New(p, false)
	}
}

func BenchmarkNewBounds(b *testing.B) {
	b.StopTimer()
	p := make(Points, 1e5)
	for i := range p {
		p[i] = Point{rand.Float64(), rand.Float64(), rand.Float64()}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = New(p, true)
	}
}

func BenchmarkInsert(b *testing.B) {
	rand.Seed(1)
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(Point{rand.Float64(), rand.Float64(), rand.Float64()}, false)
	}
}

func BenchmarkInsertBounds(b *testing.B) {
	rand.Seed(1)
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(Point{rand.Float64(), rand.Float64(), rand.Float64()}, true)
	}
}

func (s *S) TestBenches(c *check.C) {
	c.Check(bTree.Root.isKDTree(), check.Equals, true)
	for i := 0; i < 1e3; i++ {
		q := Point{rand.Float64(), rand.Float64(), rand.Float64()}
		p, d := bTree.Nearest(q)
		ep, ed := nearest(q, bData)
		c.Check(p, check.DeepEquals, ep, check.Commentf("Test %d: query %.3f expects %.3f", i, q, ep))
		c.Check(d, check.Equals, ed)
	}
	if c.Failed() && *genDot && bTree.Len() <= *dotLimit {
		err := dotFile(bTree, "TestBenches", "")
		if err != nil {
			c.Errorf("Dot file write failed: %v", err)
		}
	}
}

func BenchmarkNearest(b *testing.B) {
	var (
		r Comparable
		d float64
	)
	for i := 0; i < b.N; i++ {
		r, d = bTree.Nearest(Point{rand.Float64(), rand.Float64(), rand.Float64()})
	}
	_, _ = r, d
}

func BenchmarkNearBrute(b *testing.B) {
	var (
		r Comparable
		d float64
	)
	for i := 0; i < b.N; i++ {
		r, d = nearest(Point{rand.Float64(), rand.Float64(), rand.Float64()}, bData)
	}
	_, _ = r, d
}

func BenchmarkNearestSetN10(b *testing.B) {
	var nk = NewNKeeper(10)
	for i := 0; i < b.N; i++ {
		bTree.NearestSet(nk, Point{rand.Float64(), rand.Float64(), rand.Float64()})
		nk.Heap = nk.Heap[:1]
		nk.Heap[0] = ComparableDist{Comparable: nil, Dist: inf}
	}
}

func BenchmarkNearBruteN10(b *testing.B) {
	var r []ComparableDist
	for i := 0; i < b.N; i++ {
		r = nearestN(10, Point{rand.Float64(), rand.Float64(), rand.Float64()}, bData)
	}
	_ = r
}

func dot(t *Tree, label string) string {
	if t == nil {
		return ""
	}
	var (
		s      []string
		follow func(*Node)
	)
	follow = func(n *Node) {
		id := uintptr(unsafe.Pointer(n))
		c := fmt.Sprintf("%d[label = \"<Left> |<Elem> %s/%.3f\\n%.3f|<Right>\"];",
			id, n, n.Point.(Point)[n.Plane], *n.Bounding)
		if n.Left != nil {
			c += fmt.Sprintf("\n\t\tedge [arrowhead=normal]; \"%d\":Left -> \"%d\":Elem;",
				id, uintptr(unsafe.Pointer(n.Left)))
			follow(n.Left)
		}
		if n.Right != nil {
			c += fmt.Sprintf("\n\t\tedge [arrowhead=normal]; \"%d\":Right -> \"%d\":Elem;",
				id, uintptr(unsafe.Pointer(n.Right)))
			follow(n.Right)
		}
		s = append(s, c)
	}
	if t.Root != nil {
		follow(t.Root)
	}
	return fmt.Sprintf("digraph %s {\n\tnode [shape=record,height=0.1];\n\t%s\n}\n",
		label,
		strings.Join(s, "\n\t"),
	)
}

func dotFile(t *Tree, label, dotString string) (err error) {
	if t == nil && dotString == "" {
		return
	}
	f, err := os.Create(label + ".dot")
	if err != nil {
		return
	}
	defer f.Close()
	if dotString == "" {
		fmt.Fprintf(f, dot(t, label))
	} else {
		fmt.Fprintf(f, dotString)
	}
	return
}
