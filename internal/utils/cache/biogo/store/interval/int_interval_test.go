// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"unsafe"

	"gopkg.in/check.v1"

	"github.com/biogo/store/llrb"
)

// Integrity checks - translated from http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java

// Is this tree a BST?
func (t *IntTree) isBST() bool {
	if t == nil {
		return true
	}
	return t.Root.isBST(t.Min(), t.Max())
}

// Are all the values in the BST rooted at x between min and max,
// and does the same property hold for both subtrees?
func (n *IntNode) isBST(min, max IntInterface) bool {
	if n == nil {
		return true
	}
	if n.Elem.Range().Start < min.Range().Start || n.Elem.Range().Start > max.Range().Start {
		return false
	}
	return n.Left.isBST(min, n.Elem) || n.Right.isBST(n.Elem, max)
}

// Test BU and TD234 invariants.
func (t *IntTree) is23_234() bool {
	if t == nil {
		return true
	}
	return t.Root.is23_234()
}
func (n *IntNode) is23_234() bool {
	if n == nil {
		return true
	}
	if Mode == BU23 {
		// If the node has two children, only one of them may be red.
		// The other must be black...
		if (n.Left != nil) && (n.Right != nil) {
			if n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
				return false
			}
		}
		// and the red node should really should be the left one.
		if n.Right.color() == llrb.Red {
			return false
		}
	} else if Mode == TD234 {
		// This test is altered from that shown in the java since the trees
		// shown in the paper do not conform to the test as it existed and the
		// current situation does not break the 2-3-4 definition of the LLRB.
		if n.Right.color() == llrb.Red && n.Left.color() == llrb.Black {
			return false
		}
	} else {
		panic("cannot reach")
	}
	if n.color() == llrb.Red && n.Left.color() == llrb.Red {
		return false
	}
	return n.Left.is23_234() && n.Right.is23_234()
}

// Do all paths from root to leaf have same number of black edges?
func (t *IntTree) isBalanced() bool {
	if t == nil {
		return true
	}
	var black int // number of black links on path from root to min
	for x := t.Root; x != nil; x = x.Left {
		if x.color() == llrb.Black {
			black++
		}
	}
	return t.Root.isBalanced(black)
}

// Does every path from the root to a leaf have the given number
// of black links?
func (n *IntNode) isBalanced(black int) bool {
	if n == nil && black == 0 {
		return true
	} else if n == nil && black != 0 {
		return false
	}
	if n.color() == llrb.Black {
		black--
	}
	return n.Left.isBalanced(black) && n.Right.isBalanced(black)
}

// Does every node correctly annotate the range of its children.
func (t *IntTree) isRanged() bool {
	if t == nil {
		return true
	}
	return t.Root.isRanged()
}
func (n *IntNode) isRanged() bool {
	if n == nil {
		return true
	}
	e, r := n.Elem, n.Range
	m := n.bounding(e.Range())
	return m.Start == r.Start && m.End == r.End &&
		n.Left.isRanged() &&
		n.Right.isRanged()
}
func (n *IntNode) bounding(m IntRange) IntRange {
	m.Start = intMin(n.Elem.Range().Start, m.Start)
	m.End = intMax(n.Elem.Range().End, m.End)
	if n.Left != nil {
		m = n.Left.bounding(m)
	}
	if n.Right != nil {
		m = n.Right.bounding(m)
	}
	return m
}

// Test helpers

type intOverlap struct {
	start, end int
	id         uintptr
}

func (o *intOverlap) Overlap(r IntRange) bool {
	return o.end > r.Start && o.start < r.End
}
func (o *intOverlap) ID() uintptr     { return o.id }
func (o *intOverlap) Range() IntRange { return IntRange{o.start, o.end} }
func (o *intOverlap) String() string  { return fmt.Sprintf("[%d,%d)", o.start, o.end) }

// Return a Newick format description of a tree defined by a node
func (n *IntNode) describeTree(char, color bool) string {
	s := []rune(nil)

	var follow func(*IntNode)
	follow = func(n *IntNode) {
		children := n.Left != nil || n.Right != nil
		if children {
			s = append(s, '(')
		}
		if n.Left != nil {
			follow(n.Left)
		}
		if children {
			s = append(s, ',')
		}
		if n.Right != nil {
			follow(n.Right)
		}
		if children {
			s = append(s, ')')
		}
		if n.Elem != nil {
			s = append(s, []rune(fmt.Sprintf("%d", n.Elem))...)
			if color {
				s = append(s, []rune(fmt.Sprintf(" %v", n.color()))...)
			}
		}
	}
	if n == nil {
		s = []rune("()")
	} else {
		follow(n)
	}
	s = append(s, ';')

	return string(s)
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Tests

func (s *S) TestIntNilOperations(c *check.C) {
	t := &IntTree{}
	c.Check(t.Min(), check.Equals, nil)
	c.Check(t.Max(), check.Equals, nil)
	if Mode == TD234 {
		return
	}
	t.DeleteMin(false)
	c.Check(*t, check.Equals, IntTree{})
	t.DeleteMax(false)
	c.Check(*t, check.Equals, IntTree{})
}

func (s *S) TestIntRange(c *check.C) {
	t := &IntTree{}
	for i, iv := range []*intOverlap{
		{0, 2, 0},
		{2, 4, 0},
		{1, 6, 0},
		{3, 4, 0},
		{1, 3, 0},
		{4, 6, 0},
		{5, 8, 0},
		{6, 8, 0},
		{5, 9, 0},
	} {
		t.Insert(iv, false)
		ok := c.Check(t.isRanged(), check.Equals, true, check.Commentf("insertion %d: %v", i, iv))
		if !ok && *genDot && t.Len() <= *dotLimit {
			err := t.dotFile(fmt.Sprintf("TestRange_%d", i), "")
			if err != nil {
				c.Errorf("Dot file write failed: %v", err)
			}
		}
	}
}

func (s *S) TestIntInsertion(c *check.C) {
	var (
		min, max = 0, 1000
		t        = &IntTree{}
		length   = 100
	)
	for i := min; i <= max; i++ {
		t.Insert(&intOverlap{start: i, end: i + length}, false)
		c.Check(t.Len(), check.Equals, int(i+1))
		failed := false
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		failed = failed || !c.Check(t.isRanged(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := t.dotFile(fmt.Sprintf("TestInsertion_after_ins_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
	c.Check(t.Min().Range().Start, check.Equals, min)
	c.Check(t.Max().Range().Start, check.Equals, max)
}

func (s *S) TestIntFastInsertion(c *check.C) {
	var (
		min, max = 0, 1000
		t        = &IntTree{}
		length   = 100
	)
	for i := min; i <= max; i++ {
		t.Insert(&intOverlap{start: i, end: i + length}, true)
		c.Check(t.Len(), check.Equals, int(i+1))
		c.Check(t.isBST(), check.Equals, true)
		c.Check(t.is23_234(), check.Equals, true)
		c.Check(t.isBalanced(), check.Equals, true)
	}
	t.AdjustRanges()
	c.Check(t.isRanged(), check.Equals, true)
	c.Check(t.Min().Range().Start, check.Equals, min)
	c.Check(t.Max().Range().Start, check.Equals, max)
}

func (s *S) TestIntDeletion(c *check.C) {
	var (
		min, max = 0, 1000
		e        = int(max-min) + 1
		t        = &IntTree{}
		length   = 1
	)
	for i := min; i <= max; i++ {
		t.Insert(&intOverlap{start: i, end: i + length, id: uintptr(i)}, false)
	}
	for i := min; i <= max; i++ {
		var dotString string
		if o := t.Get(&intOverlap{start: i, end: i + length}); o != nil {
			e--
		}
		if *genDot && t.Len() <= *dotLimit {
			dotString = t.dot(fmt.Sprintf("TestDeletion_before_del_%d", i))
		}
		t.Delete(&intOverlap{start: i, end: i + length, id: uintptr(i)}, false)
		c.Check(t.Len(), check.Equals, e)
		if i < max {
			failed := false
			failed = failed || !c.Check(t.isBST(), check.Equals, true)
			failed = failed || !c.Check(t.is23_234(), check.Equals, true)
			failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
			failed = failed || !c.Check(t.isRanged(), check.Equals, true)
			if failed {
				if *printTree {
					c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
				}
				if *genDot && t.Len() < *dotLimit {
					var err error
					err = (*IntTree)(nil).dotFile(fmt.Sprintf("TestDeletion_before_del_%d", i), dotString)
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
					err = t.dotFile(fmt.Sprintf("TestDeletion_after_del_%d", i), "")
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
				}
				c.Fatal("Cannot continue test: invariant contradiction")
			}
		}
	}
	c.Check(*t, check.Equals, IntTree{})
}

func (s *S) TestIntFastDeletion(c *check.C) {
	var (
		min, max = 0, 1000
		t        = &IntTree{}
		length   = 1
	)
	for i := min; i <= max; i++ {
		t.Insert(&intOverlap{start: i, end: i + length, id: uintptr(i)}, false)
	}
	for i := min; i <= max; i++ {
		t.Delete(&intOverlap{start: i, end: i + length, id: uintptr(i)}, true)
		c.Check(t.isBST(), check.Equals, true)
		c.Check(t.is23_234(), check.Equals, true)
		c.Check(t.isBalanced(), check.Equals, true)
		if i == max/2 {
			t.AdjustRanges()
			c.Check(t.isRanged(), check.Equals, true)
		}
	}
	c.Check(*t, check.Equals, IntTree{})
}

func (s *S) TestIntGet(c *check.C) {
	var (
		min, max = 0, 1000
		t        = &IntTree{}
	)
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			t.Insert(&intOverlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			o := t.Get(&intOverlap{start: i, end: i + 1})
			c.Check(len(o), check.Equals, 1)                                   // Check inserted elements are present.
			c.Check(o[0], check.DeepEquals, &intOverlap{start: i, end: i + 1}) // Check inserted elements are correct.
		} else {
			o := t.Get(&intOverlap{start: i, end: i + 1})
			c.Check(o, check.DeepEquals, []IntInterface(nil)) // Check inserted elements are absent.
		}
	}
}

func (s *S) TestIntFloor(c *check.C) {
	min, max := 0, 1000
	t := &IntTree{}
	for i := min; i <= max; i++ {
		if i&1 == 0 { // Insert even numbers only.
			t.Insert(&intOverlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i <= max; i++ {
		l, _ := t.Floor(&intOverlap{start: i, end: i + 1})
		if i&1 == 0 {
			c.Check(l, check.DeepEquals, &intOverlap{start: i, end: i + 1}) // Check even Floors are themselves.
		} else {
			c.Check(l, check.DeepEquals, &intOverlap{start: i - 1, end: i}) // Check odd Floors are the previous number.
		}
	}
	l, _ := t.Floor(&intOverlap{start: min - 1, end: min})
	c.Check(l, check.DeepEquals, IntInterface(nil))
}

func (s *S) TestIntCeil(c *check.C) {
	min, max := 0, 1000
	t := &IntTree{}
	for i := min; i <= max; i++ {
		if i&1 == 1 { // Insert odd numbers only.
			t.Insert(&intOverlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i < max; i++ {
		u, _ := t.Ceil(&intOverlap{start: i, end: i + 1})
		if i&1 == 1 {
			c.Check(u, check.DeepEquals, &intOverlap{start: i, end: i + 1}) // Check odd Ceils are themselves.
		} else {
			c.Check(u, check.DeepEquals, &intOverlap{start: i + 1, end: i + 2}) // Check even Ceils are the next number.
		}
	}
	u, _ := t.Ceil(&intOverlap{start: max, end: max + 2})
	c.Check(u, check.DeepEquals, IntInterface(nil))
}

func (s *S) TestIntRandomlyInsertedGet(c *check.C) {
	var (
		count, max = 1000, 1000
		t          = &IntTree{}
		length     = 100
		verify     = map[intOverlap]struct{}{}
		verified   = map[intOverlap]struct{}{}
	)
	for i := 0; i < count; i++ {
		s := (rand.Intn(max))
		v := intOverlap{start: s, end: s + length}
		t.Insert(&v, false)
		verify[v] = struct{}{}
	}
	// Random fetch order.
	for v := range verify {
		o := t.Get(&v)
		c.Check(len(o), check.Not(check.Equals), 0) // Check inserted elements are present.
		for _, iv := range o {
			vr := *iv.(*intOverlap)
			_, ok := verify[vr]
			c.Check(ok, check.Equals, true, check.Commentf("%v should exist", vr))
			if ok {
				verified[vr] = struct{}{}
			}
		}
	}
	c.Check(len(verify), check.Equals, len(verified))
	for v := range verify {
		_, ok := verified[v]
		c.Check(ok, check.Equals, true, check.Commentf("%v should exist", v))
	}

	// Check all possible insertions.
	for s := 0; s <= max; s++ {
		v := intOverlap{start: s, end: s + length}
		o := t.Get(&v)
		if _, ok := verify[v]; ok {
			c.Check(len(o), check.Not(check.Equals), 0) // Check inserted elements are present.
		}
	}
}

func (s *S) TestIntRandomInsertion(c *check.C) {
	var (
		count, max = 1000, 1000
		t          = &IntTree{}
		length     = 100
	)
	for i := 0; i < count; i++ {
		s := (rand.Intn(max))
		v := intOverlap{start: s, end: s + length}
		t.Insert(&v, false)
		failed := false
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		failed = failed || !c.Check(t.isRanged(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := t.dotFile(fmt.Sprintf("TestRandomInsertion_after_ins_%d_%d", v.start, v.end), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
}

func (s *S) TestIntRandomDeletion(c *check.C) {
	var (
		count, max = 14, 3
		r          = make([]intOverlap, count)
		t          = &IntTree{}
		length     = 1
	)
	for i := range r {
		s := (rand.Intn(max))
		r[i] = intOverlap{start: s, end: s + length, id: uintptr(i)}
		t.Insert(&r[i], false)
	}
	for i, v := range r {
		var dotString string
		if *genDot && t.Len() <= *dotLimit {
			dotString = t.dot(fmt.Sprintf("TestRandomDeletion_before_del_%d_%d_%d", i, v.start, v.end))
		}
		t.Delete(&v, false)
		if t != nil {
			failed := false
			failed = failed || !c.Check(t.isBST(), check.Equals, true)
			failed = failed || !c.Check(t.is23_234(), check.Equals, true)
			failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
			failed = failed || !c.Check(t.isRanged(), check.Equals, true)
			if failed {
				if *printTree {
					c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
				}
				if *genDot && t.Len() <= *dotLimit {
					var err error
					err = (*IntTree)(nil).dotFile(fmt.Sprintf("TestRandomDeletion_before_del_%d_%d_%d", i, v.start, v.end), dotString)
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
					err = t.dotFile(fmt.Sprintf("TestRandomDeletion_after_del_%d_%d_%d", i, v.start, v.end), "")
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
				}
				c.Fatal("Cannot continue test: invariant contradiction")
			}
		}
	}
	c.Check(*t, check.DeepEquals, IntTree{})
}

func (s *S) TestIntDeleteMinMax(c *check.C) {
	var (
		min, max = 0, 10
		t        = &IntTree{}
		length   = 1
		dI       int
	)
	for i := min; i <= max; i++ {
		v := intOverlap{start: i, end: i + length}
		t.Insert(&v, false)
		dI = t.Len()
	}
	c.Check(dI, check.Equals, int(max-min+1))
	for i, m := 0, int(max); i < m/2; i++ {
		var failed bool
		t.DeleteMin(false)
		dI--
		c.Check(t.Len(), check.Equals, dI)
		min++
		failed = !c.Check(t.Min(), check.DeepEquals, &intOverlap{start: min, end: min + length})
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		failed = failed || !c.Check(t.isRanged(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := t.dotFile(fmt.Sprintf("TestDeleteMinMax_after_delmin_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
		t.DeleteMax(false)
		dI--
		c.Check(t.Len(), check.Equals, dI)
		max--
		failed = !c.Check(t.Max(), check.DeepEquals, &intOverlap{start: max, end: max + length})
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		failed = failed || !c.Check(t.isRanged(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := t.dotFile(fmt.Sprintf("TestDeleteMinMax_after_delmax_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
}

// Check for correct child range calculation when the left child
// extends beyond the right child.
func (s *S) TestIntRangeBug(c *check.C) {
	var t IntTree
	for i, e := range []*intOverlap{
		{start: 0, end: 10},
		{start: 1, end: 5},
		{start: 1, end: 5},
	} {
		e.id = uintptr(i)
		err := t.Insert(e, false)
		c.Assert(err, check.Equals, nil)
	}
	c.Check(t.isRanged(), check.Equals, true)
}

// Issue 15 is another case of a range invariant update bug.
// https://code.google.com/p/biogo/issues/detail?id=15
func (s *S) TestIssue15Int(c *check.C) {
	ranges := []*intOverlap{
		{start: 5, end: 6},
		{start: 7, end: 8},
		{start: 9, end: 10},
		{start: 0, end: 4},
		{start: 0, end: 1},
		{start: 0, end: 1},
		{start: 0, end: 1},
	}

	var t IntTree
	for i, iv := range ranges {
		iv.id = uintptr(i)
		err := t.Insert(iv, false)
		c.Assert(err, check.Equals, nil)

		failed := !c.Check(t.isRanged(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", t.Root.describeTree(false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := t.dotFile(fmt.Sprintf("Issue15IntTest_%02d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
		}
	}

	got := t.Get(&intOverlap{start: 2, end: 3})
	c.Check(len(got), check.Equals, 1, check.Commentf("Expected one overlap, got %d", len(got)))
}

func (s *S) TestIntInsertBug(c *check.C) {
	var t IntTree
	for i := 10; i > 0; i-- {
		t.Insert(&intOverlap{start: 0, end: 1, id: uintptr(i)}, false)
	}
	c.Check(t.Len(), check.Equals, 10, check.Commentf("Expected 10 entries, got %d", t.Len()))
	for i := 1; i <= 10; i++ {
		t.Delete(&intOverlap{start: 0, end: 1, id: uintptr(i)}, false)
	}
	c.Check(t.Len(), check.Equals, 0, check.Commentf("Expected 0 entries, got %d", t.Len()))
}

func (t *IntTree) dot(label string) string {
	if t == nil {
		return ""
	}
	var (
		s      []string
		follow func(*IntNode)
		arrows = map[llrb.Color]string{llrb.Red: "none", llrb.Black: "normal"}
	)
	follow = func(n *IntNode) {
		id := uintptr(unsafe.Pointer(n))
		c := fmt.Sprintf("%d[label = \"<Left> |<Elem> interval:%v\\nrange:[%d,%d)\\nid:%d|<Right>\"];",
			id, n.Elem, n.Range.Start, n.Range.End, n.Elem.ID())
		if n.Left != nil {
			c += fmt.Sprintf("\n\t\tedge [color=%v,arrowhead=%s]; \"%d\":Left -> \"%d\":Elem;",
				n.Left.color(), arrows[n.Left.color()], id, uintptr(unsafe.Pointer(n.Left)))
			follow(n.Left)
		}
		if n.Right != nil {
			c += fmt.Sprintf("\n\t\tedge [color=%v,arrowhead=%s]; \"%d\":Right -> \"%d\":Elem;",
				n.Right.color(), arrows[n.Right.color()], id, uintptr(unsafe.Pointer(n.Right)))
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

func (t *IntTree) dotFile(label, dotString string) (err error) {
	if t == nil && dotString == "" {
		return
	}
	f, err := os.Create(label + ".dot")
	if err != nil {
		return
	}
	defer f.Close()
	if dotString == "" {
		fmt.Fprintf(f, t.dot(label))
	} else {
		fmt.Fprintf(f, dotString)
	}
	return
}

// Benchmarks

func BenchmarkIntInsert(b *testing.B) {
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
}

func BenchmarkIntFastInsert(b *testing.B) {
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, true)
	}
}

func BenchmarkIntGet(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Get(&intOverlap{start: s, end: s + length})
	}
}

func BenchmarkIntMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < 1e5; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	var m IntInterface
	for i := 0; i < b.N; i++ {
		m = t.Min()
	}
	_ = m
}

func BenchmarkIntMax(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < 1e5; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	var m IntInterface
	for i := 0; i < b.N; i++ {
		m = t.Max()
	}
	_ = m
}

func BenchmarkIntDelete(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 1
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Delete(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
}

func BenchmarkIntFastDelete(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 1
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Delete(&intOverlap{start: s, end: s + length, id: uintptr(s)}, true)
	}
}

func BenchmarkIntDeleteMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.DeleteMin(false)
	}
}

func BenchmarkIntFastDeleteMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &IntTree{}
		length = 10
	)
	for i := 0; i < b.N; i++ {
		s := b.N - i
		t.Insert(&intOverlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.DeleteMin(true)
	}
}
