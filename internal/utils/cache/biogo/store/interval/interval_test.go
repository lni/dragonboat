// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"unsafe"

	"gopkg.in/check.v1"

	"github.com/biogo/store/llrb"
)

var (
	printTree = flag.Bool("trees", false, "Print failing tree in Newick format.")
	genDot    = flag.Bool("dot", false, "Generate dot code for failing trees.")
	dotLimit  = flag.Int("dotmax", 100, "Maximum size for tree output for dot format.")
)

// Integrity checks - translated from http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java

// Is this tree a BST?
func (t *Tree) isBST() bool {
	if t == nil {
		return true
	}
	return t.Root.isBST(t.Min(), t.Max())
}

// Are all the values in the BST rooted at x between min and max,
// and does the same property hold for both subtrees?
func (n *Node) isBST(min, max Interface) bool {
	if n == nil {
		return true
	}
	if n.Elem.Start().Compare(min.Start()) < 0 || n.Elem.Start().Compare(max.Start()) > 0 {
		return false
	}
	return n.Left.isBST(min, n.Elem) || n.Right.isBST(n.Elem, max)
}

// Test BU and TD234 invariants.
func (t *Tree) is23_234() bool {
	if t == nil {
		return true
	}
	return t.Root.is23_234()
}
func (n *Node) is23_234() bool {
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
func (t *Tree) isBalanced() bool {
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
func (n *Node) isBalanced(black int) bool {
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
func (t *Tree) isRanged() bool {
	if t == nil {
		return true
	}
	return t.Root.isRanged()
}
func (n *Node) isRanged() bool {
	if n == nil {
		return true
	}
	e, r := n.Elem, n.Range
	m := n.bounding(&overlap{start: e.Start().(compInt), end: e.End().(compInt)})
	return m.Start().Compare(r.Start()) == 0 && m.End().Compare(r.End()) == 0 &&
		n.Left.isRanged() &&
		n.Right.isRanged()
}
func (n *Node) bounding(m Mutable) Mutable {
	m.SetStart(min(n.Elem.Start(), m.Start()))
	m.SetEnd(max(n.Elem.End(), m.End()))
	if n.Left != nil {
		m = n.Left.bounding(m)
	}
	if n.Right != nil {
		m = n.Right.bounding(m)
	}
	return m
}

// Test helpers

type overRune rune

func (or overRune) Compare(b Comparable) int {
	return int(or) - int(b.(overRune))
}
func (or overRune) Overlap(b Range) bool {
	return or == b.(overRune)
}
func (or overRune) ID() uintptr           { return uintptr(or) } // Not semantically satisfying interface, but not used.
func (or overRune) Start() Comparable     { return or }
func (or overRune) End() Comparable       { return or }
func (or overRune) SetStart(_ Comparable) {}
func (or overRune) SetEnd(_ Comparable)   {}
func (or overRune) NewMutable() Mutable   { return or }

type compInt int

func (or compInt) Compare(b Comparable) int {
	return int(or - b.(compInt))
}

type overlap struct {
	start, end compInt
	id         uintptr
}

func (o *overlap) Overlap(b Range) bool {
	bc := b.(*overlap)
	return o.end > bc.start && o.start < bc.end
}
func (o *overlap) ID() uintptr           { return o.id }
func (o *overlap) Start() Comparable     { return o.start }
func (o *overlap) End() Comparable       { return o.end }
func (o *overlap) SetStart(c Comparable) { o.start = c.(compInt) }
func (o *overlap) SetEnd(c Comparable)   { o.end = c.(compInt) }
func (o *overlap) NewMutable() Mutable   { return &overlap{o.start, o.end, o.id} }
func (o *overlap) String() string        { return fmt.Sprintf("[%d,%d)", o.start, o.end) }

// Build a tree from a simplified Newick format returning the root node.
// Single letter node names only, no error checking and all nodes are full or leaf.
func makeTree(desc string) (n *Node) {
	var build func([]rune) (*Node, int)
	build = func(desc []rune) (cn *Node, i int) {
		if len(desc) == 0 || desc[0] == ';' {
			return nil, 0
		}

		var c int
		cn = &Node{}
		for {
			b := desc[i]
			i++
			if b == '(' {
				cn.Left, c = build(desc[i:])
				i += c
				continue
			}
			if b == ',' {
				cn.Right, c = build(desc[i:])
				i += c
				continue
			}
			if b == ')' {
				if cn.Left == nil && cn.Right == nil {
					return nil, i
				}
				continue
			}
			if b != ';' {
				cn.Elem = overRune(b)
				cn.Range = overRune(b)
			}
			return cn, i
		}

		panic("cannot reach")
	}

	n, _ = build([]rune(desc))
	if n.Left == nil && n.Right == nil {
		n = nil
	}

	return
}

// Return a Newick format description of a tree defined by a node
func (n *Node) describeTree(char, color bool) string {
	s := []rune(nil)

	var follow func(*Node)
	follow = func(n *Node) {
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
			if char {
				s = append(s, rune(n.Elem.(overRune)))
			} else {
				s = append(s, []rune(fmt.Sprintf("%d", n.Elem))...)
			}
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

func min(a, b Comparable) Comparable {
	if a.Compare(b) < 0 {
		return a
	}
	return b
}

func max(a, b Comparable) Comparable {
	if a.Compare(b) > 0 {
		return a
	}
	return b
}

// Tests
func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

func (s *S) SetUpSuite(c *check.C) {
	mode := []string{TD234: "Top-Down 2-3-4", BU23: "Bottom-Up 2-3"}
	fmt.Printf("Testing %s Left-Leaning Red Black Tree interval tree package.\n", mode[Mode])
}

func (s *S) TestMakeAndDescribeTree(c *check.C) {
	c.Check((*Node)(nil).describeTree(true, false), check.Equals, "();")
	for _, desc := range []string{
		"();",
		"((a,c)b,(e,g)f)d;",
	} {
		t := makeTree(desc)
		c.Check(t.describeTree(true, false), check.Equals, desc)
	}
}

// ((a,c)b,(e,g)f)d -rotL-> (((a,c)b,e)d,g)f
func (s *S) TestRotateLeft(c *check.C) {
	orig := "((a,c)b,(e,g)f)d;"
	rot := "(((a,c)b,e)d,g)f;"

	tree := makeTree(orig)

	tree = tree.rotateLeft()
	c.Check(tree.describeTree(true, false), check.Equals, rot)

	rotTree := makeTree(rot)
	c.Check(tree, check.DeepEquals, rotTree)
}

// ((a,c)b,(e,g)f)d -rotR-> (a,(c,(e,g)f)d)b
func (s *S) TestRotateRight(c *check.C) {
	orig := "((a,c)b,(e,g)f)d;"
	rot := "(a,(c,(e,g)f)d)b;"

	tree := makeTree(orig)

	tree = tree.rotateRight()
	c.Check(tree.describeTree(true, false), check.Equals, rot)

	rotTree := makeTree(rot)
	c.Check(tree, check.DeepEquals, rotTree)
}

func (s *S) TestNilOperations(c *check.C) {
	t := &Tree{}
	c.Check(t.Min(), check.Equals, nil)
	c.Check(t.Max(), check.Equals, nil)
	if Mode == TD234 {
		return
	}
	t.DeleteMin(false)
	c.Check(*t, check.Equals, Tree{})
	t.DeleteMax(false)
	c.Check(*t, check.Equals, Tree{})
}

func (s *S) TestRange(c *check.C) {
	t := &Tree{}
	for i, iv := range []*overlap{
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

func (s *S) TestInsertion(c *check.C) {
	var (
		min, max = compInt(0), compInt(1000)
		t        = &Tree{}
		length   = compInt(100)
	)
	for i := min; i <= max; i++ {
		t.Insert(&overlap{start: i, end: i + length}, false)
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
	c.Check(t.Min().Start(), check.DeepEquals, min)
	c.Check(t.Max().Start(), check.DeepEquals, max)
}

func (s *S) TestFastInsertion(c *check.C) {
	var (
		min, max = compInt(0), compInt(1000)
		t        = &Tree{}
		length   = compInt(100)
	)
	for i := min; i <= max; i++ {
		t.Insert(&overlap{start: i, end: i + length}, true)
		c.Check(t.Len(), check.Equals, int(i+1))
		c.Check(t.isBST(), check.Equals, true)
		c.Check(t.is23_234(), check.Equals, true)
		c.Check(t.isBalanced(), check.Equals, true)
	}
	t.AdjustRanges()
	c.Check(t.isRanged(), check.Equals, true)
	c.Check(t.Min().Start(), check.DeepEquals, min)
	c.Check(t.Max().Start(), check.DeepEquals, max)
}

func (s *S) TestDeletion(c *check.C) {
	var (
		min, max = compInt(0), compInt(1000)
		e        = int(max-min) + 1
		t        = &Tree{}
		length   = compInt(1)
	)
	for i := min; i <= max; i++ {
		t.Insert(&overlap{start: i, end: i + length, id: uintptr(i)}, false)
	}
	for i := min; i <= max; i++ {
		var dotString string
		if o := t.Get(&overlap{start: i, end: i + length}); o != nil {
			e--
		}
		if *genDot && t.Len() <= *dotLimit {
			dotString = t.dot(fmt.Sprintf("TestDeletion_before_del_%d", i))
		}
		t.Delete(&overlap{start: i, end: i + length, id: uintptr(i)}, false)
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
					err = (*Tree)(nil).dotFile(fmt.Sprintf("TestDeletion_before_del_%d", i), dotString)
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
	c.Check(*t, check.Equals, Tree{})
}

func (s *S) TestFastDeletion(c *check.C) {
	var (
		min, max = compInt(0), compInt(1000)
		t        = &Tree{}
		length   = compInt(1)
	)
	for i := min; i <= max; i++ {
		t.Insert(&overlap{start: i, end: i + length, id: uintptr(i)}, false)
	}
	for i := min; i <= max; i++ {
		t.Delete(&overlap{start: i, end: i + length, id: uintptr(i)}, true)
		c.Check(t.isBST(), check.Equals, true)
		c.Check(t.is23_234(), check.Equals, true)
		c.Check(t.isBalanced(), check.Equals, true)
		if i == max/2 {
			t.AdjustRanges()
			c.Check(t.isRanged(), check.Equals, true)
		}
	}
	c.Check(*t, check.Equals, Tree{})
}

func (s *S) TestGet(c *check.C) {
	var (
		min, max = compInt(0), compInt(1000)
		t        = &Tree{}
	)
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			t.Insert(&overlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			o := t.Get(&overlap{start: i, end: i + 1})
			c.Check(len(o), check.Equals, 1)                                // Check inserted elements are present.
			c.Check(o[0], check.DeepEquals, &overlap{start: i, end: i + 1}) // Check inserted elements are correct.
		} else {
			o := t.Get(&overlap{start: i, end: i + 1})
			c.Check(o, check.DeepEquals, []Interface(nil)) // Check inserted elements are absent.
		}
	}
}

func (s *S) TestFloor(c *check.C) {
	min, max := compInt(0), compInt(1000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 0 { // Insert even numbers only.
			t.Insert(&overlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i <= max; i++ {
		l, _ := t.Floor(&overlap{start: i, end: i + 1})
		if i&1 == 0 {
			c.Check(l, check.DeepEquals, &overlap{start: i, end: i + 1}) // Check even Floors are themselves.
		} else {
			c.Check(l, check.DeepEquals, &overlap{start: i - 1, end: i}) // Check odd Floors are the previous number.
		}
	}
	l, _ := t.Floor(&overlap{start: min - 1, end: min})
	c.Check(l, check.DeepEquals, Interface(nil))
}

func (s *S) TestCeil(c *check.C) {
	min, max := compInt(0), compInt(1000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 1 { // Insert odd numbers only.
			t.Insert(&overlap{start: i, end: i + 1}, false)
		}
	}
	for i := min; i < max; i++ {
		u, _ := t.Ceil(&overlap{start: i, end: i + 1})
		if i&1 == 1 {
			c.Check(u, check.DeepEquals, &overlap{start: i, end: i + 1}) // Check odd Ceils are themselves.
		} else {
			c.Check(u, check.DeepEquals, &overlap{start: i + 1, end: i + 2}) // Check even Ceils are the next number.
		}
	}
	u, _ := t.Ceil(&overlap{start: max, end: max + 2})
	c.Check(u, check.DeepEquals, Comparable(nil))
}

func (s *S) TestRandomlyInsertedGet(c *check.C) {
	var (
		count, max = 1000, 1000
		t          = &Tree{}
		length     = compInt(100)
		verify     = map[overlap]struct{}{}
		verified   = map[overlap]struct{}{}
	)
	for i := 0; i < count; i++ {
		s := compInt(rand.Intn(max))
		v := overlap{start: s, end: s + length}
		t.Insert(&v, false)
		verify[v] = struct{}{}
	}
	// Random fetch order.
	for v := range verify {
		o := t.Get(&v)
		c.Check(len(o), check.Not(check.Equals), 0) // Check inserted elements are present.
		for _, iv := range o {
			vr := *iv.(*overlap)
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
	for s := compInt(0); s <= compInt(max); s++ {
		v := overlap{start: s, end: s + length}
		o := t.Get(&v)
		if _, ok := verify[v]; ok {
			c.Check(len(o), check.Not(check.Equals), 0) // Check inserted elements are present.
		}
	}
}

func (s *S) TestRandomInsertion(c *check.C) {
	var (
		count, max = 1000, 1000
		t          = &Tree{}
		length     = compInt(100)
	)
	for i := 0; i < count; i++ {
		s := compInt(rand.Intn(max))
		v := overlap{start: s, end: s + length}
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

func (s *S) TestRandomDeletion(c *check.C) {
	var (
		count, max = 14, 3
		r          = make([]overlap, count)
		t          = &Tree{}
		length     = compInt(1)
	)
	for i := range r {
		s := compInt(rand.Intn(max))
		r[i] = overlap{start: s, end: s + length, id: uintptr(i)}
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
					err = (*Tree)(nil).dotFile(fmt.Sprintf("TestRandomDeletion_before_del_%d_%d_%d", i, v.start, v.end), dotString)
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
	c.Check(*t, check.DeepEquals, Tree{})
}

func (s *S) TestDeleteMinMax(c *check.C) {
	var (
		min, max = compInt(0), compInt(10)
		t        = &Tree{}
		length   = compInt(1)
		dI       int
	)
	for i := min; i <= max; i++ {
		v := overlap{start: i, end: i + length}
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
		failed = !c.Check(t.Min(), check.DeepEquals, &overlap{start: min, end: min + length})
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
		failed = !c.Check(t.Max(), check.DeepEquals, &overlap{start: max, end: max + length})
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
func (s *S) TestRangeBug(c *check.C) {
	var t Tree
	for i, e := range []*overlap{
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
func (s *S) TestIssue15(c *check.C) {
	ranges := []*overlap{
		{start: 5, end: 6},
		{start: 7, end: 8},
		{start: 9, end: 10},
		{start: 0, end: 4},
		{start: 0, end: 1},
		{start: 0, end: 1},
		{start: 0, end: 1},
	}

	var t Tree
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
				err := t.dotFile(fmt.Sprintf("Issue15Test_%02d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
		}
	}

	got := t.Get(&overlap{start: 2, end: 3})
	c.Check(len(got), check.Equals, 1, check.Commentf("Expected one overlap, got %d", len(got)))
}

func (s *S) TestInsertBug(c *check.C) {
	var t Tree
	for i := 10; i > 0; i-- {
		t.Insert(&overlap{start: 0, end: 1, id: uintptr(i)}, false)
	}
	c.Check(t.Len(), check.Equals, 10, check.Commentf("Expected 10 entries, got %d", t.Len()))
	for i := 1; i <= 10; i++ {
		t.Delete(&overlap{start: 0, end: 1, id: uintptr(i)}, false)
	}
	c.Check(t.Len(), check.Equals, 0, check.Commentf("Expected 0 entries, got %d", t.Len()))
}

func (t *Tree) dot(label string) string {
	if t == nil {
		return ""
	}
	var (
		arrows = map[llrb.Color]string{llrb.Red: "none", llrb.Black: "normal"}
		s      []string
		follow func(*Node)
	)
	follow = func(n *Node) {
		id := uintptr(unsafe.Pointer(n))
		c := fmt.Sprintf("%d[label = \"<Left> |<Elem> interval:%v\\nrange:%v\\nid:%d|<Right>\"];",
			id, n.Elem, n.Range, n.Elem.ID())
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

func (t *Tree) dotFile(label, dotString string) (err error) {
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

func BenchmarkInsert(b *testing.B) {
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
}

func BenchmarkFastInsert(b *testing.B) {
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, true)
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Get(&overlap{start: s, end: s + length})
	}
}

func BenchmarkMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < 1e5; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	var m Interface
	for i := compInt(0); i < N; i++ {
		m = t.Min()
	}
	_ = m
}

func BenchmarkMax(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < 1e5; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	var m Interface
	for i := compInt(0); i < N; i++ {
		m = t.Max()
	}
	_ = m
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(1)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Delete(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
}

func BenchmarkFastDelete(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(1)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Delete(&overlap{start: s, end: s + length, id: uintptr(s)}, true)
	}
}

func BenchmarkDeleteMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := compInt(0); i < N; i++ {
		t.DeleteMin(false)
	}
}

func BenchmarkFastDeleteMin(b *testing.B) {
	b.StopTimer()
	var (
		t      = &Tree{}
		length = compInt(10)
		N      = compInt(b.N)
	)
	for i := compInt(0); i < N; i++ {
		s := N - i
		t.Insert(&overlap{start: s, end: s + length, id: uintptr(s)}, false)
	}
	b.StartTimer()
	for i := compInt(0); i < N; i++ {
		t.DeleteMin(true)
	}
}
