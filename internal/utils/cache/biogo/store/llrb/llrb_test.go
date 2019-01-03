// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package llrb

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"gopkg.in/check.v1"
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
func (n *Node) isBST(min, max Comparable) bool {
	if n == nil {
		return true
	}
	if n.Elem.Compare(min) < 0 || n.Elem.Compare(max) > 0 {
		return false
	}
	return n.Left.isBST(min, n.Elem) && n.Right.isBST(n.Elem, max)
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
			if n.Left.color() == Red && n.Right.color() == Red {
				return false
			}
		}
		// and the red node should really should be the left one.
		if n.Right.color() == Red {
			return false
		}
	} else if Mode == TD234 {
		// This test is altered from that shown in the java since the trees
		// shown in the paper do not conform to the test as it existed and the
		// current situation does not break the 2-3-4 definition of the LLRB.
		if n.Right.color() == Red && n.Left.color() == Black {
			return false
		}
	} else {
		panic("cannot reach")
	}
	if n.color() == Red && n.Left.color() == Red {
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
		if x.color() == Black {
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
	if n.color() == Black {
		black--
	}
	return n.Left.isBalanced(black) && n.Right.isBalanced(black)
}

// Test helpers

type compRune rune

func (cr compRune) Compare(r Comparable) int {
	return int(cr) - int(r.(compRune))
}

type compInt int

func (ci compInt) Compare(i Comparable) (c int) {
	switch i := i.(type) {
	case compIntUpper:
		c = int(ci) - int(i)
	case compInt:
		c = int(ci) - int(i)
	}
	return c
}

type compIntUpper int

func (ci compIntUpper) Compare(i Comparable) (c int) {
	switch i := i.(type) {
	case compIntUpper:
		c = int(ci) - int(i)
	case compInt:
		c = int(ci) - int(i)
	}
	if c == 0 {
		return 1
	}
	return c
}

type Reverse struct {
	sort.Interface
}

func (r Reverse) Less(i, j int) bool { return r.Interface.Less(j, i) }

type compInts []compInt

func (c compInts) Len() int           { return len(c) }
func (c compInts) Less(i, j int) bool { return c[i].Compare(c[j]) < 0 }
func (c compInts) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

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
				cn.Elem = compRune(b)
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
func describeTree(n *Node, char, color bool) string {
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
				s = append(s, rune(n.Elem.(compRune)))
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

// Tests
func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

func (s *S) SetUpSuite(c *check.C) {
	mode := []string{TD234: "Top-Down 2-3-4", BU23: "Bottom-Up 2-3"}
	fmt.Printf("Testing %s Left-Leaning Red Black Tree package.\n", mode[Mode])
}

func (s *S) TestMakeAndDescribeTree(c *check.C) {
	c.Check(describeTree((*Node)(nil), true, false), check.Equals, "();")
	for _, desc := range []string{
		"();",
		"((a,c)b,(e,g)f)d;",
	} {
		t := makeTree(desc)
		c.Check(describeTree(t, true, false), check.Equals, desc)
	}
}

// ((a,c)b,(e,g)f)d -rotL-> (((a,c)b,e)d,g)f
func (s *S) TestRotateLeft(c *check.C) {
	orig := "((a,c)b,(e,g)f)d;"
	rot := "(((a,c)b,e)d,g)f;"

	tree := makeTree(orig)

	tree = tree.rotateLeft()
	c.Check(describeTree(tree, true, false), check.Equals, rot)

	rotTree := makeTree(rot)
	c.Check(tree, check.DeepEquals, rotTree)
}

// ((a,c)b,(e,g)f)d -rotR-> (a,(c,(e,g)f)d)b
func (s *S) TestRotateRight(c *check.C) {
	orig := "((a,c)b,(e,g)f)d;"
	rot := "(a,(c,(e,g)f)d)b;"

	tree := makeTree(orig)

	tree = tree.rotateRight()
	c.Check(describeTree(tree, true, false), check.Equals, rot)

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
	t.DeleteMin()
	c.Check(*t, check.Equals, Tree{})
	t.DeleteMax()
	c.Check(*t, check.Equals, Tree{})
}

func (s *S) TestInsertion(c *check.C) {
	min, max := compRune(0), compRune(1000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		t.Insert(i)
		c.Check(t.Len(), check.Equals, int(i+1))
		failed := false
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := dotFile(t, fmt.Sprintf("TestInsertion_after_ins_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
	c.Check(t.Min(), check.Equals, compRune(min))
	c.Check(t.Max(), check.Equals, compRune(max))
}

func (s *S) TestDeletion(c *check.C) {
	min, max := compRune(0), compRune(10000)
	e := int(max-min) + 1
	t := &Tree{}
	for i := min; i <= max; i++ {
		t.Insert(i)
	}
	for i := min; i <= max; i++ {
		var dotString string
		if t.Get(i) != nil {
			e--
		}
		if *genDot && t.Len() <= *dotLimit {
			dotString = dot(t, fmt.Sprintf("TestDeletion_before_%d", i))
		}
		t.Delete(i)
		c.Check(t.Len(), check.Equals, e)
		if i < max {
			failed := false
			failed = failed || !c.Check(t.isBST(), check.Equals, true)
			failed = failed || !c.Check(t.is23_234(), check.Equals, true)
			failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
			if failed {
				if *printTree {
					c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
				}
				if *genDot && t.Len() < *dotLimit {
					var err error
					err = dotFile(nil, fmt.Sprintf("TestDeletion_before_del_%d", i), dotString)
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
					err = dotFile(t, fmt.Sprintf("TestDeletion_after_del_%d", i), "")
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

func (s *S) TestGet(c *check.C) {
	min, max := compRune(0), compRune(100000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			t.Insert(i)
		}
	}
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			c.Check(t.Get(i), check.Equals, compRune(i)) // Check inserted elements are present.
		} else {
			c.Check(t.Get(i), check.Equals, Comparable(nil)) // Check inserted elements are absent.
		}
	}
}

func (s *S) TestFloor(c *check.C) {
	min, max := compRune(0), compRune(100000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 0 { // Insert even numbers only.
			t.Insert(i)
		}
	}
	for i := min; i <= max; i++ {
		if i&1 == 0 {
			c.Check(t.Floor(i), check.Equals, compRune(i)) // Check even Floors are themselves.
		} else {
			c.Check(t.Floor(i), check.Equals, compRune(i-1)) // Check odd Floors are the previous number.
		}
	}
	c.Check(t.Floor(min-1), check.Equals, Comparable(nil))
}

func (s *S) TestCeil(c *check.C) {
	min, max := compRune(0), compRune(100000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 1 { // Insert odd numbers only.
			t.Insert(i)
		}
	}
	for i := min; i < max; i++ {
		if i&1 == 1 {
			c.Check(t.Ceil(i), check.Equals, compRune(i)) // Check odd Ceils are themselves.
		} else {
			c.Check(t.Ceil(i), check.Equals, compRune(i+1)) // Check even Ceils are the next number.
		}
	}
	c.Check(t.Ceil(max+1), check.Equals, Comparable(nil))
}

func (s *S) TestUpper(c *check.C) {
	min, max := compInt(0), compInt(100000)
	t := &Tree{}
	for i := min; i <= max; i++ {
		if i&1 == 1 { // Insert odd numbers only.
			t.Insert(i)
		}
	}
	for i := min; i < max-1; i++ {
		if i&1 == 1 {
			c.Check(t.Ceil(compIntUpper(i)), check.Equals, compInt(i+2)) // Check odd Uppers are the next odd.
		} else {
			c.Check(t.Ceil(compIntUpper(i)), check.Equals, compInt(i+1)) // Check even Uppers are the next number.
		}
	}
	c.Check(t.Ceil(compIntUpper(max+1)), check.Equals, Comparable(nil))
}

func (s *S) TestRandomlyInsertedGet(c *check.C) {
	count, max := 100000, 1000
	t := &Tree{}
	verify := map[rune]struct{}{}
	for i := 0; i < count; i++ {
		v := compRune(rand.Intn(max))
		t.Insert(v)
		verify[rune(v)] = struct{}{}
	}
	// Random fetch order - check only those inserted.
	for v := range verify {
		c.Check(t.Get(compRune(v)), check.Equals, compRune(v)) // Check inserted elements are present.
	}
	// Check all possible insertions.
	for i := compRune(0); i <= compRune(max); i++ {
		if _, ok := verify[rune(i)]; ok {
			c.Check(t.Get(i), check.Equals, compRune(i)) // Check inserted elements are present.
		} else {
			c.Check(t.Get(i), check.Equals, Comparable(nil)) // Check inserted elements are absent.
		}
	}
}

func (s *S) TestRandomInsertion(c *check.C) {
	count, max := 100000, 1000
	t := &Tree{}
	for i := 0; i < count; i++ {
		r := rand.Intn(max)
		t.Insert(compRune(r))
		failed := false
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := dotFile(t, fmt.Sprintf("TestRandomInsertion_after_ins_%d", r), "")
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
		count, max = 100000, 1000
		r          = make([]compRune, count)
		t          = &Tree{}
	)
	for i := range r {
		r[i] = compRune(rand.Intn(max))
		t.Insert(r[i])
	}
	for _, v := range r {
		t.Delete(v)
		if t != nil {
			failed := false
			failed = failed || !c.Check(t.isBST(), check.Equals, true)
			failed = failed || !c.Check(t.is23_234(), check.Equals, true)
			failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
			if failed {
				if *printTree {
					c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
				}
				if *genDot && t.Len() <= *dotLimit {
					err := dotFile(t, fmt.Sprintf("TestRandomDeletion_after_del_%d", v), "")
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

type compStructUpper struct {
	key int
	val byte
}

func (cs compStructUpper) Compare(s Comparable) (c int) {
	c = cs.key - s.(compStructUpper).key
	if c == 0 {
		c = int(cs.val) - int(s.(compStructUpper).val)
	}
	return c
}
func (cs compStructUpper) String() string { return fmt.Sprintf("[%d:%c]", cs.key, cs.val) }

func (s *S) TestNonUniqueDeletion(c *check.C) {
	var (
		r = []compStructUpper{{1, 'a'}, {1, 'b'}, {1, 'c'}, {0, 'd'}}
		t = &Tree{}
	)
	for i, v := range r {
		t.Insert(v)
		c.Check(t.Len(), check.Equals, i+1)
	}
	for _, v := range r {
		var dotString string
		if *genDot && t.Len() <= *dotLimit {
			dotString = dot(t, "TestNonUniqueDeletion_before_del")
		}
		t.Delete(v)
		if t != nil {
			failed := false
			failed = failed || !c.Check(t.is23_234(), check.Equals, true)
			failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
			if failed {
				if *printTree {
					c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
				}
				if *genDot && t.Len() <= *dotLimit {
					var err error
					err = dotFile(nil, "TestNonUniqueDeletion_before_del", dotString)
					if err != nil {
						c.Errorf("Dot file write failed: %v", err)
					}
					err = dotFile(t, "TestNonUniqueDeletion_after_del", "")
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

func (s *S) TestDeleteMinMax(c *check.C) {
	var (
		min, max = compRune(0), compRune(10)
		t        = &Tree{}
		dI       int
	)
	for i := min; i <= max; i++ {
		t.Insert(i)
		dI = t.Len()
	}
	c.Check(dI, check.Equals, int(max-min+1))
	for i, m := 0, int(max); i < m/2; i++ {
		var failed bool
		t.DeleteMin()
		dI--
		c.Check(t.Len(), check.Equals, dI)
		min++
		failed = !c.Check(t.Min(), check.Equals, min)
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := dotFile(t, fmt.Sprintf("TestDeleteMinMax_after_delmin_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
		t.DeleteMax()
		dI--
		c.Check(t.Len(), check.Equals, dI)
		max--
		failed = !c.Check(t.Max(), check.Equals, max)
		failed = failed || !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := dotFile(t, fmt.Sprintf("TestDeleteMinMax_after_delmax_%d", i), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
}

func (s *S) TestRandomInsertionDeletion(c *check.C) {
	var (
		count, max = 100000, 1000
		t          = &Tree{}
		verify     = map[int]struct{}{}
	)
	for i := 0; i < count; i++ {
		var (
			failed    bool
			rI, rD    int
			dotString string
		)
		if rand.Float64() < 0.5 {
			rI = rand.Intn(max)
			t.Insert(compRune(rI))
			verify[rI] = struct{}{}
			c.Check(t.Len(), check.Equals, len(verify))
		}
		failed = !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if *genDot && t.Len() <= *dotLimit {
			dotString = dot(t, fmt.Sprintf("TestRandomInsertionDeletion_after_ins_%d_%d", i, rI))
		}
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				err := dotFile(nil, fmt.Sprintf("TestRandomInsertionDeletion_after_ins_%d_%d", i, rI), dotString)
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
		if rand.Float64() < 0.5 {
			rD = rand.Intn(max)
			t.Delete(compRune(rD))
			delete(verify, rD)
			c.Check(t.Len(), check.Equals, len(verify))
		} else {
			continue
		}
		failed = !c.Check(t.isBST(), check.Equals, true)
		failed = failed || !c.Check(t.is23_234(), check.Equals, true)
		failed = failed || !c.Check(t.isBalanced(), check.Equals, true)
		if failed {
			if *printTree {
				c.Logf("Failing tree: %s\n\n", describeTree(t.Root, false, true))
			}
			if *genDot && t.Len() <= *dotLimit {
				var err error
				err = dotFile(nil, fmt.Sprintf("TestRandomInsertionDeletion_after_ins_%d_%d", i, rI), dotString)
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
				err = dotFile(t, fmt.Sprintf("TestRandomInsertionDeletion_after_del_%d_%d", i, rD), "")
				if err != nil {
					c.Errorf("Dot file write failed: %v", err)
				}
			}
			c.Fatal("Cannot continue test: invariant contradiction")
		}
	}
}

func (s *S) TestDeleteRight(c *check.C) {
	type target struct {
		min, max, target compRune
	}
	for _, r := range []target{
		{0, 14, -1},
		{0, 14, 14},
		{0, 14, 15},
		{0, 15, -1},
		{0, 15, 15},
		{0, 15, 16},
		{0, 16, -1},
		{0, 16, 15},
		{0, 16, 16},
		{0, 16, 17},
		{0, 17, -1},
		{0, 17, 16},
		{0, 17, 17},
		{0, 17, 18},
	} {
		var format, dotString string
		t := &Tree{}
		for i := r.min; i <= r.max; i++ {
			t.Insert(i)
		}
		before := describeTree(t.Root, false, true)
		format = "Before deletion: %#v %s"
		ok := checkTree(t, c, format, r, before)
		if !ok {
			c.Fatal("Cannot continue test: invariant contradiction")
		}
		if *genDot && t.Len() <= *dotLimit {
			dotString = dot(t, strings.Replace(
				fmt.Sprintf("TestDeleteRight_%s_before_del_%d_%d_%d", modeName[Mode], r.min, r.max, r.target),
				"-", "_", -1))
		}
		t.Delete(r.target)
		if r.min <= r.target && r.target <= r.max {
			c.Check(t.Len(), check.Equals, int(r.max-r.min)) // Key in tree.
		} else {
			c.Check(t.Len(), check.Equals, int(r.max-r.min)+1) // Key not in tree.
		}
		format = "%#v\nBefore deletion: %s\nAfter deletion:  %s"
		ok = checkTree(t, c, format, r, before, describeTree(t.Root, false, true))
		if !ok && *genDot && t.Len() < *dotLimit {
			var err error
			err = dotFile(nil, strings.Replace(
				fmt.Sprintf("TestDeleteRight_%s_before_del_%d_%d_%d", modeName[Mode], r.min, r.max, r.target),
				"-", "_", -1), dotString)
			if err != nil {
				c.Errorf("Dot file write failed: %v", err)
			}
			err = dotFile(t, strings.Replace(
				fmt.Sprintf("TestDeleteRight_%s_after_del_%d_%d_%d", modeName[Mode], r.min, r.max, r.target),
				"-", "_", -1), "")
			if err != nil {
				c.Errorf("Dot file write failed: %v", err)
			}
		}
	}
}

func checkTree(t *Tree, c *check.C, f string, i ...interface{}) (ok bool) {
	comm := check.Commentf(f, i...)
	ok = true
	ok = ok && c.Check(t.isBST(), check.Equals, true, comm)
	ok = ok && c.Check(t.is23_234(), check.Equals, true, comm)
	ok = ok && c.Check(t.isBalanced(), check.Equals, true, comm)
	return
}

var (
	modeName = []string{TD234: "TD234", BU23: "BU23"}
	arrows   = map[Color]string{Red: "none", Black: "normal"}
)

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
		c := fmt.Sprintf("%d[label = \"<Left> |<Elem> %v|<Right>\"];", id, n.Elem)
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

// Test values for range methods.
var values = compInts{-10, -32, 100, 46, 239, 2349, 101, 0, 1}

func (s *S) TestDo(c *check.C) {
	values := append(compInts(nil), values...)
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
	}
	var result compInts
	f := func(c Comparable) (done bool) {
		result = append(result, c.(compInt))
		return
	}
	killed := t.Do(f)
	sort.Sort(values)
	c.Check(result, check.DeepEquals, values)
	c.Check(killed, check.Equals, false)
}

func (s *S) TestDoShortCircuit(c *check.C) {
	values := append(compInts(nil), values...)
	elem := 3
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
	}
	sort.Sort(values)
	target := values[elem]
	var result compInts
	f := func(c Comparable) (done bool) {
		result = append(result, c.(compInt))
		if target.Compare(c) == 0 {
			done = true
		}
		return
	}
	killed := t.Do(f)
	c.Check(result, check.DeepEquals, values[:elem+1])
	c.Check(killed, check.Equals, true)
}

func (s *S) TestDoReverse(c *check.C) {
	values := append(compInts(nil), values...)
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
	}
	var result compInts
	f := func(c Comparable) (done bool) {
		result = append(result, c.(compInt))
		return
	}
	killed := t.DoReverse(f)
	sort.Sort(Reverse{values})
	c.Check(result, check.DeepEquals, values)
	c.Check(killed, check.Equals, false)
}

func (s *S) TestDoRange(c *check.C) {
	values := append(compInts(nil), values...)
	lo, hi := compInt(0), compInt(100)
	var limValues compInts
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
		if v >= lo && v < hi {
			limValues = append(limValues, v)
		}
	}
	var result compInts
	f := func(c Comparable) (done bool) {
		result = append(result, c.(compInt))
		return
	}
	killed := t.DoRange(f, lo, hi)
	sort.Sort(limValues)
	c.Check(result, check.DeepEquals, limValues)
	c.Check(killed, check.Equals, false)
}

func (s *S) TestDoRangeReverse(c *check.C) {
	values := append(compInts(nil), values...)
	lo, hi := compInt(0), compInt(100)
	var limValues compInts
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
		if v >= lo && v < hi {
			limValues = append(limValues, v)
		}
	}
	var result compInts
	f := func(c Comparable) (done bool) {
		result = append(result, c.(compInt))
		return
	}
	killed := t.DoRangeReverse(f, hi, lo)
	sort.Sort(Reverse{limValues})
	c.Check(result, check.DeepEquals, limValues)
	c.Check(killed, check.Equals, false)
}

func (s *S) TestDoMatch(c *check.C) {
	values := append(compInts(nil), values...)
	elem := 3
	t := &Tree{}
	for _, v := range values {
		t.Insert(v)
	}
	sort.Sort(values)
	target := values[elem]
	f := func(r Comparable) (done bool) {
		c.Check(r, check.Equals, target)
		return
	}
	killed := t.DoMatching(f, target)
	c.Check(killed, check.Equals, false)
}

// Benchmarks

func BenchmarkInsert(b *testing.B) {
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(compInt(b.N - i))
	}
}

func BenchmarkInsertNoRep(b *testing.B) {
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(compIntUpper(b.N - i))
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(compInt(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.Get(compInt(i))
	}
}

func BenchmarkMin(b *testing.B) {
	b.StopTimer()
	t := &Tree{}
	for i := 0; i < 1e5; i++ {
		t.Insert(compInt(i))
	}
	b.StartTimer()
	var m Comparable
	for i := 0; i < b.N; i++ {
		m = t.Min()
	}
	_ = m
}

func BenchmarkMax(b *testing.B) {
	b.StopTimer()
	t := &Tree{}
	for i := 0; i < 1e5; i++ {
		t.Insert(compInt(i))
	}
	b.StartTimer()
	var m Comparable
	for i := 0; i < b.N; i++ {
		m = t.Max()
	}
	_ = m
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(compInt(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.Delete(compInt(i))
	}
}

func BenchmarkDeleteMin(b *testing.B) {
	b.StopTimer()
	t := &Tree{}
	for i := 0; i < b.N; i++ {
		t.Insert(compInt(b.N - i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t.DeleteMin()
	}
}

// Benchmarks for comparison to the built-in type.

func BenchmarkInsertMap(b *testing.B) {
	var m = map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		m[i] = struct{}{}
	}
}

func BenchmarkGetMap(b *testing.B) {
	b.StopTimer()
	var m = map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		m[i] = struct{}{}
	}
	b.StartTimer()
	var r struct{}
	for i := 0; i < b.N; i++ {
		r = m[i]
	}
	_ = r
}

func BenchmarkDeleteMap(b *testing.B) {
	b.StopTimer()
	var m = map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		m[i] = struct{}{}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		delete(m, i)
	}
}
