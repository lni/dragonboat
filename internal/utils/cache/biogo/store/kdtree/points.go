// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kdtree

import (
	"math"
)

var (
	_ Interface  = Points{}
	_ Comparable = Point{}
)

// Randoms is the maximum number of random values to sample for calculation of median of
// random elements.
var Randoms = 100

// A Point represents a point in a k-d space that satisfies the Comparable interface.
type Point []float64

// Compare ...
func (p Point) Compare(c Comparable, d Dim) float64 { q := c.(Point); return p[d] - q[d] }

// Dims ...
func (p Point) Dims() int { return len(p) }

// Distance ...
func (p Point) Distance(c Comparable) float64 {
	q := c.(Point)
	var sum float64
	for dim, c := range p {
		d := c - q[dim]
		sum += d * d
	}
	return sum
}

// Extend ...
func (p Point) Extend(b *Bounding) *Bounding {
	if b == nil {
		b = &Bounding{append(Point(nil), p...), append(Point(nil), p...)}
	}
	min := b[0].(Point)
	max := b[1].(Point)
	for d, v := range p {
		min[d] = math.Min(min[d], v)
		max[d] = math.Max(max[d], v)
	}
	*b = Bounding{min, max}
	return b
}

// A Points is a collection of point values that satisfies the Interface.
type Points []Point

// Bounds ...
func (p Points) Bounds() *Bounding {
	if p.Len() == 0 {
		return nil
	}
	min := append(Point(nil), p[0]...)
	max := append(Point(nil), p[0]...)
	for _, e := range p[1:] {
		for d, v := range e {
			min[d] = math.Min(min[d], v)
			max[d] = math.Max(max[d], v)
		}
	}
	return &Bounding{min, max}
}

// Index ...
func (p Points) Index(i int) Comparable { return p[i] }

// Len ...
func (p Points) Len() int { return len(p) }

// Pivot ...
func (p Points) Pivot(d Dim) int { return Plane{Points: p, Dim: d}.Pivot() }

// Slice ...
func (p Points) Slice(start, end int) Interface { return p[start:end] }

// Plane is a wrapping type that allows a Points type be pivoted on a dimension.
type Plane struct {
	Dim
	Points
}

// Less ...
func (p Plane) Less(i, j int) bool { return p.Points[i][p.Dim] < p.Points[j][p.Dim] }

// Pivot ...
func (p Plane) Pivot() int { return Partition(p, MedianOfRandoms(p, Randoms)) }

// Slice ...
func (p Plane) Slice(start, end int) SortSlicer { p.Points = p.Points[start:end]; return p }

// Swap ...
func (p Plane) Swap(i, j int) {
	p.Points[i], p.Points[j] = p.Points[j], p.Points[i]
}
