/*
Copyright (C) 2016 Thomas Adam

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// This file is modified by the dragonboat project
// exported names have been updated from gorocksdb_* to dragonboat_*
// so user applications can use the gorocksdb package as well.

package gorocksdb

// #include "rocksdb/c.h"
import "C"

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.
type Comparator interface {
	// Three-way comparison. Returns value:
	//   < 0 iff "a" < "b",
	//   == 0 iff "a" == "b",
	//   > 0 iff "a" > "b"
	Compare(a, b []byte) int

	// The name of the comparator.
	Name() string
}

// NewNativeComparator creates a Comparator object.
func NewNativeComparator(c *C.rocksdb_comparator_t) Comparator {
	return nativeComparator{c}
}

type nativeComparator struct {
	c *C.rocksdb_comparator_t
}

func (c nativeComparator) Compare(a, b []byte) int { return 0 }
func (c nativeComparator) Name() string            { return "" }

// Hold references to comperators.
var comperators []Comparator

func registerComperator(cmp Comparator) int {
	comperators = append(comperators, cmp)
	return len(comperators) - 1
}

//export dragonboat_comparator_compare
func dragonboat_comparator_compare(idx int, cKeyA *C.char, cKeyALen C.size_t, cKeyB *C.char, cKeyBLen C.size_t) C.int {
	keyA := charToByte(cKeyA, cKeyALen)
	keyB := charToByte(cKeyB, cKeyBLen)
	return C.int(comperators[idx].Compare(keyA, keyB))
}

//export dragonboat_comparator_name
func dragonboat_comparator_name(idx int) *C.char {
	return stringToChar(comperators[idx].Name())
}
