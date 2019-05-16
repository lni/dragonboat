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

// A CompactionFilter can be used to filter keys during compaction time.
type CompactionFilter interface {
	// If the Filter function returns false, it indicates
	// that the kv should be preserved, while a return value of true
	// indicates that this key-value should be removed from the
	// output of the compaction. The application can inspect
	// the existing value of the key and make decision based on it.
	//
	// When the value is to be preserved, the application has the option
	// to modify the existing value and pass it back through a new value.
	// To retain the previous value, simply return nil
	//
	// If multithreaded compaction is being used *and* a single CompactionFilter
	// instance was supplied via SetCompactionFilter, this the Filter function may be
	// called from different threads concurrently. The application must ensure
	// that the call is thread-safe.
	Filter(level int, key, val []byte) (remove bool, newVal []byte)

	// The name of the compaction filter, for logging
	Name() string
}

// NewNativeCompactionFilter creates a CompactionFilter object.
func NewNativeCompactionFilter(c *C.rocksdb_comparator_t) Comparator {
	return nativeComparator{c}
}

type nativeCompactionFilter struct {
	c *C.rocksdb_compactionfilter_t
}

func (c nativeCompactionFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	return false, nil
}
func (c nativeCompactionFilter) Name() string { return "" }

// Hold references to compaction filters.
var compactionFilters []CompactionFilter

func registerCompactionFilter(filter CompactionFilter) int {
	compactionFilters = append(compactionFilters, filter)
	return len(compactionFilters) - 1
}

//export dragonboat_compactionfilter_filter
func dragonboat_compactionfilter_filter(idx int, cLevel C.int, cKey *C.char, cKeyLen C.size_t, cVal *C.char, cValLen C.size_t, cNewVal **C.char, cNewValLen *C.size_t, cValChanged *C.uchar) C.int {
	key := charToByte(cKey, cKeyLen)
	val := charToByte(cVal, cValLen)

	remove, newVal := compactionFilters[idx].Filter(int(cLevel), key, val)
	if remove {
		return C.int(1)
	} else if newVal != nil {
		*cNewVal = byteToChar(newVal)
		*cNewValLen = C.size_t(len(newVal))
		*cValChanged = C.uchar(1)
	}
	return C.int(0)
}

//export dragonboat_compactionfilter_name
func dragonboat_compactionfilter_name(idx int) *C.char {
	return stringToChar(compactionFilters[idx].Name())
}
