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

#include <stdlib.h>
#include "rocksdb/c.h"

// This API provides convenient C wrapper functions for rocksdb client.

/* Base */

extern void dragonboat_destruct_handler(void* state);

/* CompactionFilter */

extern rocksdb_compactionfilter_t* dragonboat_compactionfilter_create(uintptr_t idx);

/* Comparator */

extern rocksdb_comparator_t* dragonboat_comparator_create(uintptr_t idx);

/* Filter Policy */

extern rocksdb_filterpolicy_t* dragonboat_filterpolicy_create(uintptr_t idx);
extern void dragonboat_filterpolicy_delete_filter(void* state, const char* v, size_t s);

/* Merge Operator */

extern rocksdb_mergeoperator_t* dragonboat_mergeoperator_create(uintptr_t idx);
extern void dragonboat_mergeoperator_delete_value(void* state, const char* v, size_t s);

/* Slice Transform */

extern rocksdb_slicetransform_t* dragonboat_slicetransform_create(uintptr_t idx);
