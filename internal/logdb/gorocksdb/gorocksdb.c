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

#include "gorocksdb.h"
#include "_cgo_export.h"

/* Base */

void dragonboat_destruct_handler(void* state) { }

/* Comparator */

rocksdb_comparator_t* dragonboat_comparator_create(uintptr_t idx) {
    return rocksdb_comparator_create(
        (void*)idx,
        dragonboat_destruct_handler,
        (int (*)(void*, const char*, size_t, const char*, size_t))(dragonboat_comparator_compare),
        (const char *(*)(void*))(dragonboat_comparator_name));
}

/* CompactionFilter */

rocksdb_compactionfilter_t* dragonboat_compactionfilter_create(uintptr_t idx) {
    return rocksdb_compactionfilter_create(
        (void*)idx,
        dragonboat_destruct_handler,
        (unsigned char (*)(void*, int, const char*, size_t, const char*, size_t, char**, size_t*, unsigned char*))(dragonboat_compactionfilter_filter),
        (const char *(*)(void*))(dragonboat_compactionfilter_name));
}

/* Filter Policy */

rocksdb_filterpolicy_t* dragonboat_filterpolicy_create(uintptr_t idx) {
    return rocksdb_filterpolicy_create(
        (void*)idx,
        dragonboat_destruct_handler,
        (char* (*)(void*, const char* const*, const size_t*, int, size_t*))(dragonboat_filterpolicy_create_filter),
        (unsigned char (*)(void*, const char*, size_t, const char*, size_t))(dragonboat_filterpolicy_key_may_match),
        dragonboat_filterpolicy_delete_filter,
        (const char *(*)(void*))(dragonboat_filterpolicy_name));
}

void dragonboat_filterpolicy_delete_filter(void* state, const char* v, size_t s) { }

/* Merge Operator */

rocksdb_mergeoperator_t* dragonboat_mergeoperator_create(uintptr_t idx) {
    return rocksdb_mergeoperator_create(
        (void*)idx,
        dragonboat_destruct_handler,
        (char* (*)(void*, const char*, size_t, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(dragonboat_mergeoperator_full_merge),
        (char* (*)(void*, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(dragonboat_mergeoperator_partial_merge_multi),
        dragonboat_mergeoperator_delete_value,
        (const char* (*)(void*))(dragonboat_mergeoperator_name));
}

void dragonboat_mergeoperator_delete_value(void* id, const char* v, size_t s) { }

/* Slice Transform */

rocksdb_slicetransform_t* dragonboat_slicetransform_create(uintptr_t idx) {
    return rocksdb_slicetransform_create(
    	(void*)idx,
    	dragonboat_destruct_handler,
    	(char* (*)(void*, const char*, size_t, size_t*))(dragonboat_slicetransform_transform),
    	(unsigned char (*)(void*, const char*, size_t))(dragonboat_slicetransform_in_domain),
    	(unsigned char (*)(void*, const char*, size_t))(dragonboat_slicetransform_in_range),
    	(const char* (*)(void*))(dragonboat_slicetransform_name));
}
