// +build dragonboat_rdbpatched

package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

func (opts *Options) EnablePipelinedWrite(value bool) {
	C.rocksdb_options_set_enable_pipelined_write(opts.c, boolToChar(value))
}

func (opts *Options) SetMaxSubCompactions(value uint32) {
	C.rocksdb_options_set_max_subcompactions(opts.c, C.uint32_t(value))
}

func (opts *Options) SetMemtableInsertWithHintFixedLengthPrefixExtractor(length int) {
	C.rocksdb_options_set_memtable_insert_with_hint_fixed_length_prefix_extractor(opts.c, C.size_t(length))
}

func (opts *ReadOptions) IgnoreRangeDeletions(value bool) {
	C.rocksdb_readoptions_set_ignore_range_deletions(opts.c, boolToChar(value))
}

func (opts *CompactionOptions) SetForceBottommostLevelCompaction() {
	C.rocksdb_compactoptions_set_force_bottommost_level_compaction(opts.c)
}
