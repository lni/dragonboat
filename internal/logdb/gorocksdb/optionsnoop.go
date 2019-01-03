// +build !dragonboat_rdbpatched

package gorocksdb

func (opts *Options) EnablePipelinedWrite(value bool) {
}

func (opts *Options) SetMaxSubCompactions(value uint32) {
}

func (opts *Options) SetMemtableInsertWithHintFixedLengthPrefixExtractor(length int) {
}

func (opts *ReadOptions) IgnoreRangeDeletions(value bool) {
}

func (opts *CompactionOptions) SetForceBottommostLevelCompaction() {
}
