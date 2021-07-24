// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"github.com/lni/vfs"
)

const (
	// MaxManifestFileSize is the default max manifest file size
	MaxManifestFileSize int64 = 1024 * 1024 * 2
	// MaxLogFileSize is the default max log file size
	MaxLogFileSize int64 = 1024 * 1024 * 64
)

// Options is the option type used by tan
type Options struct {
	MaxLogFileSize      int64
	MaxManifestFileSize int64
	FS                  vfs.FS
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified. Returns the new options.
func (o *Options) EnsureDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.MaxLogFileSize == 0 {
		o.MaxLogFileSize = MaxLogFileSize
	}
	if o.MaxManifestFileSize == 0 {
		o.MaxManifestFileSize = MaxManifestFileSize
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	return o
}
