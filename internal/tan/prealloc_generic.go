// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

// +build !linux

package tan

import (
	"github.com/lni/vfs"
)

func prealloc(f vfs.File, size int64) error {
	return nil
}
