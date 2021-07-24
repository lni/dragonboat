// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

// +build linux

package tan

import (
	"os"
	"syscall"

	"github.com/lni/vfs"
	"golang.org/x/sys/unix"
)

func prealloc(f vfs.File, size int64) error {
	osf, ok := f.(*os.File)
	if !ok {
		return nil
	}
	return syscall.Fallocate(int(osf.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
}
