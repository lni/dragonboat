// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"github.com/cespare/xxhash/v2"
)

func getCRC(data []byte) uint32 {
	return uint32(xxhash.Sum64(data))
}
