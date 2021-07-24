// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.
//
// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tan

import (
	"fmt"
)

func firstError(err1 error, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

func panicNow(err error) {
	panic(fmt.Sprintf("%+v", err))
}
