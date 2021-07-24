// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tan

import (
	"sync"
	"testing"
)

func TestVersionUnref(t *testing.T) {
	list := &versionList{}
	list.init(&sync.Mutex{})
	v := &version{deleted: func([]*fileMetadata) {}}
	v.ref()
	list.pushBack(v)
	v.unref()
	if !list.empty() {
		t.Fatalf("expected version list to be empty")
	}
}
