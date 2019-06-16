// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rsm

import (
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(int64(time.Now().UnixNano()))
}

func TestCountedWriteCanReportTotalWritten(t *testing.T) {
	cw := countedWriter{
		w: ioutil.Discard,
	}
	total := uint64(0)
	for i := 0; i < 16; i++ {
		sz := rand.Uint64() % 1024
		v := make([]byte, sz)
		total += sz
		_, err := cw.Write(v)
		if err != nil {
			t.Fatalf("write failed %v", err)
		}
	}
	if total != cw.total {
		t.Errorf("total %d, want %d", cw.total, total)
	}
}
