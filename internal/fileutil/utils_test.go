// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package fileutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/internal/vfs"
)

func TestTempDir(t *testing.T) {
	dir1, err := TempDir("", "test-dir", vfs.DefaultFS)
	require.NoError(t, err)
	dir2, err := TempDir("", "test-dir", vfs.DefaultFS)
	require.NoError(t, err)
	require.NotEqual(t, dir1, dir2)
}
