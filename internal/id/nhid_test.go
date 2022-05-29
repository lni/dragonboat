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

package id

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNodeHostID(t *testing.T) {
	v := New()
	assert.True(t, IsNodeHostID(v.String()))
	assert.False(t, IsNodeHostID("this is not a uuid"))
}

func TestNew(t *testing.T) {
	values := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		u := New()
		values[u.String()] = struct{}{}
	}
	assert.Equal(t, 1000, len(values))
}

func TestNewUUID(t *testing.T) {
	u := New()
	v, err := NewUUID(u.String())
	assert.NoError(t, err)
	assert.Equal(t, u.String(), v.String())
}

func TestMarshalUnMarshal(t *testing.T) {
	v := New()
	data, err := v.Marshal()
	assert.NoError(t, err)
	v2 := New()
	assert.NoError(t, v2.Unmarshal(data))
	assert.Equal(t, v.String(), v2.String())

	v3 := New()
	data, err = v3.Marshal()
	assert.NoError(t, err)
	data2 := make([]byte, len(data))
	l, err := v3.MarshalTo(data2)
	assert.NoError(t, err)
	assert.Equal(t, len(data), l)
	assert.Equal(t, data, data2)
}
