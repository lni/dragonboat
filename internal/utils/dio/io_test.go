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

package dio

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type wc struct{}

func (wc) Write(data []byte) (int, error) {
	return len(data), nil
}

func (wc) Close() error { return nil }

func TestCountedWriterCountsWrittenBytes(t *testing.T) {
	w := NewCountedWriter(&wc{})
	n, err := w.Write(make([]byte, 128))
	require.Equal(t, 128, n)
	require.NoError(t, err)
	n, err = w.Write(make([]byte, 1))
	require.Equal(t, 1, n)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.Equal(t, uint64(129), w.BytesWritten())
}

func TestCountedWriteMustBeClosedFirst(t *testing.T) {
	w := NewCountedWriter(&wc{})
	n, err := w.Write(make([]byte, 128))
	require.Equal(t, 128, n)
	require.NoError(t, err)
	n, err = w.Write(make([]byte, 1))
	require.Equal(t, 1, n)
	require.NoError(t, err)
	require.Panics(t, func() {
		_ = w.BytesWritten()
	})
}

func TestSnappyBlockCompression(t *testing.T) {
	for i := 1; i <= 512; i++ {
		src := make([]byte, i*1024)
		sz, ok := MaxEncodedLen(Snappy, uint64(i*1024))
		require.True(t, ok, "failed to get encoded len")
		dst := make([]byte, sz)
		_, err := rand.Read(src)
		require.NoError(t, err)
		n := CompressSnappyBlock(src, dst)
		decompressed := make([]byte, i*1024)
		err = DecompressSnappyBlock(dst[:n], decompressed)
		require.NoError(t, err, "snappy compression failed")
		require.Equal(t, src, decompressed, "content changed")
	}
}

type tb struct {
	buf *bytes.Buffer
}

func (tb) Close() error { return nil }

func (t *tb) Write(data []byte) (int, error) {
	return t.buf.Write(data)
}

func (t *tb) Read(data []byte) (int, error) {
	return t.buf.Read(data)
}

func TestCompressorDecompressor(t *testing.T) {
	for i := 1; i <= 128; i++ {
		src := make([]byte, 10*i*1024)
		dst := make([]byte, 0)
		data := make([]byte, 0)
		_, err := rand.Read(src)
		require.NoError(t, err)
		buf := &tb{buf: bytes.NewBuffer(data)}
		c := NewCompressor(Snappy, buf)
		n, err := c.Write(src)
		require.Equal(t, len(src), n, "failed to write all data")
		require.NoError(t, err)
		require.NoError(t, c.Close())
		d := NewDecompressor(Snappy, buf)
		for {
			r := make([]byte, 1024)
			_, err = d.Read(r)
			require.NoError(t, err, "failed to read")
			dst = append(dst, r...)
			if len(dst) == len(src) {
				break
			}
		}
		require.Equal(t, src, dst, "content changed")
	}
}
