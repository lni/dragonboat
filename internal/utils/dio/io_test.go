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
	"math/rand"
	"testing"
)

type wc struct{}

func (wc) Write(data []byte) (int, error) {
	return len(data), nil
}

func (wc) Close() error { return nil }

func TestCountedWriterCountsWrittenBytes(t *testing.T) {
	w := NewCountedWriter(&wc{})
	n, err := w.Write(make([]byte, 128))
	if n != 128 || err != nil {
		t.Errorf("write failed")
	}
	n, err = w.Write(make([]byte, 1))
	if n != 1 || err != nil {
		t.Errorf("write failed")
	}
	if err := w.Close(); err != nil {
		t.Errorf("close failed, %v", err)
	}
	if w.BytesWritten() != 129 {
		t.Errorf("failed to report count")
	}
}

func TestCountedWriteMustBeClosedFirst(t *testing.T) {
	w := NewCountedWriter(&wc{})
	n, err := w.Write(make([]byte, 128))
	if n != 128 || err != nil {
		t.Errorf("write failed")
	}
	n, err = w.Write(make([]byte, 1))
	if n != 1 || err != nil {
		t.Errorf("write failed")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("no panic")
		}
	}()
	_ = w.BytesWritten()
}

func TestSnappyBlockCompression(t *testing.T) {
	for i := 1; i <= 512; i++ {
		src := make([]byte, i*1024)
		sz, ok := MaxEncodedLen(Snappy, uint64(i*1024))
		if !ok {
			t.Fatalf("failed to get encoded len")
		}
		dst := make([]byte, sz)
		rand.Read(src)
		n := CompressSnappyBlock(src, dst)
		decompressed := make([]byte, i*1024)
		if err := DecompressSnappyBlock(dst[:n], decompressed); err != nil {
			t.Fatalf("snappy compression failed, %v", err)
		} else {
			if !bytes.Equal(src, decompressed) {
				t.Fatalf("content changed")
			}
		}
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
		rand.Read(src)
		buf := &tb{buf: bytes.NewBuffer(data)}
		c := NewCompressor(Snappy, buf)
		n, err := c.Write(src)
		if n != len(src) || err != nil {
			t.Fatalf("failed to write all data")
		}
		c.Close()
		d := NewDecompressor(Snappy, buf)
		for {
			r := make([]byte, 1024)
			_, err = d.Read(r)
			if err != nil {
				t.Fatalf("failed to read %v", err)
			}
			dst = append(dst, r...)
			if len(dst) == len(src) {
				break
			}
		}
		if !bytes.Equal(src, dst) {
			t.Fatalf("content changed")
		}
	}
}
