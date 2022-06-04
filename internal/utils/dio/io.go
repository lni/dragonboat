// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"io"
	"math"
	"sync/atomic"

	"github.com/golang/snappy"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// CompressionType is the type of the compression.
type CompressionType = pb.CompressionType

const (
	// NoCompression is the CompressionType value used to indicate not to use
	// any compression.
	NoCompression CompressionType = pb.NoCompression
	// Snappy is the CompressionType value used to indicate that google snappy
	// is used for data compression.
	Snappy CompressionType = pb.Snappy
)

// CountedWriter is a io.WriteCloser wrapper that keeps the total number of bytes
// written to the underlying writer.
type CountedWriter struct {
	closed uint32
	total  uint64
	w      io.WriteCloser
}

// NewCountedWriter creates a new CountedWriter.
func NewCountedWriter(w io.WriteCloser) *CountedWriter {
	return &CountedWriter{w: w}
}

// Write writes the specified content to the underlying writer.
func (cw *CountedWriter) Write(data []byte) (int, error) {
	cw.total += uint64(len(data))
	return cw.w.Write(data)
}

// Close closes the underlying writer.
func (cw *CountedWriter) Close() error {
	defer func() {
		atomic.StoreUint32(&cw.closed, 1)
	}()
	return cw.w.Close()
}

// BytesWritten returns the total number of bytes written.
func (cw *CountedWriter) BytesWritten() uint64 {
	if atomic.LoadUint32(&cw.closed) == 0 {
		panic("calling BytesWritten before close is called")
	}
	return cw.total
}

// Compressor is a io.WriteCloser that compresses its input data to its
// underlying io.Writer.
type Compressor struct {
	uw io.WriteCloser
	wc io.WriteCloser
	ct CompressionType
}

// NewCompressor returns a Compressor instance.
func NewCompressor(ct CompressionType, wc io.WriteCloser) io.WriteCloser {
	if ct == NoCompression {
		return wc
	} else if ct == Snappy {
		c := &Compressor{
			uw: wc,
			wc: snappy.NewBufferedWriter(wc),
			ct: ct,
		}
		return c
	} else {
		panic("unknown compression type")
	}
}

// Write compresses the input data and writes to the underlying writer.
func (c *Compressor) Write(data []byte) (int, error) {
	return c.wc.Write(data)
}

// Close closes the compressor.
func (c *Compressor) Close() error {
	if c.ct == NoCompression {
		panic("not suppose to reach here")
	}
	if err := c.wc.Close(); err != nil {
		return err
	}
	if c.uw != nil {
		return c.uw.Close()
	}
	return nil
}

// Decompressor is a io.WriteCloser that decompresses data read from its
// underlying reader.
type Decompressor struct {
	ur io.ReadCloser
	rc io.Reader
	ct CompressionType
}

// NewDecompressor return a decompressor instance.
func NewDecompressor(ct CompressionType, r io.ReadCloser) io.ReadCloser {
	if ct == NoCompression {
		return r
	} else if ct == Snappy {
		d := &Decompressor{
			ur: r,
			rc: snappy.NewReader(r),
			ct: ct,
		}
		return d
	} else {
		panic("unknown compression type")
	}
}

// Read reads from the underlying reader.
func (dc *Decompressor) Read(data []byte) (int, error) {
	return dc.rc.Read(data)
}

// Close closes the decompressor.
func (dc *Decompressor) Close() error {
	if dc.ct == NoCompression {
		panic("not suppose to reach here")
	} else if dc.ct == Snappy {
		return dc.ur.Close()
	} else {
		panic("unknown compression type")
	}
}

// MaxEncodedLen returns the maximum length of the encoded block given the
// specified compression type and src length.
func MaxEncodedLen(ct CompressionType, srcLen uint64) (uint64, bool) {
	if ct == Snappy {
		if srcLen > MaxBlockLen(ct) {
			return 0, false
		}
		sz := snappy.MaxEncodedLen(int(srcLen))
		if sz == -1 {
			return 0, false
		}
		return uint64(sz), true
	}
	panic("not supported compression type")
}

// MaxBlockLen returns the maximum length allowed for specified compression
// type.
func MaxBlockLen(ct CompressionType) uint64 {
	if ct == Snappy {
		// https://github.com/golang/snappy/blob/2a8bb927dd31d8daada140a5d09578521ce5c36a/encode.go#L76
		return 6 * (0xffffffff - 32) / 7
	}
	return math.MaxUint64
}

// CompressSnappyBlock compresses the src block using snappy and store the
// compressed block into dst. The length of the compressed block is returned.
func CompressSnappyBlock(src []byte, dst []byte) int {
	dstLen := len(dst)
	result := snappy.Encode(dst, src)
	if len(result) > dstLen {
		panic("dst length is too small")
	}
	return len(result)
}

// DecompressSnappyBlock decompresses the snappy compressed data in src to the
// dst slice. The dst slice must be of the exact length of the uncompressed
// data.
func DecompressSnappyBlock(src []byte, dst []byte) error {
	dstLen := len(dst)
	result, err := snappy.Decode(dst, src)
	if len(result) != dstLen {
		panic("corrupted decodedLen in header")
	}
	if err != nil {
		return err
	}
	return nil
}
