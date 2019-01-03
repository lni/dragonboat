// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

package compression

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/lni/dragonboat/internal/utils/compression/snappy"
)

// Compressor is used for compressing gRPC traffic using the snappy library.
// It implements the grpc.Compressor interface.
type Compressor struct {
	writerPool *sync.Pool
}

// NewCompressor creates a new Compressor instance.
func NewCompressor() *Compressor {
	return &Compressor{
		writerPool: &sync.Pool{},
	}
}

// Do implements the grpc.Compressor interface, it compresses the data into
// the specified writer.
func (c *Compressor) Do(writer io.Writer, data []byte) error {
	w, ok := c.writerPool.Get().(*snappy.Writer)
	if ok {
		w.Reset(writer)
	} else {
		w = snappy.NewBufferedWriter(writer)
	}
	_, err := w.Write(data)
	if err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	c.writerPool.Put(w)
	return nil
}

// Type implements the grpc.Compressor interface, it returns the algorithm
// name used by the compressor.
func (c *Compressor) Type() string {
	return "snappy"
}

// Decompressor is used for decompress gRPC traffic using the snappy library.
type Decompressor struct {
	readerPool *sync.Pool
}

// NewDecompressor creates a new decompressor instance. It implements the
// grpc.Decompressor interface.
func NewDecompressor() *Decompressor {
	return &Decompressor{
		readerPool: &sync.Pool{},
	}
}

// Do implements the grpc.Decompressor interface, it read the data from the
// reader and decompress it into the return []byte.
func (d *Decompressor) Do(reader io.Reader) ([]byte, error) {
	r, ok := d.readerPool.Get().(*snappy.Reader)
	if ok {
		r.Reset(reader)
	} else {
		r = snappy.NewReader(reader)
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	d.readerPool.Put(r)
	return data, nil
}

// Type implements the grpc.Decompressor interface. It returns the compression
// algorithm used by the Decompressor.
func (d *Decompressor) Type() string {
	return "snappy"
}
