// Copyright 2017-2019 Reusee (https://github.com/reusee)
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
//
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

// the blockWriter implementation had reference reusee's project below
// https://github.com/reusee/hashingwriter

package rsm

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"

	"github.com/lni/dragonboat/internal/settings"
)

var (
	writerMagicNumber = settings.BlockFileMagicNumber
)

type blockWriter struct {
	h          hash.Hash
	block      []byte
	blockSize  int
	onNewBlock func(data []byte, crc []byte) error
	nextStop   int
	written    int
	total      int
}

func newBlockWriter(blockSize int,
	onNewBlock func(data []byte, crc []byte) error) *blockWriter {
	return &blockWriter{
		blockSize:  blockSize,
		block:      make([]byte, 0, blockSize+4),
		onNewBlock: onNewBlock,
		nextStop:   blockSize,
		h:          crc32.NewIEEE(),
	}
}

func (bw *blockWriter) Write(bs []byte) (int, error) {
	var totalN int
	for len(bs) > 0 {
		l := bw.nextStop - bw.written
		if l > len(bs) {
			l = len(bs)
		}
		bw.block = append(bw.block, bs[:l]...)
		if _, err := bw.h.Write(bs[:l]); err != nil {
			panic(err)
		}
		bw.written += l
		if bw.written == bw.nextStop {
			bw.total += (len(bw.block) + 4)
			if err := bw.onNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
				return totalN, err
			}
			bw.nextStop = bw.nextStop + bw.blockSize
			bw.h.Reset()
			bw.block = bw.block[:0]
		}
		bs = bs[l:]
		totalN += l
	}
	return totalN, nil
}

func (bw *blockWriter) Close() error {
	if len(bw.block) > 0 {
		bw.total += (len(bw.block) + 4)
		if err := bw.onNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
			return err
		}
	}
	totalbs := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalbs, uint64(bw.total))
	if err := bw.onNewBlock(append(totalbs, writerMagicNumber...), nil); err != nil {
		return err
	}
	return nil
}

type blockReader struct {
	r         io.Reader
	blockSize int
	block     []byte
}

// the input reader should be a reader to all blocks, thus the 16 bytes length
// and magic number fields should not be included, use a io.LimitReader to
// exclude them
func newBlockReader(r io.Reader, blockSize int) *blockReader {
	return &blockReader{
		r:         r,
		blockSize: blockSize,
		block:     make([]byte, 0, blockSize+4),
	}
}

func (br *blockReader) Close() error {
	return nil
}

func (br *blockReader) Read(data []byte) (int, error) {
	want := len(data)
	if want <= len(br.block) {
		copy(data, br.block[:want])
		br.block = br.block[want:]
		return want, nil
	}
	read := len(br.block)
	copy(data, br.block)
	for read < want {
		_, err := br.readBlock()
		if err != nil {
			return read, err
		}
		toRead := want - read
		if toRead > len(br.block) {
			toRead = len(br.block)
		}
		copy(data[read:], br.block[:toRead])
		br.block = br.block[toRead:]
		read += toRead
	}
	return read, nil
}

func (br *blockReader) readBlock() (int, error) {
	br.block = make([]byte, br.blockSize+4)
	n, err := io.ReadFull(br.r, br.block)
	if err != nil && err != io.ErrUnexpectedEOF {
		return n, err
	}
	br.block = br.block[:n]
	if n <= 4 {
		panic("invalid block")
	}
	exp := br.block[len(br.block)-4:]
	br.block = br.block[:len(br.block)-4]
	h := crc32.NewIEEE()
	if _, err := h.Write(br.block); err != nil {
		panic(err)
	}
	if bytes.Compare(h.Sum(nil), exp) != 0 {
		panic("corrupted block")
	}
	return len(br.block), nil
}
