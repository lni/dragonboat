// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/rand"
	"testing"
)

func TestTooShortBlockAreRejected(t *testing.T) {
	for i := 1; i <= 4; i++ {
		v := make([]byte, i)
		if validateBlock(v, getDefaultChecksum()) {
			t.Fatalf("not rejected")
		}
	}
}

func TestRandomBlocksAreRejected(t *testing.T) {
	for i := 1; i < 128; i++ {
		v := make([]byte, i*128)
		rand.Read(v)
		if validateBlock(v, getDefaultChecksum()) {
			t.Fatalf("not rejected")
		}
	}
}

func TestCorruptedBlockIsRejected(t *testing.T) {
	v := make([]byte, 1023)
	rand.Read(v)
	h := GetDefaultChecksum()
	if _, err := h.Write(v); err != nil {
		t.Fatalf("failed to update hash")
	}
	v = append(v, h.Sum(nil)...)
	if !validateBlock(v, h) {
		t.Fatalf("validation failed")
	}
	v[0] = v[0] + 1
	if validateBlock(v, h) {
		t.Fatalf("validation didn't failed")
	}
}

func TestWellFormedBlocksAreAccepted(t *testing.T) {
	for i := 1; i < 128; i++ {
		v := make([]byte, i*128)
		rand.Read(v)
		h := getDefaultChecksum()
		_, err := h.Write(v)
		if err != nil {
			t.Fatalf("failed to write %v", err)
		}
		v = append(v, h.Sum(nil)...)
		if !validateBlock(v, h) {
			t.Fatalf("incorrectly rejected")
		}
	}
}

func TestWellFormedDataCanPassV2Validator(t *testing.T) {
	szs := []uint64{
		1,
		blockSize,
		blockSize - 1,
		blockSize + 1,
		blockSize * 5,
		blockSize*6 - 1,
		blockSize*6 + 1,
	}
	for idx, sz := range szs {
		v := make([]byte, sz)
		rand.Read(v)
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		w := newV2Writer(buf, defaultChecksumType)
		_, err := w.Write(v)
		if err != nil {
			t.Fatalf("failed to write %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to flush %v", err)
		}
		header := make([]byte, HeaderSize)
		data := append(header, buf.Bytes()...)
		validator := newV2Validator(getDefaultChecksum())
		if !validator.AddChunk(data, 0) {
			t.Errorf("%d, add chunk failed", idx)
		}
		if !validator.Validate() {
			t.Errorf("%d, validation failed", idx)
		}
		if sz > HeaderSize {
			s := sz / 2
			fh := data[:s]
			lh := data[s:]
			validator := newV2Validator(getDefaultChecksum())
			if !validator.AddChunk(fh, 0) {
				t.Errorf("%d, add fh chunk failed", idx)
			}
			if !validator.AddChunk(lh, 1) {
				t.Errorf("%d, add lh chunk failed", idx)
			}
			if !validator.Validate() {
				t.Errorf("%d, parts validation failed", idx)
			}
		}
	}
}

func testCorruptedDataCanBeDetectedByValidator(t *testing.T,
	corruptFn func([]byte) []byte, ok bool) {
	szs := []uint64{
		1,
		blockSize,
		blockSize - 1,
		blockSize + 1,
		blockSize * 3,
		blockSize*3 - 1,
		blockSize*3 + 1,
	}
	for idx, sz := range szs {
		v := make([]byte, sz)
		rand.Read(v)
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		w := newV2Writer(buf, defaultChecksumType)
		_, err := w.Write(v)
		if err != nil {
			t.Fatalf("failed to write %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to flush %v", err)
		}
		payload := buf.Bytes()
		payload = corruptFn(payload)
		header := make([]byte, HeaderSize)
		data := append(header, payload...)
		vf := func(data []byte) bool {
			validator := newV2Validator(getDefaultChecksum())
			if !validator.AddChunk(data, 0) {
				return false
			}
			return validator.Validate()
		}
		if result := vf(data); result != ok {
			t.Errorf("%d, validation failed", idx)
		}
		if sz > HeaderSize {
			szz := sz
			vf := func(data []byte) bool {
				s := szz / 2
				fh := data[:s]
				lh := data[s:]
				validator := newV2Validator(getDefaultChecksum())
				if !validator.AddChunk(fh, 0) {
					return false
				}
				if !validator.AddChunk(lh, 1) {
					return false
				}
				return validator.Validate()
			}
			if result := vf(data); result != ok {
				t.Errorf("%d, half-half validation failed", idx)
			}
		}
	}
}

func TestCorruptedDataCanBeDetectedByValidator(t *testing.T) {
	noop := func(data []byte) []byte {
		return data
	}
	testCorruptedDataCanBeDetectedByValidator(t, noop, true)
	firstByte := func(data []byte) []byte {
		data[0] = data[0] + 1
		return data
	}
	testCorruptedDataCanBeDetectedByValidator(t, firstByte, false)
	lastByte := func(data []byte) []byte {
		data[len(data)-1] = data[len(data)-1] + 1
		return data
	}
	testCorruptedDataCanBeDetectedByValidator(t, lastByte, false)
	truncateFirstByte := func(data []byte) []byte {
		return data[1:]
	}
	testCorruptedDataCanBeDetectedByValidator(t, truncateFirstByte, false)
	truncateLastByte := func(data []byte) []byte {
		return data[:len(data)-1]
	}
	testCorruptedDataCanBeDetectedByValidator(t, truncateLastByte, false)
	extraFirstByte := func(data []byte) []byte {
		return append([]byte{0}, data...)
	}
	testCorruptedDataCanBeDetectedByValidator(t, extraFirstByte, false)
	extraLastByte := func(data []byte) []byte {
		return append(data, 0)
	}
	testCorruptedDataCanBeDetectedByValidator(t, extraLastByte, false)
}

func TestBlockWriterCanWriteData(t *testing.T) {
	blockSize := uint64(128)
	testSz := []uint64{
		1,
		blockSize - 1,
		blockSize + 1,
		blockSize*2 - 1,
		blockSize * 2,
		blockSize*2 + 1,
		blockSize*2 + 5,
		blockSize*2 - 5,
		blockSize * 128,
		blockSize*128 + 4,
		blockSize*128 - 4,
	}
	for idx, sz := range testSz {
		lastBlock := false
		written := make([]byte, 0)
		onBlock := func(data []byte, crc []byte) error {
			if lastBlock {
				if len(crc) == 0 {
					t.Fatalf("empty crc value")
				}
			}
			if len(crc) == 0 {
				lastBlock = true
			}
			if len(crc) != 0 {
				h := crc32.NewIEEE()
				if _, err := h.Write(data); err != nil {
					t.Fatalf("failed to write %v", err)
				}
				if !bytes.Equal(h.Sum(nil), crc) {
					t.Errorf("unexpected CRC value")
				}
			}
			written = append(written, data...)
			return nil
		}
		writer := newBlockWriter(blockSize, onBlock, defaultChecksumType)
		input := make([]byte, sz)
		for i := range input {
			input[i] = byte((sz + uint64(i)) % 256)
		}
		n, err := writer.Write(input)
		if uint64(n) != sz {
			t.Errorf("failed to write all data")
		}
		if err != nil {
			t.Errorf("write failed %v", err)
		}

		writer.Close()
		result := written[:len(written)-16]
		meta := written[len(written)-16:]
		total := binary.LittleEndian.Uint64(meta[:8])
		magic := meta[8:]
		expSz := getChecksumedBlockSize(sz, blockSize)
		if total != expSz {
			t.Errorf("%d, total %d, size %d", idx, total, sz)
		}
		if !bytes.Equal(magic, writerMagicNumber) {
			t.Errorf("magic number changed")
		}
		if !bytes.Equal(input, result) {
			t.Errorf("%d, input changed, %+v, %+v", idx, input, result)
		}
	}
}

func TestBlockReaderCanReadData(t *testing.T) {
	blockSize := uint64(128)
	testSz := []uint64{
		1,
		blockSize - 1,
		blockSize + 1,
		blockSize*2 - 1,
		blockSize * 2,
		blockSize*2 + 1,
		blockSize*2 + 5,
		blockSize*2 - 5,
		blockSize * 128,
		blockSize*128 + 4,
		blockSize*128 - 4,
	}
	for idx, sz := range testSz {
		readBufSz := []uint64{3, 1,
			blockSize - 1, blockSize, blockSize + 1,
			blockSize * 3, blockSize*3 - 1, blockSize*3 + 1,
			sz, sz - 1, sz + 1}
		for _, bufSz := range readBufSz {
			buf := bytes.NewBuffer(make([]byte, 0, 128*1024))
			onBlock := func(data []byte, crc []byte) error {
				toWrite := append(data, crc...)
				n, err := buf.Write(toWrite)
				if n != len(toWrite) {
					t.Fatalf("failed to write all data")
				}
				if err != nil {
					t.Fatalf("failed to write %v", err)
				}
				return nil
			}
			writer := newBlockWriter(blockSize, onBlock, defaultChecksumType)
			input := make([]byte, sz)
			v := 0
			for i := range input {
				input[i] = byte(v % 256)
				v++
			}
			n, err := writer.Write(input)
			if uint64(n) != sz {
				t.Errorf("failed to write all data")
			}
			if err != nil {
				t.Errorf("write failed %v", err)
			}
			writer.Close()
			written := buf.Bytes()
			expSz := getChecksumedBlockSize(sz, blockSize) + 16
			if expSz != uint64(len(written)) {
				t.Errorf("exp %d, written %d", expSz, len(written))
			}

			allRead := make([]byte, 0)
			if bufSz == 0 {
				continue
			}
			curRead := make([]byte, bufSz)
			lr := io.LimitReader(buf, int64(len(buf.Bytes())-16))
			reader := newBlockReader(lr, blockSize, defaultChecksumType)
			for {
				n, err = reader.Read(curRead)
				if err != nil && err != io.EOF {
					t.Fatalf("failed to read %v", err)
				}
				allRead = append(allRead, curRead[:n]...)
				if err == io.EOF {
					break
				}
			}
			if !bytes.Equal(allRead, input) {
				t.Errorf("%d, %d, %d, returned data changed, input sz %d, returned sz %d",
					idx, sz, bufSz, len(input), len(allRead))
			}
		}
	}
}

func TestBlockReaderPanicOnCorruptedBlock(t *testing.T) {
	blockSize := uint64(128)
	sz := blockSize*5 + 4
	buf := bytes.NewBuffer(make([]byte, 0, blockSize*12))
	onBlock := func(data []byte, crc []byte) error {
		toWrite := append(data, crc...)
		n, err := buf.Write(toWrite)
		if n != len(toWrite) {
			t.Fatalf("failed to write all data")
		}
		if err != nil {
			t.Fatalf("failed to write %v", err)
		}
		return nil
	}
	writer := newBlockWriter(blockSize, onBlock, defaultChecksumType)
	input := make([]byte, sz)
	v := 0
	for i := range input {
		input[i] = byte(v % 256)
		v++
	}
	n, err := writer.Write(input)
	if uint64(n) != sz {
		t.Errorf("failed to write all data")
	}
	if err != nil {
		t.Errorf("write failed %v", err)
	}
	writer.Close()
	written := append([]byte{}, buf.Bytes()...)
	for idx := 0; idx < len(written)-16; idx++ {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("panic not trigger")
				}
			}()
			curRead := make([]byte, 4)
			written[idx] = written[idx] + 1
			lr := io.LimitReader(bytes.NewBuffer(written), int64(len(written)-16))
			reader := newBlockReader(lr, blockSize, defaultChecksumType)
			for {
				n, err = reader.Read(curRead)
				if err != nil && err != io.EOF {
					t.Fatalf("failed to read %v", err)
				}
				if err == io.EOF {
					break
				}
			}
		}()
	}
}
