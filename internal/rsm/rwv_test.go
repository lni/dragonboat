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
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTooShortBlockAreRejected(t *testing.T) {
	for i := 1; i <= 4; i++ {
		v := make([]byte, i)
		require.False(t, validateBlock(v, getDefaultChecksum()))
	}
}

func TestRandomBlocksAreRejected(t *testing.T) {
	for i := 1; i < 128; i++ {
		v := make([]byte, i*128)
		_, err := rand.Read(v)
		require.NoError(t, err)
		require.False(t, validateBlock(v, getDefaultChecksum()))
	}
}

func TestCorruptedBlockIsRejected(t *testing.T) {
	v := make([]byte, 1023)
	_, err := rand.Read(v)
	require.NoError(t, err)
	h := GetDefaultChecksum()
	_, err = h.Write(v)
	require.NoError(t, err)
	v = append(v, h.Sum(nil)...)
	require.True(t, validateBlock(v, h))
	v[0] = v[0] + 1
	require.False(t, validateBlock(v, h))
}

func TestWellFormedBlocksAreAccepted(t *testing.T) {
	for i := 1; i < 128; i++ {
		v := make([]byte, i*128)
		_, err := rand.Read(v)
		require.NoError(t, err)
		h := getDefaultChecksum()
		_, err = h.Write(v)
		require.NoError(t, err)
		v = append(v, h.Sum(nil)...)
		require.True(t, validateBlock(v, h))
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
	for _, sz := range szs {
		v := make([]byte, sz)
		_, err := rand.Read(v)
		require.NoError(t, err)
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		w := newV2Writer(buf, defaultChecksumType)
		_, err = w.Write(v)
		require.NoError(t, err)
		err = w.Close()
		require.NoError(t, err)
		header := make([]byte, HeaderSize)
		data := append(header, buf.Bytes()...)
		validator := newV2Validator(getDefaultChecksum())
		require.True(t, validator.AddChunk(data, 0))
		require.True(t, validator.Validate())
		if sz > HeaderSize {
			s := sz / 2
			fh := data[:s]
			lh := data[s:]
			validator := newV2Validator(getDefaultChecksum())
			require.True(t, validator.AddChunk(fh, 0))
			require.True(t, validator.AddChunk(lh, 1))
			require.True(t, validator.Validate())
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
	for _, sz := range szs {
		v := make([]byte, sz)
		_, err := rand.Read(v)
		require.NoError(t, err)
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		w := newV2Writer(buf, defaultChecksumType)
		_, err = w.Write(v)
		require.NoError(t, err)
		err = w.Close()
		require.NoError(t, err)
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
		result := vf(data)
		require.Equal(t, ok, result)
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
			result := vf(data)
			require.Equal(t, ok, result)
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
	for _, sz := range testSz {
		lastBlock := false
		written := make([]byte, 0)
		onBlock := func(data []byte, crc []byte) error {
			if lastBlock {
				require.NotEmpty(t, crc)
			}
			if len(crc) == 0 {
				lastBlock = true
			}
			if len(crc) != 0 {
				h := crc32.NewIEEE()
				_, err := h.Write(data)
				require.NoError(t, err)
				require.Equal(t, h.Sum(nil), crc)
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
		require.Equal(t, uint64(n), sz)
		require.NoError(t, err)

		require.NoError(t, writer.Close())
		result := written[:len(written)-16]
		meta := written[len(written)-16:]
		total := binary.LittleEndian.Uint64(meta[:8])
		magic := meta[8:]
		expSz := getChecksumedBlockSize(sz, blockSize)
		require.Equal(t, expSz, total)
		require.Equal(t, writerMagicNumber, magic)
		require.Equal(t, input, result)
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
	for _, sz := range testSz {
		readBufSz := []uint64{3, 1,
			blockSize - 1, blockSize, blockSize + 1,
			blockSize * 3, blockSize*3 - 1, blockSize*3 + 1,
			sz, sz - 1, sz + 1}
		for _, bufSz := range readBufSz {
			buf := bytes.NewBuffer(make([]byte, 0, 128*1024))
			onBlock := func(data []byte, crc []byte) error {
				toWrite := append(data, crc...)
				n, err := buf.Write(toWrite)
				require.Equal(t, len(toWrite), n)
				require.NoError(t, err)
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
			require.Equal(t, uint64(n), sz)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			written := buf.Bytes()
			expSz := getChecksumedBlockSize(sz, blockSize) + 16
			require.Equal(t, expSz, uint64(len(written)))

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
					require.NoError(t, err)
				}
				allRead = append(allRead, curRead[:n]...)
				if err == io.EOF {
					break
				}
			}
			require.Equal(t, input, allRead)
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
		require.Equal(t, len(toWrite), n)
		require.NoError(t, err)
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
	require.Equal(t, uint64(n), sz)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	written := append([]byte{}, buf.Bytes()...)
	for idx := 0; idx < len(written)-16; idx++ {
		func() {
			curRead := make([]byte, 4)
			written[idx] = written[idx] + 1
			lr := io.LimitReader(bytes.NewBuffer(written),
				int64(len(written)-16))
			reader := newBlockReader(lr, blockSize, defaultChecksumType)
			require.Panics(t, func() {
				for {
					n, err = reader.Read(curRead)
					if err != nil && err != io.EOF {
						require.NoError(t, err)
					}
					if err == io.EOF {
						break
					}
				}
			})
		}()
	}
}
