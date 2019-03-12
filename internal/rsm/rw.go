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
	"io"
	"math"

	"github.com/lni/dragonboat/internal/settings"
	pb "github.com/lni/dragonboat/raftpb"
)

const (
	snapshotBlockSize = settings.SnapshotChunkSize
)

var (
	writerMagicNumber = settings.BlockFileMagicNumber
	tailSize          = uint64(16)
)

func GetV2PayloadSize(sz uint64) uint64 {
	return getV2PayloadSize(sz, snapshotBlockSize)
}

func getV2PayloadSize(sz uint64, blockSize uint64) uint64 {
	return getChecksumedBlockSize(sz, blockSize) + tailSize
}

func getChecksumedBlockSize(sz uint64, blockSize uint64) uint64 {
	return uint64(math.Ceil(float64(sz)/float64(blockSize)))*4 + sz
}

func validateBlock(block []byte, h hash.Hash) bool {
	if len(block) <= 4 {
		return false
	}
	payload := block[:len(block)-4]
	crc := block[len(block)-4:]
	h.Reset()
	_, err := h.Write(payload)
	if err != nil {
		panic(err)
	}
	return bytes.Equal(crc, h.Sum(nil))
}

type blockWriter struct {
	h          hash.Hash
	block      []byte
	blockSize  uint64
	onNewBlock func(data []byte, crc []byte) error
	nextStop   uint64
	written    uint64
	total      uint64
	done       bool
}

func newBlockWriter(blockSize uint64,
	onNewBlock func(data []byte, crc []byte) error) *blockWriter {
	return &blockWriter{
		blockSize:  blockSize,
		block:      make([]byte, 0, blockSize+4),
		onNewBlock: onNewBlock,
		nextStop:   blockSize,
		h:          getDefaultChecksum(),
	}
}

func (bw *blockWriter) Write(bs []byte) (int, error) {
	if bw.done {
		panic("write called after flush")
	}
	var totalN uint64
	for len(bs) > 0 {
		l := bw.nextStop - bw.written
		if l > uint64(len(bs)) {
			l = uint64(len(bs))
		}
		bw.block = append(bw.block, bs[:l]...)
		if _, err := bw.h.Write(bs[:l]); err != nil {
			panic(err)
		}
		bw.written += l
		if bw.written == bw.nextStop {
			bw.total += uint64(len(bw.block) + 4)
			if err := bw.onNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
				return int(totalN), err
			}
			bw.nextStop = bw.nextStop + bw.blockSize
			bw.h.Reset()
			bw.block = bw.block[:0]
		}
		bs = bs[l:]
		totalN += l
	}
	return int(totalN), nil
}

func (bw *blockWriter) Flush() error {
	if bw.done {
		panic("flush called again")
	} else {
		bw.done = true
	}
	if len(bw.block) > 0 {
		bw.total += uint64(len(bw.block) + 4)
		if err := bw.onNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
			plog.Infof("onNewBlock failed %v", err)
			return err
		}
	}
	totalbs := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalbs, uint64(bw.total))
	if err := bw.onNewBlock(append(totalbs, writerMagicNumber...), nil); err != nil {
		plog.Infof("on tail failed %v", err)
		return err
	}
	return nil
}

type blockReader struct {
	r         io.Reader
	blockSize uint64
	block     []byte
}

// the input reader should be a reader to all blocks, thus the 16 bytes length
// and magic number fields should not be included, use a io.LimitReader to
// exclude them
func newBlockReader(r io.Reader, blockSize uint64) *blockReader {
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
	h := getDefaultChecksum()
	if !validateBlock(br.block, h) {
		panic("corrupted block")
	}
	br.block = br.block[:len(br.block)-4]
	return len(br.block), nil
}

type IVWriter interface {
	Write(data []byte) (int, error)
	GetVersion() SnapshotVersion
	GetPayloadSum() []byte
	GetPayloadSize(uint64) uint64
	Flush() error
}

type IVReader interface {
	Read(data []byte) (int, error)
	Sum() []byte
}

type v1writer struct {
	f io.Writer
	h hash.Hash
}

func newV1Wrtier(f io.Writer) *v1writer {
	h := getDefaultChecksum()
	return &v1writer{f: io.MultiWriter(f, h), h: h}
}

func (v1w *v1writer) Write(data []byte) (int, error) {
	return v1w.f.Write(data)
}

func (v1w *v1writer) GetVersion() SnapshotVersion {
	return V1SnapshotVersion
}

func (v1w *v1writer) GetPayloadSize(sz uint64) uint64 {
	return sz
}

func (v1w *v1writer) GetPayloadSum() []byte {
	return v1w.h.Sum(nil)
}

func (v1w *v1writer) Flush() error {
	return nil
}

type v1reader struct {
	r io.Reader
	h hash.Hash
}

func newV1Reader(r io.Reader) *v1reader {
	h := getDefaultChecksum()
	return &v1reader{
		r: io.TeeReader(r, h),
		h: h,
	}
}

func (v1r *v1reader) Read(data []byte) (int, error) {
	return v1r.r.Read(data)
}

func (v1r *v1reader) Sum() []byte {
	return v1r.h.Sum(nil)
}

type v2writer struct {
	bw *blockWriter
}

func newV2Writer(fw io.Writer) *v2writer {
	onBlock := func(data []byte, crc []byte) error {
		v := append(data, crc...)
		n, err := fw.Write(v)
		if n != len(v) {
			return io.ErrShortWrite
		}
		return err
	}
	return &v2writer{
		bw: newBlockWriter(snapshotBlockSize, onBlock),
	}
}

func (v2w *v2writer) Write(data []byte) (int, error) {
	return v2w.bw.Write(data)
}

func (v2w *v2writer) GetVersion() SnapshotVersion {
	return V2SnapshotVersion
}

func (v2w *v2writer) GetPayloadSize(sz uint64) uint64 {
	return getV2PayloadSize(sz, snapshotBlockSize)
}

func (v2w *v2writer) GetPayloadSum() []byte {
	return []byte{0, 0, 0, 0}
}

func (v2w *v2writer) Flush() error {
	return v2w.bw.Flush()
}

type v2reader struct {
	br *blockReader
}

func newV2Reader(fr io.Reader) *v2reader {
	return &v2reader{br: newBlockReader(fr, snapshotBlockSize)}
}

func (br *v2reader) Read(data []byte) (int, error) {
	return br.br.Read(data)
}

func (br *v2reader) Sum() []byte {
	return []byte{0, 0, 0, 0}
}

func getHeaderFromFirstChunk(data []byte) (pb.SnapshotHeader, bool) {
	if uint64(len(data)) < SnapshotHeaderSize {
		panic("first chunk is too small")
	}
	sz := binary.LittleEndian.Uint64(data)
	if sz > SnapshotHeaderSize-8 {
		return pb.SnapshotHeader{}, false
	}
	headerData := data[8 : 8+sz]
	r := pb.SnapshotHeader{}
	if err := r.Unmarshal(headerData); err != nil {
		return pb.SnapshotHeader{}, false
	}
	return r, true
}

type IVValidator interface {
	AddChunk(data []byte, chunkID uint64) bool
	Validate() bool
}

type v1validator struct {
	header pb.SnapshotHeader
	h      hash.Hash
}

func newV1Validator(header pb.SnapshotHeader) *v1validator {
	return &v1validator{
		header: header,
		h:      getChecksum(header.ChecksumType),
	}
}

func (v *v1validator) AddChunk(data []byte, chunkID uint64) bool {
	var p []byte
	if chunkID == 0 {
		p = data[SnapshotHeaderSize:]
	} else {
		p = data
	}
	if _, err := v.h.Write(p); err != nil {
		return false
	}
	return true
}

func (v *v1validator) Validate() bool {
	return bytes.Equal(v.h.Sum(nil), v.header.PayloadChecksum)
}

type v2validator struct {
	block []byte
	total int
	h     hash.Hash
}

func newV2Validator(h hash.Hash) *v2validator {
	plog.Infof("creating v2 validator")
	return &v2validator{
		block: make([]byte, 0, snapshotBlockSize+4),
		h:     h,
	}
}

func (v *v2validator) AddChunk(data []byte, chunkID uint64) bool {
	var p []byte
	if chunkID == 0 {
		p = data[SnapshotHeaderSize:]
	} else {
		p = data
	}
	v.total += len(p)
	v.block = append(v.block, p...)
	for uint64(len(v.block)) >= 2*(snapshotBlockSize+4) {
		block := v.block[:snapshotBlockSize+4]
		v.block = v.block[snapshotBlockSize+4:]
		if !v.validateBlock(block) {
			return false
		}
	}
	return true
}

func (v *v2validator) Validate() bool {
	if uint64(len(v.block)) < tailSize {
		return false
	}
	tail := v.block[uint64(len(v.block))-tailSize:]
	block := v.block[:uint64(len(v.block))-tailSize]
	if !v.validateMagicSize(tail) {
		return false
	}
	for uint64(len(block)) > snapshotBlockSize+4 {
		c := block[:snapshotBlockSize+4]
		block = block[snapshotBlockSize+4:]
		if !v.validateBlock(c) {
			return false
		}
	}
	if len(block) > 0 {
		if !v.validateBlock(block) {
			return false
		}
	}
	return true
}

func (v *v2validator) validateMagicSize(tail []byte) bool {
	if uint64(len(tail)) != tailSize {
		panic("invalid size")
	}
	total := tail[:8]
	magic := tail[8:]
	if !bytes.Equal(magic, writerMagicNumber) {
		return false
	}
	totalSz := binary.LittleEndian.Uint64(total)
	return totalSz == uint64(v.total)-tailSize
}

func (v *v2validator) validateBlock(block []byte) bool {
	return validateBlock(block, v.h)
}
