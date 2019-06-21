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
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

// the blockWriter implementation had reference to reusee's project below
// https://github.com/reusee/hashingwriter

package rsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"math"
	"os"

	"github.com/lni/dragonboat/v3/internal/settings"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	snapshotBlockSize = settings.SnapshotChunkSize
)

var (
	writerMagicNumber = settings.BlockFileMagicNumber
	tailSize          = uint64(16)
	checksumSize      = uint64(4)
)

// GetV2PayloadSize returns the actual on disk size for the input user payload
// size.
func GetV2PayloadSize(sz uint64) uint64 {
	return getV2PayloadSize(sz, snapshotBlockSize)
}

func getV2PayloadSize(sz uint64, blockSize uint64) uint64 {
	return getChecksumedBlockSize(sz, blockSize) + tailSize
}

func getChecksumedBlockSize(sz uint64, blockSize uint64) uint64 {
	return uint64(math.Ceil(float64(sz)/float64(blockSize)))*checksumSize + sz
}

func validateHeader(header pb.SnapshotHeader) bool {
	checksum := header.HeaderChecksum
	header.HeaderChecksum = nil
	if header.ChecksumType != pb.CRC32IEEE {
		plog.Errorf("unsupported checksum type %d", header.ChecksumType)
		return false
	}
	version := (SnapshotVersion)(header.Version)
	if version != V1SnapshotVersion && version != V2SnapshotVersion {
		plog.Errorf("unsupported version %d", header.Version)
		return false
	}
	data, err := header.Marshal()
	if err != nil {
		panic(err)
	}
	headerHash, ok := getChecksum(header.ChecksumType)
	if !ok {
		return false
	}
	if _, err := headerHash.Write(data); err != nil {
		panic(err)
	}
	return bytes.Equal(headerHash.Sum(nil), checksum)
}

func validateBlock(block []byte, h hash.Hash) bool {
	if uint64(len(block)) <= checksumSize {
		return false
	}
	payload := block[:uint64(len(block))-checksumSize]
	crc := block[uint64(len(block))-checksumSize:]
	h.Reset()
	_, err := h.Write(payload)
	if err != nil {
		panic(err)
	}
	return bytes.Equal(crc, h.Sum(nil))
}

// BlockWriter is a writer type that writes the input data to the underlying
// storage with checksum appended at the end of each block.
type BlockWriter struct {
	h          hash.Hash
	fh         hash.Hash
	block      []byte
	blockSize  uint64
	onNewBlock func(data []byte, crc []byte) error
	nextStop   uint64
	written    uint64
	total      uint64
	flushed    bool
}

// IBlockWriter is the interface for writing checksumed data blocks.
type IBlockWriter interface {
	Write(bs []byte) (int, error)
	Flush() error
	GetPayloadChecksum() []byte
}

// NewBlockWriter creates and returns a block writer.
func NewBlockWriter(blockSize uint64,
	onNewBlock func(data []byte, crc []byte) error,
	t pb.ChecksumType) *BlockWriter {
	return newBlockWriter(blockSize, onNewBlock, t)
}

func newBlockWriter(blockSize uint64,
	onNewBlock func(data []byte, crc []byte) error,
	t pb.ChecksumType) *BlockWriter {
	return &BlockWriter{
		blockSize:  blockSize,
		block:      make([]byte, 0, blockSize+checksumSize),
		onNewBlock: onNewBlock,
		nextStop:   blockSize,
		h:          mustGetChecksum(t),
		fh:         mustGetChecksum(t),
	}
}

// Write writes the specified data using the block writer.
func (bw *BlockWriter) Write(bs []byte) (int, error) {
	if bw.flushed {
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
			bw.total += uint64(len(bw.block)) + checksumSize
			if err := bw.processNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
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

// Flush writes all in memory buffered data.
func (bw *BlockWriter) Flush() error {
	if bw.flushed {
		panic("flush called again")
	} else {
		bw.flushed = true
	}
	if len(bw.block) > 0 {
		bw.total += uint64(len(bw.block)) + checksumSize
		if err := bw.processNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
			plog.Infof("onNewBlock failed %v", err)
			return err
		}
	}
	totalbs := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalbs, uint64(bw.total))
	tailBlock := append(totalbs, writerMagicNumber...)
	if err := bw.processNewBlock(tailBlock, nil); err != nil {
		plog.Infof("process tail block failed %v", err)
		return err
	}
	return nil
}

// GetPayloadChecksum returns the checksum for the entire payload.
func (bw *BlockWriter) GetPayloadChecksum() []byte {
	if !bw.flushed {
		panic("not flushed yet")
	}
	return bw.fh.Sum(nil)
}

func (bw *BlockWriter) processNewBlock(data []byte, crc []byte) error {
	if len(crc) > 0 {
		if _, err := bw.fh.Write(crc); err != nil {
			panic(err)
		}
	}
	return bw.onNewBlock(data, crc)
}

type blockReader struct {
	r         io.Reader
	t         pb.ChecksumType
	blockSize uint64
	block     []byte
}

// the input reader should be a reader to all blocks, thus the 16 bytes length
// and magic number fields should not be included, use a io.LimitReader to
// exclude them
func newBlockReader(r io.Reader,
	blockSize uint64, t pb.ChecksumType) *blockReader {
	return &blockReader{
		r:         r,
		t:         t,
		blockSize: blockSize,
		block:     make([]byte, 0, blockSize+checksumSize),
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
	br.block = make([]byte, br.blockSize+checksumSize)
	n, err := io.ReadFull(br.r, br.block)
	if err != nil && err != io.ErrUnexpectedEOF {
		return n, err
	}
	br.block = br.block[:n]
	h := mustGetChecksum(br.t)
	if !validateBlock(br.block, h) {
		panic("corrupted block")
	}
	br.block = br.block[:uint64(len(br.block))-checksumSize]
	return len(br.block), nil
}

// IVWriter is the interface for versioned snapshot writer.
type IVWriter interface {
	Write(data []byte) (int, error)
	GetVersion() SnapshotVersion
	GetPayloadSum() []byte
	GetPayloadSize(uint64) uint64
	Flush() error
}

// IVReader is the interface for versioned snapshot reader.
type IVReader interface {
	Read(data []byte) (int, error)
	Sum() []byte
}

type v1writer struct {
	f io.Writer
	h hash.Hash
}

func newV1Wrtier(f io.Writer) *v1writer {
	// v1 is hard coded to use pb.CRC32IEEE
	h := mustGetChecksum(pb.CRC32IEEE)
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
	// v1 is hard coded to use pb.CRC32IEEE
	h := mustGetChecksum(pb.CRC32IEEE)
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
	bw *BlockWriter
}

func newV2Writer(fw io.Writer, t pb.ChecksumType) *v2writer {
	onBlock := func(data []byte, crc []byte) error {
		if len(crc) > 0 && uint64(len(crc)) != checksumSize {
			panic("unexpected crc length")
		}
		_, err := fw.Write(append(data, crc...))
		return err
	}
	return &v2writer{
		bw: newBlockWriter(snapshotBlockSize, onBlock, t),
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
	return v2w.bw.GetPayloadChecksum()
}

func (v2w *v2writer) Flush() error {
	return v2w.bw.Flush()
}

type v2reader struct {
	br *blockReader
}

func newV2Reader(fr io.Reader, t pb.ChecksumType) *v2reader {
	return &v2reader{br: newBlockReader(fr, snapshotBlockSize, t)}
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

// IVValidator is the interface for versioned validator.
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
		h:      mustGetChecksum(header.ChecksumType),
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
	return &v2validator{
		block: make([]byte, 0, snapshotBlockSize+checksumSize),
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
	for uint64(len(v.block)) >= 2*(snapshotBlockSize+checksumSize) {
		block := v.block[:snapshotBlockSize+checksumSize]
		v.block = v.block[snapshotBlockSize+checksumSize:]
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
	for uint64(len(block)) > snapshotBlockSize+checksumSize {
		c := block[:snapshotBlockSize+checksumSize]
		block = block[snapshotBlockSize+checksumSize:]
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

// GetV2PayloadChecksum calculates the payload checksum of the specified
// snapshot file.
func GetV2PayloadChecksum(fp string) (crc []byte, err error) {
	offsets, err := getV2CRCOffsetList(fp)
	if err != nil {
		return nil, err
	}
	t, err := getV2ChecksumType(fp)
	if err != nil {
		return nil, err
	}
	h := mustGetChecksum(t)
	f, err := os.OpenFile(fp, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	for _, offset := range offsets {
		if _, err := f.Seek(int64(offset), 0); err != nil {
			return nil, err
		}
		crc := make([]byte, checksumSize)
		if _, err := io.ReadFull(f, crc); err != nil {
			return nil, err
		}
		if _, err := h.Write(crc); err != nil {
			return nil, err
		}
	}
	crc = h.Sum(nil)
	return
}

func getV2ChecksumType(fp string) (ct pb.ChecksumType, err error) {
	reader, err := NewSnapshotReader(fp)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := reader.Close(); err == nil {
			err = cerr
		}
	}()
	header, err := reader.GetHeader()
	if err != nil {
		return pb.ChecksumType(0), err
	}
	reader.ValidateHeader(header)
	if header.Version != uint64(V2SnapshotVersion) {
		return pb.ChecksumType(0), errors.New("not a v2 snapshot file")
	}
	return header.ChecksumType, nil
}

func getV2CRCOffsetList(fp string) ([]uint64, error) {
	fi, err := os.Stat(fp)
	if err != nil {
		return nil, err
	}
	return getV2CRCOffsetListFromFileSize(uint64(fi.Size()))
}

func getV2CRCOffsetListFromFileSize(sz uint64) ([]uint64, error) {
	if sz <= tailSize+SnapshotHeaderSize {
		return nil, errors.New("invalid file size")
	}
	sz = sz - tailSize - SnapshotHeaderSize
	result := make([]uint64, 0)
	offset := SnapshotHeaderSize
	for sz > 0 {
		if sz >= snapshotBlockSize+checksumSize {
			result = append(result, offset+snapshotBlockSize)
			offset = offset + snapshotBlockSize + checksumSize
			sz = sz - (snapshotBlockSize + checksumSize)
		} else {
			result = append(result, offset+sz-checksumSize)
			break
		}
	}
	return result, nil
}
