// Copyright 2017-2021 Reusee (https://github.com/reusee)
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

// the blockWriter implementation had reference to reusee's project below
// https://github.com/reusee/hashingwriter

package rsm

import (
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

const (
	blockSize = settings.SnapshotChunkSize
)

var (
	writerMagicNumber = settings.BlockFileMagicNumber
	tailSize          = uint64(16)
	checksumSize      = uint64(4)
)

// GetV2PayloadSize returns the actual on disk size for the input user payload
// size.
func GetV2PayloadSize(sz uint64) uint64 {
	return getV2PayloadSize(sz, blockSize)
}

func getV2PayloadSize(sz uint64, blockSize uint64) uint64 {
	return getChecksumedBlockSize(sz, blockSize) + tailSize
}

func getChecksumedBlockSize(sz uint64, blockSize uint64) uint64 {
	return uint64(math.Ceil(float64(sz)/float64(blockSize)))*checksumSize + sz
}

func validateBlock(block []byte, h hash.Hash) bool {
	if uint64(len(block)) <= checksumSize {
		return false
	}
	payload := block[:uint64(len(block))-checksumSize]
	crc := block[uint64(len(block))-checksumSize:]
	h.Reset()
	fileutil.MustWrite(h, payload)
	return bytes.Equal(crc, h.Sum(nil))
}

// BlockWriter is a writer type that writes the input data to the underlying
// storage with checksum appended at the end of each block.
type BlockWriter struct {
	h          hash.Hash
	fh         hash.Hash
	onNewBlock func(data []byte, crc []byte) error
	block      []byte
	blockSize  uint64
	nextStop   uint64
	written    uint64
	total      uint64
	flushed    bool
}

var _ IBlockWriter = (*BlockWriter)(nil)

// IBlockWriter is the interface for writing checksumed data blocks.
type IBlockWriter interface {
	io.WriteCloser
	GetPayloadChecksum() []byte
}

// NewBlockWriter creates and returns a block writer.
func NewBlockWriter(blockSize uint64,
	nb func(data []byte, crc []byte) error, t pb.ChecksumType) *BlockWriter {
	return newBlockWriter(blockSize, nb, t)
}

func newBlockWriter(blockSize uint64,
	nb func(data []byte, crc []byte) error, t pb.ChecksumType) *BlockWriter {
	return &BlockWriter{
		blockSize:  blockSize,
		block:      make([]byte, 0, blockSize+checksumSize),
		onNewBlock: nb,
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
		fileutil.MustWrite(bw.h, bs[:l])
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

// Close closes the writer by passing all in memory buffered data to the
// underlying onNewBlock function.
func (bw *BlockWriter) Close() error {
	if bw.flushed {
		panic("flush called again")
	} else {
		bw.flushed = true
	}
	if len(bw.block) > 0 {
		bw.total += uint64(len(bw.block)) + checksumSize
		if err := bw.processNewBlock(bw.block, bw.h.Sum(nil)); err != nil {
			plog.Errorf("onNewBlock failed %v", err)
			return err
		}
	}
	totalbs := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalbs, bw.total)
	tailBlock := append(totalbs, writerMagicNumber...)
	return bw.processNewBlock(tailBlock, nil)
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
		fileutil.MustWrite(bw.fh, crc)
	}
	return bw.onNewBlock(data, crc)
}

type blockReader struct {
	r         io.Reader
	block     []byte
	blockSize uint64
	t         pb.ChecksumType
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
		if _, err := br.readBlock(); err != nil {
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
	if !validateBlock(br.block, mustGetChecksum(br.t)) {
		panic("corrupted block")
	}
	br.block = br.block[:uint64(len(br.block))-checksumSize]
	return len(br.block), nil
}

// IVWriter is the interface for versioned snapshot writer.
type IVWriter interface {
	io.WriteCloser
	GetVersion() SSVersion
	GetPayloadSum() []byte
	GetPayloadSize(uint64) uint64
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

var _ IVWriter = (*v1writer)(nil)

func newV1Wrtier(f io.Writer) *v1writer {
	// v1 is hard coded to use pb.CRC32IEEE
	h := mustGetChecksum(pb.CRC32IEEE)
	return &v1writer{f: io.MultiWriter(f, h), h: h}
}

func (v1w *v1writer) Write(data []byte) (int, error) {
	return v1w.f.Write(data)
}

func (v1w *v1writer) GetVersion() SSVersion {
	return V1
}

func (v1w *v1writer) GetPayloadSize(sz uint64) uint64 {
	return sz
}

func (v1w *v1writer) GetPayloadSum() []byte {
	return v1w.h.Sum(nil)
}

func (v1w *v1writer) Close() error {
	return nil
}

type v1reader struct {
	r io.Reader
	h hash.Hash
}

var _ IVReader = (*v1reader)(nil)

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

var _ IVWriter = (*v2writer)(nil)

func newV2Writer(fw io.Writer, t pb.ChecksumType) *v2writer {
	onBlock := func(data []byte, crc []byte) error {
		if len(crc) > 0 && uint64(len(crc)) != checksumSize {
			panic("unexpected crc length")
		}
		_, err := fw.Write(append(data, crc...))
		return err
	}
	return &v2writer{
		bw: newBlockWriter(blockSize, onBlock, t),
	}
}

func (v2w *v2writer) Write(data []byte) (int, error) {
	return v2w.bw.Write(data)
}

func (v2w *v2writer) GetVersion() SSVersion {
	return V2
}

func (v2w *v2writer) GetPayloadSize(sz uint64) uint64 {
	return getV2PayloadSize(sz, blockSize)
}

func (v2w *v2writer) GetPayloadSum() []byte {
	return v2w.bw.GetPayloadChecksum()
}

func (v2w *v2writer) Close() error {
	return v2w.bw.Close()
}

type v2reader struct {
	br *blockReader
}

var _ IVReader = (*v2reader)(nil)

func newV2Reader(fr io.Reader, t pb.ChecksumType) *v2reader {
	return &v2reader{br: newBlockReader(fr, blockSize, t)}
}

func (br *v2reader) Read(data []byte) (int, error) {
	return br.br.Read(data)
}

func (br *v2reader) Sum() []byte {
	return []byte{0, 0, 0, 0}
}

func getHeaderFromFirstChunk(data []byte) ([]byte, []byte, bool) {
	if uint64(len(data)) < HeaderSize {
		panic("first chunk is too small")
	}
	sz := binary.LittleEndian.Uint64(data)
	if sz > HeaderSize-8 {
		return nil, nil, false
	}
	return data[8 : 8+sz], data[8+sz : 12+sz], true
}

// IVValidator is the interface for versioned validator.
type IVValidator interface {
	AddChunk(data []byte, chunkID uint64) bool
	Validate() bool
}

type v1validator struct {
	h      hash.Hash
	header pb.SnapshotHeader
}

var _ IVValidator = (*v1validator)(nil)

func newV1Validator(header pb.SnapshotHeader) *v1validator {
	return &v1validator{
		header: header,
		h:      mustGetChecksum(header.ChecksumType),
	}
}

func (v *v1validator) AddChunk(data []byte, chunkID uint64) bool {
	var p []byte
	if chunkID == 0 {
		p = data[HeaderSize:]
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
	h     hash.Hash
	block []byte
	total int
}

var _ IVValidator = (*v2validator)(nil)

func newV2Validator(h hash.Hash) *v2validator {
	return &v2validator{
		block: make([]byte, 0, blockSize+checksumSize),
		h:     h,
	}
}

func (v *v2validator) AddChunk(data []byte, chunkID uint64) bool {
	var p []byte
	if chunkID == 0 {
		p = data[HeaderSize:]
	} else {
		p = data
	}
	v.total += len(p)
	v.block = append(v.block, p...)
	for uint64(len(v.block)) >= 2*(blockSize+checksumSize) {
		block := v.block[:blockSize+checksumSize]
		v.block = v.block[blockSize+checksumSize:]
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
	for uint64(len(block)) > blockSize+checksumSize {
		c := block[:blockSize+checksumSize]
		block = block[blockSize+checksumSize:]
		if !v.validateBlock(c) {
			return false
		}
	}
	return len(block) == 0 || v.validateBlock(block)
}

func (v *v2validator) validateMagicSize(tail []byte) bool {
	if uint64(len(tail)) != tailSize {
		panic("invalid size")
	}
	if !bytes.Equal(tail[8:], writerMagicNumber) {
		return false
	}
	return binary.LittleEndian.Uint64(tail[:8]) == uint64(v.total)-tailSize
}

func (v *v2validator) validateBlock(block []byte) bool {
	return validateBlock(block, v.h)
}

// GetV2PayloadChecksum calculates the payload checksum of the specified
// snapshot file.
func GetV2PayloadChecksum(fp string, fs vfs.IFS) (crc []byte, err error) {
	offsets, err := getV2CRCOffsetList(fp, fs)
	if err != nil {
		return nil, err
	}
	t, err := getV2ChecksumType(fp, fs)
	if err != nil {
		return nil, err
	}
	h := mustGetChecksum(t)
	f, err := fs.Open(fp)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = firstError(err, f.Close())
	}()
	for _, offset := range offsets {
		crc := make([]byte, checksumSize)
		if _, err := f.ReadAt(crc, int64(offset)); err != nil {
			return nil, err
		}
		if _, err = h.Write(crc); err != nil {
			return nil, err
		}
	}
	crc = h.Sum(nil)
	return
}

func getV2ChecksumType(fp string, fs vfs.IFS) (ct pb.ChecksumType, err error) {
	reader, header, err := NewSnapshotReader(fp, fs)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = firstError(err, reader.Close())
	}()
	if header.Version != uint64(V2) {
		return pb.ChecksumType(0), errors.New("not a v2 snapshot file")
	}
	return header.ChecksumType, nil
}

func getV2CRCOffsetList(fp string, fs vfs.IFS) ([]uint64, error) {
	fi, err := fs.Stat(fp)
	if err != nil {
		return nil, err
	}
	return getV2CRCOffsetListFromFileSize(uint64(fi.Size()))
}

func getV2CRCOffsetListFromFileSize(sz uint64) ([]uint64, error) {
	if sz <= tailSize+HeaderSize {
		return nil, errors.New("invalid file size")
	}
	sz = sz - tailSize - HeaderSize
	result := make([]uint64, 0)
	offset := HeaderSize
	for sz > 0 {
		if sz >= blockSize+checksumSize {
			result = append(result, offset+blockSize)
			offset = offset + blockSize + checksumSize
			sz = sz - (blockSize + checksumSize)
		} else {
			result = append(result, offset+sz-checksumSize)
			break
		}
	}
	return result, nil
}
