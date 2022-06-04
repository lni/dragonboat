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
	"hash"
	"hash/crc32"
	"io"
	"time"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// SSVersion is the snapshot version value type.
type SSVersion uint64

const (
	// V1 is the value of snapshot version 1.
	V1 SSVersion = 1
	// V2 is the value of snapshot version 2.
	V2 SSVersion = 2
	// DefaultVersion is the snapshot binary format version.
	DefaultVersion SSVersion = V2
	// HeaderSize is the size of snapshot in number of bytes.
	HeaderSize = settings.SnapshotHeaderSize
	// which checksum type to use.
	// CRC32IEEE and google's highway hash are supported
	defaultChecksumType = pb.CRC32IEEE
	// DefaultChecksumType is the default checksum type.
	DefaultChecksumType = defaultChecksumType
)

func newCRC32Hash() hash.Hash {
	return crc32.NewIEEE()
}

func getChecksumType() pb.ChecksumType {
	return defaultChecksumType
}

// GetDefaultChecksum returns the default hash.Hash instance.
func GetDefaultChecksum() hash.Hash {
	return getDefaultChecksum()
}

func getDefaultChecksum() hash.Hash {
	return mustGetChecksum(getChecksumType())
}

func getChecksum(t pb.ChecksumType) (hash.Hash, bool) {
	if t == pb.CRC32IEEE {
		return newCRC32Hash(), true
	}
	return nil, false
}

func mustGetChecksum(t pb.ChecksumType) hash.Hash {
	c, ok := getChecksum(t)
	if !ok {
		plog.Panicf("failed to get checksum, type %d", t)
	}
	return c
}

func getVersionedWriter(w io.Writer, v SSVersion) (IVWriter, bool) {
	if v == V1 {
		return newV1Wrtier(w), true
	} else if v == V2 {
		return newV2Writer(w, defaultChecksumType), true
	}
	return nil, false
}

func mustGetVersionedWriter(w io.Writer, v SSVersion) IVWriter {
	vw, ok := getVersionedWriter(w, v)
	if !ok {
		plog.Panicf("failed to get version writer, v %d", v)
	}
	return vw
}

func getVersionedReader(r io.Reader,
	v SSVersion, t pb.ChecksumType) (IVReader, bool) {
	if v == V1 {
		return newV1Reader(r), true
	} else if v == V2 {
		return newV2Reader(r, t), true
	}
	return nil, false
}

func mustGetVersionedReader(r io.Reader,
	v SSVersion, t pb.ChecksumType) IVReader {
	vr, ok := getVersionedReader(r, v, t)
	if !ok {
		plog.Panicf("failed to get version reader, v %d", v)
	}
	return vr
}

func getVersionedValidator(header pb.SnapshotHeader) (IVValidator, bool) {
	v := (SSVersion)(header.Version)
	if v == V1 {
		return newV1Validator(header), true
	} else if v == V2 {
		h, ok := getChecksum(header.ChecksumType)
		if !ok {
			return nil, false
		}
		return newV2Validator(h), true
	}
	return nil, false
}

// GetWitnessSnapshot returns the content of a witness snapshot.
func GetWitnessSnapshot(fs vfs.IFS) (result []byte, err error) {
	f, path, err := fileutil.TempFile("", "dragonboat-witness-snapshot", fs)
	if err != nil {
		return nil, err
	}
	w, err := newSnapshotWriter(f, path, DefaultVersion, pb.NoCompression, fs)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(GetEmptyLRUSession()); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	df, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = firstError(err, df.Close())
	}()
	buf := bytes.NewBuffer(nil)
	if _, err = io.Copy(buf, df); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SnapshotWriter is an io.Writer used to write snapshot file.
type SnapshotWriter struct {
	vw     IVWriter
	file   vfs.File
	fs     vfs.IFS
	fp     string
	ct     pb.CompressionType
	closed bool
}

// NewSnapshotWriter creates a new snapshot writer instance.
func NewSnapshotWriter(fp string,
	ct pb.CompressionType, fs vfs.IFS) (*SnapshotWriter, error) {
	return newVersionedSnapshotWriter(fp, DefaultVersion, ct, fs)
}

func newVersionedSnapshotWriter(fp string,
	v SSVersion, ct pb.CompressionType, fs vfs.IFS) (*SnapshotWriter, error) {
	f, err := fs.Create(fp)
	if err != nil {
		return nil, err
	}
	return newSnapshotWriter(f, fp, v, ct, fs)
}

func newSnapshotWriter(f vfs.File, fp string,
	v SSVersion, ct pb.CompressionType, fs vfs.IFS) (*SnapshotWriter, error) {
	dummy := make([]byte, HeaderSize)
	if _, err := f.Write(dummy); err != nil {
		return nil, err
	}
	sw := &SnapshotWriter{
		vw:   mustGetVersionedWriter(f, v),
		file: f,
		fp:   fp,
		ct:   ct,
		fs:   fs,
	}
	return sw, nil
}

// Close closes the snapshot writer instance.
func (sw *SnapshotWriter) Close() (err error) {
	sw.closed = true
	err = firstError(err, sw.flush())
	err = firstError(err, sw.saveHeader())
	err = firstError(err, sw.file.Sync())
	err = firstError(err, sw.file.Close())
	return firstError(err, fileutil.SyncDir(sw.fs.PathDir(sw.fp), sw.fs))
}

// Write writes the specified data to the snapshot.
func (sw *SnapshotWriter) Write(data []byte) (int, error) {
	return sw.vw.Write(data)
}

// GetPayloadSize returns the payload size.
func (sw *SnapshotWriter) GetPayloadSize(sz uint64) uint64 {
	if !sw.closed {
		panic("not closed")
	}
	return sw.vw.GetPayloadSize(sz)
}

// GetPayloadChecksum returns the payload checksum.
func (sw *SnapshotWriter) GetPayloadChecksum() []byte {
	if !sw.closed {
		panic("not closed")
	}
	return sw.vw.GetPayloadSum()
}

func (sw *SnapshotWriter) flush() error {
	return sw.vw.Close()
}

func (sw *SnapshotWriter) saveHeader() error {
	// for v2, the PayloadChecksu field is really the checksum of all block
	// checksums
	sh := pb.SnapshotHeader{
		UnreliableTime:  uint64(time.Now().UnixNano()),
		PayloadChecksum: sw.GetPayloadChecksum(),
		ChecksumType:    getChecksumType(),
		Version:         uint64(sw.vw.GetVersion()),
		CompressionType: sw.ct,
	}
	data := pb.MustMarshal(&sh)
	headerHash := getDefaultChecksum()
	if _, err := headerHash.Write(data); err != nil {
		return err
	}
	sh.HeaderChecksum = headerHash.Sum(nil)
	data = pb.MustMarshal(&sh)
	if uint64(len(data)) > HeaderSize-8 {
		panic("snapshot header is too large")
	}
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(len(data)))
	if _, err := sw.file.WriteAt(lenbuf, 0); err != nil {
		return err
	}
	if _, err := sw.file.WriteAt(data, 8); err != nil {
		return err
	}
	return nil
}

// SnapshotReader is an io.Reader for reading from snapshot files.
type SnapshotReader struct {
	r      IVReader
	file   vfs.File
	header pb.SnapshotHeader
}

// NewSnapshotReader creates a new snapshot reader instance.
func NewSnapshotReader(fp string,
	fs vfs.IFS) (*SnapshotReader, pb.SnapshotHeader, error) {
	f, err := fs.Open(fp)
	if err != nil {
		return nil, pb.SnapshotHeader{}, err
	}
	r := &SnapshotReader{file: f}
	header, err := r.getHeader()
	if err != nil {
		return nil, pb.SnapshotHeader{}, err
	}
	return r, header, nil
}

// Close closes the snapshot reader instance.
func (sr *SnapshotReader) Close() error {
	// defer is used here to make sure file is always closed, otherwise tests that
	// panic in validatePayload() will complain about not closed file.
	defer sr.validatePayload()
	return sr.file.Close()
}

// Read reads up to len(data) bytes from the snapshot file.
func (sr *SnapshotReader) Read(data []byte) (int, error) {
	return sr.r.Read(data)
}

func (sr *SnapshotReader) validatePayload() {
	if sr.header.Version == uint64(V1) {
		if !bytes.Equal(sr.r.Sum(), sr.header.PayloadChecksum) {
			panic("corrupted snapshot payload")
		}
	}
}

func (sr *SnapshotReader) getHeader() (pb.SnapshotHeader, error) {
	if sr.r != nil {
		panic("getHeader called again?")
	}
	empty := pb.SnapshotHeader{}
	lenbuf := make([]byte, 8)
	if _, err := io.ReadFull(sr.file, lenbuf); err != nil {
		return empty, err
	}
	sz := binary.LittleEndian.Uint64(lenbuf)
	if sz > HeaderSize-8 {
		panic("invalid snapshot header size")
	}
	data := make([]byte, sz)
	if _, err := io.ReadFull(sr.file, data); err != nil {
		return empty, err
	}
	pb.MustUnmarshal(&sr.header, data)
	crcdata := make([]byte, 4)
	if _, err := io.ReadFull(sr.file, crcdata); err != nil {
		return empty, err
	}
	if !validateHeader(data, crcdata) {
		panic("corrupted header")
	}
	blank := make([]byte, HeaderSize-8-sz-4)
	if _, err := io.ReadFull(sr.file, blank); err != nil {
		return empty, err
	}
	var reader io.Reader = sr.file
	v := SSVersion(sr.header.Version)
	if v == V2 {
		st, err := sr.file.Stat()
		if err != nil {
			return empty, err
		}
		payloadSz := st.Size() - int64(HeaderSize) - int64(tailSize)
		reader = io.LimitReader(reader, payloadSz)
	}
	sr.r = mustGetVersionedReader(reader, v, sr.header.ChecksumType)
	return sr.header, nil
}

var fourZeroBytes = []byte{0, 0, 0, 0}

func validateHeader(header []byte, crc32 []byte) bool {
	if len(crc32) != 4 {
		plog.Panicf("invalid crc32 len: %d", len(crc32))
	}
	if !bytes.Equal(crc32, fourZeroBytes) {
		h := newCRC32Hash()
		fileutil.MustWrite(h, header)
		return bytes.Equal(h.Sum(nil), crc32)
	}
	return true
}

// SnapshotValidator is the validator used to check incoming snapshot chunks.
type SnapshotValidator struct {
	v IVValidator
}

// NewSnapshotValidator creates and returns a new SnapshotValidator instance.
func NewSnapshotValidator() *SnapshotValidator {
	return &SnapshotValidator{}
}

// AddChunk adds a new snapshot chunk to the validator.
func (v *SnapshotValidator) AddChunk(data []byte, chunkID uint64) bool {
	if chunkID == 0 {
		header, crc, ok := getHeaderFromFirstChunk(data)
		if !ok {
			plog.Errorf("failed to get header from first chunk")
			return false
		}
		if v.v != nil {
			return false
		}
		if !validateHeader(header, crc) {
			plog.Errorf("validate header failed")
			return false
		}
		var headerRec pb.SnapshotHeader
		pb.MustUnmarshal(&headerRec, header)
		v.v, ok = getVersionedValidator(headerRec)
		if !ok {
			return false
		}
	} else {
		if v.v == nil {
			return false
		}
	}
	return v.v.AddChunk(data, chunkID)
}

// Validate validates the added chunks and return a boolean flag indicating
// whether the snapshot chunks are valid.
func (v *SnapshotValidator) Validate() bool {
	if v.v == nil {
		return false
	}
	return v.v.Validate()
}

// IsShrunkSnapshotFile returns a boolean flag indicating whether the
// specified snapshot file is already shrunk.
func IsShrunkSnapshotFile(fp string, fs vfs.IFS) (shrunk bool, err error) {
	reader, _, err := NewSnapshotReader(fp, fs)
	if err != nil {
		return false, err
	}
	defer func() {
		err = firstError(err, reader.Close())
	}()
	sz := make([]byte, 8)
	if _, err := io.ReadFull(reader, sz); err != nil {
		return false, err
	}
	if _, err = io.ReadFull(reader, sz); err != nil {
		return false, err
	}
	if size := binary.LittleEndian.Uint64(sz); size != 0 {
		return false, nil
	}
	oneByte := make([]byte, 1)
	if _, err := io.ReadFull(reader, oneByte); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func mustInSameDir(fp string, newFp string, fs vfs.IFS) {
	if fs.PathDir(fp) != fs.PathDir(newFp) {
		plog.Panicf("not in the same dir, dir 1: %s, dir 2: %s",
			fs.PathDir(fp), fs.PathDir(newFp))
	}
}

// ShrinkSnapshot shrinks the specified snapshot file and save the generated
// shrunk version to the path specified by newFp.
func ShrinkSnapshot(fp string, newFp string, fs vfs.IFS) (err error) {
	mustInSameDir(fp, newFp, fs)
	reader, _, err := NewSnapshotReader(fp, fs)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, reader.Close())
	}()
	writer, err := NewSnapshotWriter(newFp, pb.NoCompression, fs)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, writer.Close())
	}()
	if _, err := writer.Write(GetEmptyLRUSession()); err != nil {
		return err
	}
	return nil
}

// ReplaceSnapshot replace the specified snapshot file with the shrunk
// version atomically.
func ReplaceSnapshot(newFp string, fp string, fs vfs.IFS) error {
	mustInSameDir(fp, newFp, fs)
	if err := fs.Rename(newFp, fp); err != nil {
		return err
	}
	return fileutil.SyncDir(fs.PathDir(fp), fs)
}
