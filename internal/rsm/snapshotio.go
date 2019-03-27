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

package rsm

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	pb "github.com/lni/dragonboat/raftpb"
)

type SnapshotVersion uint64

const (
	V1SnapshotVersion SnapshotVersion = 1
	V2SnapshotVersion SnapshotVersion = 2
	// Current snapshot binary format version.
	CurrentSnapshotVersion SnapshotVersion = V2SnapshotVersion
	// SnapshotHeaderSize is the size of snapshot in number of bytes.
	SnapshotHeaderSize = settings.SnapshotHeaderSize
	// which checksum type to use.
	// CRC32IEEE and google's highway hash are supported
	defaultChecksumType = pb.CRC32IEEE
	DefaultChecksumType = defaultChecksumType
)

func newCRC32Hash() hash.Hash {
	return crc32.NewIEEE()
}

func getChecksumType() pb.ChecksumType {
	return defaultChecksumType
}

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

func getVersionWriter(w io.Writer, v SnapshotVersion) (IVWriter, bool) {
	if v == V1SnapshotVersion {
		return newV1Wrtier(w), true
	} else if v == V2SnapshotVersion {
		return newV2Writer(w), true
	}
	return nil, false
}

func mustGetVersionWriter(w io.Writer, v SnapshotVersion) IVWriter {
	vw, ok := getVersionWriter(w, v)
	if !ok {
		plog.Panicf("failed to get version writer, v %d", v)
	}
	return vw
}

func getVersionReader(r io.Reader, v SnapshotVersion) (IVReader, bool) {
	if v == V1SnapshotVersion {
		return newV1Reader(r), true
	} else if v == V2SnapshotVersion {
		return newV2Reader(r), true
	}
	return nil, false
}

func mustGetVersionReader(r io.Reader, v SnapshotVersion) IVReader {
	vr, ok := getVersionReader(r, v)
	if !ok {
		plog.Panicf("failed to get version reader, v %d", v)
	}
	return vr
}

func getVersionValidator(header pb.SnapshotHeader) (IVValidator, bool) {
	v := (SnapshotVersion)(header.Version)
	if v == V1SnapshotVersion {
		return newV1Validator(header), true
	} else if v == V2SnapshotVersion {
		h, ok := getChecksum(header.ChecksumType)
		if !ok {
			return nil, false
		}
		return newV2Validator(h), true
	}
	return nil, false
}

// SnapshotWriter is an io.Writer used to write snapshot file.
type SnapshotWriter struct {
	vw   IVWriter
	file *os.File
	fp   string
}

// NewSnapshotWriter creates a new snapshot writer instance.
func NewSnapshotWriter(fp string,
	version SnapshotVersion) (*SnapshotWriter, error) {
	f, err := os.OpenFile(fp,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileutil.DefaultFileMode)
	if err != nil {
		return nil, err
	}
	dummy := make([]byte, SnapshotHeaderSize)
	for i := uint64(0); i < SnapshotHeaderSize; i++ {
		dummy[i] = 0
	}
	if _, err := f.Write(dummy); err != nil {
		return nil, err
	}
	sw := &SnapshotWriter{
		vw:   mustGetVersionWriter(f, version),
		file: f,
		fp:   fp,
	}
	return sw, nil
}

// Close closes the snapshot writer instance.
func (sw *SnapshotWriter) Close() error {
	if err := sw.file.Sync(); err != nil {
		return err
	}
	if err := fileutil.SyncDir(filepath.Dir(sw.fp)); err != nil {
		return err
	}
	return sw.file.Close()
}

// Write writes the specified data to the snapshot.
func (sw *SnapshotWriter) Write(data []byte) (int, error) {
	return sw.vw.Write(data)
}

func (sw *SnapshotWriter) Flush() error {
	return sw.vw.Flush()
}

func (sw *SnapshotWriter) GetPayloadSize(sz uint64) uint64 {
	return sw.vw.GetPayloadSize(sz)
}

func (sw *SnapshotWriter) GetPayloadChecksum() []byte {
	return sw.vw.GetPayloadSum()
}

// SaveHeader saves the snapshot header to the snapshot.
func (sw *SnapshotWriter) SaveHeader(smsz uint64, sz uint64) error {
	// for v2
	// the PayloadChecksu field is really the checksum of all block
	// checksums
	sh := pb.SnapshotHeader{
		SessionSize:     smsz,
		DataStoreSize:   sz,
		UnreliableTime:  uint64(time.Now().UnixNano()),
		PayloadChecksum: sw.GetPayloadChecksum(),
		ChecksumType:    getChecksumType(),
		Version:         uint64(sw.vw.GetVersion()),
	}
	data, err := sh.Marshal()
	if err != nil {
		panic(err)
	}
	headerHash := getDefaultChecksum()
	if _, err := headerHash.Write(data); err != nil {
		panic(err)
	}
	headerChecksum := headerHash.Sum(nil)
	sh.HeaderChecksum = headerChecksum
	data, err = sh.Marshal()
	if err != nil {
		panic(err)
	}
	if uint64(len(data)) > SnapshotHeaderSize-8 {
		panic("snapshot header is too large")
	}
	if _, err = sw.file.Seek(0, 0); err != nil {
		panic(err)
	}
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(len(data)))
	if _, err := sw.file.Write(lenbuf); err != nil {
		return err
	}
	if _, err := sw.file.Write(data); err != nil {
		return err
	}
	return nil
}

// SnapshotReader is an io.Reader for reading from snapshot files.
type SnapshotReader struct {
	r      IVReader
	file   *os.File
	header pb.SnapshotHeader
}

// NewSnapshotReader creates a new snapshot reader instance.
func NewSnapshotReader(fp string) (*SnapshotReader, error) {
	f, err := os.OpenFile(fp, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &SnapshotReader{file: f}, nil
}

// Close closes the snapshot reader instance.
func (sr *SnapshotReader) Close() error {
	return sr.file.Close()
}

// GetHeader returns the snapshot header instance.
func (sr *SnapshotReader) GetHeader() (pb.SnapshotHeader, error) {
	empty := pb.SnapshotHeader{}
	lenbuf := make([]byte, 8)
	n, err := io.ReadFull(sr.file, lenbuf)
	if err != nil {
		return empty, err
	}
	if n != len(lenbuf) {
		return empty, io.ErrUnexpectedEOF
	}
	sz := binary.LittleEndian.Uint64(lenbuf)
	if sz > SnapshotHeaderSize-8 {
		panic("invalid snapshot header size")
	}
	data := make([]byte, sz)
	n, err = io.ReadFull(sr.file, data)
	if err != nil {
		return empty, err
	}
	if n != len(data) {
		return empty, io.ErrUnexpectedEOF
	}
	if err := sr.header.Unmarshal(data); err != nil {
		panic(err)
	}
	offset, err := sr.file.Seek(int64(SnapshotHeaderSize), 0)
	if err != nil {
		return empty, err
	}
	if uint64(offset) != SnapshotHeaderSize {
		return empty, io.ErrUnexpectedEOF
	}
	var reader io.Reader = sr.file
	if (SnapshotVersion)(sr.header.Version) == V2SnapshotVersion {
		st, err := sr.file.Stat()
		if err != nil {
			return empty, err
		}
		fileSz := st.Size()
		payloadSz := fileSz - int64(SnapshotHeaderSize) - int64(tailSize)
		reader = io.LimitReader(reader, payloadSz)
	}
	sr.r = mustGetVersionReader(reader, (SnapshotVersion)(sr.header.Version))
	return sr.header, nil
}

// Read reads up to len(data) bytes from the snapshot file.
func (sr *SnapshotReader) Read(data []byte) (int, error) {
	if sr.r == nil {
		panic("Read called before GetHeader")
	}
	return sr.r.Read(data)
}

// ValidatePayload validates whether the snapshot content matches the checksum
// recorded in the header.
func (sr *SnapshotReader) ValidatePayload(header pb.SnapshotHeader) {
	if sr.r == nil {
		panic("ValidatePayload called when the header is not even read")
	}
	if sr.header.Version == uint64(V1SnapshotVersion) {
		checksum := sr.r.Sum()
		if !bytes.Equal(checksum, header.PayloadChecksum) {
			panic("corrupted snapshot payload")
		}
	}
}

// ValidateHeader validates whether the header matches the header checksum
// recorded in the header.
func (sr *SnapshotReader) ValidateHeader(header pb.SnapshotHeader) {
	if !validateHeader(header) {
		panic("corrupted snapshot header")
	}
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
		header, ok := getHeaderFromFirstChunk(data)
		if !ok {
			return false
		}
		if v.v != nil {
			return false
		}
		if !validateHeader(header) {
			return false
		}
		v.v, ok = getVersionValidator(header)
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

func IsShrinkedSnapshotFile(fp string) (bool, error) {
	reader, err := NewSnapshotReader(fp)
	if err != nil {
		return false, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			panic(err)
		}
	}()
	if _, err := reader.GetHeader(); err != nil {
		return false, err
	}
	sz := make([]byte, 8)
	if _, err := io.ReadFull(reader, sz); err != nil {
		return false, err
	}
	if _, err := io.ReadFull(reader, sz); err != nil {
		return false, err
	}
	if size := binary.LittleEndian.Uint64(sz); size != 0 {
		return false, nil
	}
	oneByte := make([]byte, 1)
	n, err := io.ReadFull(reader, oneByte)
	if err == nil || n != 0 {
		return false, nil
	} else if err == io.ErrUnexpectedEOF || err == io.EOF {
		return true, nil
	}
	return false, err
}

func mustInSameDir(fp string, newFp string) {
	if filepath.Dir(fp) != filepath.Dir(newFp) {
		plog.Panicf("not in the same dir, dir 1: %s, dir 2: %s",
			filepath.Dir(fp), filepath.Dir(newFp))
	}
}

func ShrinkSnapshot(fp string, newFp string) error {
	mustInSameDir(fp, newFp)
	reader, err := NewSnapshotReader(fp)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			panic(err)
		}
	}()
	writer, err := NewSnapshotWriter(newFp, CurrentSnapshotVersion)
	if err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()
	if _, err := reader.GetHeader(); err != nil {
		return err
	}
	if _, err := writer.Write(GetEmptyLRUSession()); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return writer.SaveHeader(0, 0)
}

func ReplaceSnapshotFile(newFp string, fp string) error {
	mustInSameDir(fp, newFp)
	if err := os.Rename(newFp, fp); err != nil {
		return err
	}
	return fileutil.SyncDir(filepath.Dir(fp))
}
