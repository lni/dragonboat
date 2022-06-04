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
	"encoding/binary"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/utils/dio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// Entry Cmd format when Type = pb.EncodedEntry
//
// -------------------------------------
// |Version|CompressionFlag|SessionFlag|
// | 4Bits |     3Bits     |   1Bit    |
// -------------------------------------
const (
	EEHeaderSize uint8 = 1
	EEVersion    uint8 = 0 << 4
	EEV0         uint8 = 0 << 4

	// for V0 format, entries with empty payload will cause panic as such
	// entries always have their TYPE value set to ApplicationEntry
	EENoCompression uint8 = 0 << 1
	EESnappy        uint8 = 1 << 1

	EENoSession  uint8 = 0
	EEHasSession uint8 = 1
	// uncompressed size is binary.Uvarint encoded
	EEV0SizeOffset int = 1
)

// GetMaxBlockSize returns the maximum block length supported by the specified
// compression type.
func GetMaxBlockSize(ct config.CompressionType) uint64 {
	return dio.MaxBlockLen(ToDioType(ct))
}

// GetPayload returns the payload of the entry ready to be applied into the
// state machine.
func GetPayload(e pb.Entry) ([]byte, error) {
	if e.Type == pb.ApplicationEntry || e.Type == pb.ConfigChangeEntry {
		return e.Cmd, nil
	} else if e.Type == pb.EncodedEntry {
		return getDecodedPayload(e.Cmd, nil)
	}
	panic("unknown entry type")
}

// ToDioType converts the CompressionType type defined in the config package to
// the CompressionType value defined in the dio package.
func ToDioType(ct config.CompressionType) dio.CompressionType {
	return ct
}

// GetEncoded returns the encoded payload using the specified compression type
// and the default encoded entry version.
func GetEncoded(ct dio.CompressionType, cmd []byte, dst []byte) []byte {
	if len(cmd) == 0 {
		panic("empty payload")
	}
	return getEncoded(ct, cmd, dst)
}

// get v0 encoded payload
func getEncoded(ct dio.CompressionType, cmd []byte, dst []byte) []byte {
	if ct == dio.NoCompression {
		// output is 1 byte header, len(cmd) bytes of payload
		if len(dst) < len(cmd)+1 {
			dst = make([]byte, len(cmd)+1)
		}
		dst[0] = getEncodedHeader(EEV0, EENoCompression, false)
		copy(dst[1:], cmd)
		return dst[:len(cmd)+1]
	} else if ct == dio.Snappy {
		maxSize, ok := dio.MaxEncodedLen(dio.Snappy, uint64(len(cmd)))
		if !ok {
			panic("invalid payload length")
		}
		// 1 byte header
		maxSize = maxSize + 1
		if uint64(len(dst)) < maxSize {
			dst = make([]byte, maxSize)
		}
		dst[0] = getEncodedHeader(EEV0, EESnappy, false)
		dstLen := dio.CompressSnappyBlock(cmd, dst[1:])
		result := make([]byte, dstLen+1)
		copy(result, dst[:dstLen+1])
		return result
	}
	panic("unknown compression type")
}

func getEncodedHeader(version uint8, cf uint8, session bool) uint8 {
	result := uint8(0)
	result = result | version
	result = result | cf
	if session {
		result = result | EEHasSession
	} else {
		result = result | EENoSession
	}
	return result
}

func parseEncodedHeader(cmd []byte) (uint8, uint8, bool) {
	vermask := uint8(15 << 4)
	ctmask := uint8(7 << 1)
	sesmask := uint8(1)
	header := cmd[0]
	return header & vermask, header & ctmask, header&sesmask == 1
}

func getDecodedPayload(cmd []byte, buf []byte) ([]byte, error) {
	ver, ct, hasSession := parseEncodedHeader(cmd)
	if ver == EEV0 {
		if hasSession {
			plog.Panicf("v0 cmd has session info")
		}
		if ct == EENoCompression {
			return getV0NoCompressPayload(cmd), nil
		} else if ct == EESnappy {
			sz, offset := getV0PayloadUncompressedSize(cmd)
			if sz == 0 {
				plog.Panicf("empty uncompressed size found")
			}
			if offset == 0 {
				plog.Panicf("zero offset found")
			}
			compressed := cmd[EEV0SizeOffset:]
			var result []byte
			if uint64(len(buf)) >= sz {
				result = buf[:sz]
			} else {
				result = make([]byte, sz)
			}
			if err := dio.DecompressSnappyBlock(compressed, result); err != nil {
				return nil, err
			}
			return result, nil
		} else {
			plog.Panicf("unknown compression type %d", ct)
		}
	}
	panic("unknown cmd encoding version")
}

func getV0NoCompressPayload(cmd []byte) []byte {
	return cmd[EEHeaderSize:]
}

func getV0PayloadUncompressedSize(cmd []byte) (uint64, int) {
	return binary.Uvarint(cmd[EEV0SizeOffset:])
}
