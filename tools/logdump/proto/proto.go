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

package proto

import (
	"encoding/json"
	"fmt"
	"sync"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type ProtoField struct {
	ID       int32          `json:"id"`
	WireType protowire.Type `json:"-"`

	Type  string      `json:"type_url,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

// ProtoMessage represents a decoded protobuf message.
type ProtoMessage []ProtoField

// Type registry for protojson decode of known Any types.
var (
	registryMu    sync.RWMutex
	registryTypes = map[string]proto.Message{}
)

func init() {
	var file_google_protobuf_wrappers_proto_goTypes = []interface{}{
		(*wrapperspb.DoubleValue)(nil), // 0: google.protobuf.DoubleValue
		(*wrapperspb.FloatValue)(nil),  // 1: google.protobuf.FloatValue
		(*wrapperspb.Int64Value)(nil),  // 2: google.protobuf.Int64Value
		(*wrapperspb.UInt64Value)(nil), // 3: google.protobuf.UInt64Value
		(*wrapperspb.Int32Value)(nil),  // 4: google.protobuf.Int32Value
		(*wrapperspb.UInt32Value)(nil), // 5: google.protobuf.UInt32Value
		(*wrapperspb.BoolValue)(nil),   // 6: google.protobuf.BoolValue
		(*wrapperspb.StringValue)(nil), // 7: google.protobuf.StringValue
		(*wrapperspb.BytesValue)(nil),  // 8: google.protobuf.BytesValue
	}

	for _, m := range file_google_protobuf_wrappers_proto_goTypes {
		if m, ok := m.(proto.Message); ok {
			RegisterProtoType(m)
		}
	}
}

// RegisterProtoType registers a proto.Message for protojson decode when
// a google.protobuf.Any with a matching type_url is encountered.
func RegisterProtoType(m proto.Message) {
	if m == nil {
		panic("RegisterProtoType: nil message")
	}
	name := string(m.ProtoReflect().Descriptor().FullName())
	typeURL := "type.googleapis.com/" + name
	registryMu.Lock()
	registryTypes[typeURL] = m
	registryMu.Unlock()
}

func lookupRegisteredType(typeURL string) (proto.Message, bool) {
	registryMu.RLock()
	m, ok := registryTypes[typeURL]
	registryMu.RUnlock()
	return m, ok
}

// safeIntValue returns the integer as a float64, or as a string if the value
// exceeds 2^53 where float64 precision loss would occur.
func safeIntValue(v uint64) interface{} {
	if v > 9007199254740992 {
		return fmt.Sprintf("%d", v)
	}
	return float64(v)
}

// bytesValue returns a human-readable representation of raw bytes.
// Printable UTF-8 strings are returned as-is; otherwise hex-encoded.
func bytesValue(raw []byte) interface{} {
	if utf8.Valid(raw) && isPrintable(string(raw)) {
		return string(raw)
	}

	return raw
}

// isPlausibleMessage heuristically checks whether raw bytes might be
// a protobuf message by peeking at the first byte's wire type.
func isPlausibleMessage(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	wt := data[0] & 0x7
	return wt <= 5
}

const anyTypePrefix = "type.googleapis.com/"

// isAnyShape detects a google.protobuf.Any wrapper by its 2-field shape:
// field 1 = type_url (string), field 2 = value (bytes).
func isAnyShape(fields []ProtoField) (typeURL string, ok bool) {
	if len(fields) < 1 {
		return "", false
	}
	f1 := fields[0]
	if f1.ID != 1 || f1.WireType != protowire.BytesType {
		return "", false
	}
	s, isStr := f1.Value.(string)
	if !isStr {
		return "", false
	}
	if len(s) <= len(anyTypePrefix) || s[:len(anyTypePrefix)] != anyTypePrefix {
		return "", false
	}
	if len(fields) > 1 {
		f2 := fields[1]
		if f2.ID != 2 || f2.WireType != protowire.BytesType {
			return "", false
		}
	}
	return s, true
}

const maxWalkDepth = 64

// WalkProtoMessage parses raw protobuf wire-format bytes into a ProtoMessage.
// It auto-detects google.protobuf.Any wrappers and transparently unwraps them.
// Nested message depth is limited to 64.
func WalkProtoMessage(data []byte) (ProtoMessage, error) {
	if len(data) == 0 {
		return ProtoMessage{}, nil
	}

	fields, n, err := walkFields(data, 0)
	if err != nil {
		return nil, err
	}

	if n != len(data) {
		return nil, fmt.Errorf("trailing bytes after protobuf message: %d of %d consumed", n, len(data))
	}

	return fields, nil
}

func walkFields(data []byte, depth int) ([]ProtoField, int, error) {
	start := len(data)

	if depth >= maxWalkDepth {
		return nil, 0, fmt.Errorf("max protobuf nesting depth %d exceeded", maxWalkDepth)
	}

	var fields []ProtoField
	for len(data) > 0 {
		tag, wt, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fields, 0, fmt.Errorf("truncated protobuf tag")
		}
		data = data[n:]

		field := ProtoField{ID: int32(tag), WireType: wt}

		switch wt {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fields, start - len(data), fmt.Errorf("truncated varint for field %d", tag)
			}
			field.Value = safeIntValue(v)
			data = data[n:]
		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return fields, start - len(data), fmt.Errorf("truncated fixed32 for field %d", tag)
			}
			field.Value = safeIntValue(uint64(v))
			data = data[n:]
		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return fields, start - len(data), fmt.Errorf("truncated fixed64 for field %d", tag)
			}
			field.Value = safeIntValue(v)
			data = data[n:]
		case protowire.BytesType:
			raw, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return fields, start - len(data), fmt.Errorf("truncated length-delimited for field %d", tag)
			}
			field.Value = bytesValue(raw)
			data = data[n:]

			if isPlausibleMessage(raw) {
				children, _, err := walkFields(raw, depth+1)
				if err == nil {
					if typeURL, ok := isAnyShape(children); ok {
						field.Type = typeURL
						field.Value = nil
						if len(children) > 1 {
							field.Value = children[1].Value
						}
						if regMsg, registered := lookupRegisteredType(typeURL); registered {
							msg := regMsg.ProtoReflect().New().Interface()
							if err := proto.Unmarshal(raw, msg); err != nil {
								return fields, start - len(data), fmt.Errorf("failed to unmarshal Any message for field %d: %w", tag, err)
							}
							// convert to some universal JSON representation using protojson.
							mo := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: true}
							b, err := mo.Marshal(msg)
							if err != nil {
								return fields, start - len(data), fmt.Errorf("failed to marshal Any message to JSON for field %d: %w", tag, err)
							}
							field.Value = json.RawMessage(b)
						}
					} else if len(children) > 0 {
						field.Value = ProtoMessage(children)
					}
				}
			}
		case protowire.StartGroupType:
			children, n, err := walkFields(data, depth+1)
			if err != nil {
				return fields, start - len(data), fmt.Errorf("failed to walk group for field %d: %w", tag, err)
			}
			data = data[n:]
			if len(children) > 0 {
				field.Value = ProtoMessage(children)
			}
		case protowire.EndGroupType:
			consumed := start - len(data)
			return fields, consumed, nil
		}

		fields = append(fields, field)
	}

	return fields, start - len(data), nil
}
