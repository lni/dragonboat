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
	"math"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ---------------------------------------------------------------------------
// SafeIntValue
// ---------------------------------------------------------------------------

func TestSafeIntValue_Boundary(t *testing.T) {
	tests := []struct {
		name string
		v    uint64
		want interface{}
	}{
		{"zero", 0, float64(0)},
		{"one", 1, float64(1)},
		{"small", 42, float64(42)},
		{"exact_2pow53", 9007199254740992, float64(9007199254740992)},
		{"above_2pow53", 9007199254740993, "9007199254740993"},
		{"max_uint64", math.MaxUint64, "18446744073709551615"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeIntValue(tt.v)
			if got != tt.want {
				t.Errorf("safeIntValue(%d) = %#v, want %#v", tt.v, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IsPlausibleMessage
// ---------------------------------------------------------------------------

func TestIsPlausibleMessage_Valid(t *testing.T) {
	valid := [][]byte{
		{0x00},       // wt=0 (varint)
		{0x08},       // wt=0 (varint), fn=1
		{0x09},       // wt=1 (fixed64), fn=1
		{0x0a},       // wt=2 (bytes), fn=1
		{0x0b},       // wt=3 (start_group), fn=1
		{0x0c},       // wt=4 (end_group), fn=1
		{0x0d},       // wt=5 (fixed32), fn=1
		{0x12},       // wt=2 (bytes), fn=2
		{0x1d},       // wt=5 (fixed32), fn=3
		{0x22},       // wt=2 (bytes), fn=4
		{0x2d},       // wt=5 (fixed32), fn=5
		{0x08, 0x00}, // valid varint field with value 0
	}
	for _, b := range valid {
		if !isPlausibleMessage(b) {
			t.Errorf("isPlausibleMessage(%x) = false, want true", b)
		}
	}
}

func TestIsPlausibleMessage_Invalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"empty_slice", []byte{}},
		{"wt6", []byte{0x06}},              // illegal wire type 6
		{"wt7", []byte{0x07}},              // illegal wire type 7
		{"fn1_wt6", []byte{0x0e}},          // fn=1, wt=6
		{"fn1_wt7", []byte{0x0f}},          // fn=1, wt=7
		{"fn2_wt6", []byte{0x16}},          // fn=2, wt=6
		{"high_wt6", []byte{0x3e}},         // fn=7, wt=6
		{"highest_reserved", []byte{0x3f}}, // fn=7, wt=7
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isPlausibleMessage(tt.data) {
				t.Errorf("isPlausibleMessage(%x) = true, want false", tt.data)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IsAnyShape
// ---------------------------------------------------------------------------

func TestIsAnyShape_Positive(t *testing.T) {
	fields := []ProtoField{
		{ID: 1, WireType: protowire.BytesType, Value: "type.googleapis.com/google.protobuf.UInt64Value"},
		{ID: 2, WireType: protowire.BytesType},
	}
	typeURL, ok := isAnyShape(fields)
	if !ok {
		t.Fatal("isAnyShape returned false for valid Any shape")
	}
	if typeURL != "type.googleapis.com/google.protobuf.UInt64Value" {
		t.Errorf("typeURL = %q, want %q", typeURL, "type.googleapis.com/google.protobuf.UInt64Value")
	}
}

func TestIsAnyShape_Negative(t *testing.T) {
	tests := []struct {
		name   string
		fields []ProtoField
	}{
		{
			"wrong_field_numbers_1_and_3",
			[]ProtoField{
				{ID: 1, WireType: protowire.BytesType, Value: "type.googleapis.com/SomeType"},
				{ID: 3, WireType: protowire.BytesType},
			},
		},
		{
			"wrong_field_numbers_2_and_3",
			[]ProtoField{
				{ID: 2, WireType: protowire.BytesType, Value: "type.googleapis.com/SomeType"},
				{ID: 3, WireType: protowire.BytesType},
			},
		},
		{
			"field1_not_string",
			[]ProtoField{
				{ID: 1, WireType: protowire.VarintType, Value: float64(42)},
				{ID: 2, WireType: protowire.BytesType},
			},
		},
		{
			"field1_wrong_prefix",
			[]ProtoField{
				{ID: 1, WireType: protowire.BytesType, Value: "x/type.googleapis.com/Foo"},
				{ID: 2, WireType: protowire.BytesType},
			},
		},
		{
			"field1_short_prefix",
			[]ProtoField{
				{ID: 1, WireType: protowire.BytesType, Value: "type.googleapis.com/"},
				{ID: 2, WireType: protowire.BytesType},
			},
		},
		{
			"field2_wrong_wire_type",
			[]ProtoField{
				{ID: 1, WireType: protowire.BytesType, Value: "type.googleapis.com/SomeType"},
				{ID: 2, WireType: protowire.VarintType, Value: float64(42)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := isAnyShape(tt.fields)
			if ok {
				t.Errorf("isAnyShape returned true, want false")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Field values
// ---------------------------------------------------------------------------

func TestWalkProtoMessage_VarintValues(t *testing.T) {
	tests := []struct {
		name    string
		val     uint64
		wantVal interface{}
	}{
		{"zero", 0, float64(0)},
		{"one", 1, float64(1)},
		{"large_2pow53", 9007199254740992, float64(9007199254740992)},
		{"above_float_safe", 9007199254740993, "9007199254740993"},
		{"max_uint64", math.MaxUint64, "18446744073709551615"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := protowire.AppendTag(nil, 1, protowire.VarintType)
			b = protowire.AppendVarint(b, tt.val)

			pm, err := WalkProtoMessage(b)
			if err != nil {
				t.Fatalf("WalkProtoMessage: %v", err)
			}
			if len(pm) != 1 {
				t.Fatalf("got %d fields, want 1", len(pm))
			}
			f := pm[0]
			if f.ID != 1 {
				t.Errorf("ID = %d, want 1", f.ID)
			}
			if f.WireType != protowire.VarintType {
				t.Errorf("WireType = %q, want varint", f.WireType)
			}
			if f.Value != tt.wantVal {
				t.Errorf("Value = %#v (%T), want %#v (%T)", f.Value, f.Value, tt.wantVal, tt.wantVal)
			}
		})
	}
}

func TestWalkProtoMessage_Fixed32(t *testing.T) {
	const val uint32 = 0xDEADBEEF
	b := protowire.AppendTag(nil, 2, protowire.Fixed32Type)
	b = protowire.AppendFixed32(b, val)

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f := pm[0]
	if f.ID != 2 {
		t.Errorf("ID = %d, want 2", f.ID)
	}
	if f.WireType != protowire.Fixed32Type {
		t.Errorf("WireType = %q, want fixed32", f.WireType)
	}
	if f.Value != float64(val) {
		t.Errorf("Value = %#v, want %#v", f.Value, float64(val))
	}
}

func TestWalkProtoMessage_Fixed64(t *testing.T) {
	const val uint64 = 0xABCD
	b := protowire.AppendTag(nil, 3, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, val)

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f := pm[0]
	if f.ID != 3 {
		t.Errorf("ID = %d, want 3", f.ID)
	}
	if f.WireType != protowire.Fixed64Type {
		t.Errorf("WireType = %q, want fixed64", f.WireType)
	}
	if f.Value != float64(val) {
		t.Errorf("Value = %#v (%T), want %#v", f.Value, f.Value, float64(val))
	}
}

func TestWalkProtoMessage_Fixed64HighValue(t *testing.T) {
	const val uint64 = 9007199254740993 // > 2^53
	b := protowire.AppendTag(nil, 1, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, val)

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	f := pm[0]
	if f.Value != "9007199254740993" {
		t.Errorf("Value = %#v (%T), want string \"9007199254740993\"", f.Value, f.Value)
	}
}

func TestWalkProtoMessage_StringField(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("hello world"))

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f := pm[0]
	if _, ok := f.Value.(ProtoMessage); !ok {
		t.Errorf("Value type = %T, want ProtoMessage (printable bytes parsed as nested)", f.Value)
	}
}

func TestWalkProtoMessage_BytesField_NonPrintable(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte{0x00, 0xFF, 0xDE, 0xAD})

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	f := pm[0]
	got, ok := f.Value.([]byte)
	if !ok {
		t.Fatalf("Value type = %T, want []byte", f.Value)
	}
	if len(got) != 4 || got[0] != 0x00 || got[1] != 0xFF || got[2] != 0xDE || got[3] != 0xAD {
		t.Errorf("Value = %#v, want []byte{0x00, 0xFF, 0xDE, 0xAD}", got)
	}
}

func TestWalkProtoMessage_BytesField_NonUTF8(t *testing.T) {
	// A byte sequence that is not valid UTF-8
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte{0xFF, 0xFE, 0xFD})

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	f := pm[0]
	got, ok := f.Value.([]byte)
	if !ok {
		t.Fatalf("Value type = %T, want []byte", f.Value)
	}
	if len(got) != 3 || got[0] != 0xFF || got[1] != 0xFE || got[2] != 0xFD {
		t.Errorf("Value = %#v, want []byte{0xFF, 0xFE, 0xFD}", got)
	}
}

func TestWalkProtoMessage_EmptyBytesField(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, nil)

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	f := pm[0]
	if f.Value != "" {
		t.Errorf("Value = %#v, want empty string", f.Value)
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Nested messages
// ---------------------------------------------------------------------------

func TestWalkProtoMessage_NestedMessages(t *testing.T) {
	inner := protowire.AppendTag(nil, 1, protowire.VarintType)
	inner = protowire.AppendVarint(inner, 100)

	outer := protowire.AppendTag(nil, 2, protowire.BytesType)
	outer = protowire.AppendBytes(outer, inner)

	pm, err := WalkProtoMessage(outer)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f := pm[0]
	if f.ID != 2 {
		t.Errorf("ID = %d, want 2", f.ID)
	}
	if f.WireType != protowire.BytesType {
		t.Errorf("WireType = %q, want length_delimited", f.WireType)
	}
	if len(f.Value.(ProtoMessage)) != 1 {
		t.Fatalf("got %d children, want 1", len(f.Value.(ProtoMessage)))
	}
	c := f.Value.(ProtoMessage)[0]
	if c.ID != 1 || c.WireType != protowire.VarintType || c.Value != float64(100) {
		t.Errorf("child: number=%d wire=%q val=%#v", c.ID, c.WireType, c.Value)
	}
}

func TestWalkProtoMessage_NestedMessagesTwoLevels(t *testing.T) {
	// Level 2: innermost
	l2 := protowire.AppendTag(nil, 1, protowire.VarintType)
	l2 = protowire.AppendVarint(l2, 42)

	// Level 1: wraps l2
	l1 := protowire.AppendTag(nil, 1, protowire.BytesType)
	l1 = protowire.AppendBytes(l1, l2)

	// Level 0: top-level, wraps l1
	l0 := protowire.AppendTag(nil, 1, protowire.BytesType)
	l0 = protowire.AppendBytes(l0, l1)

	pm, err := WalkProtoMessage(l0)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f0 := pm[0]
	if len(f0.Value.(ProtoMessage)) != 1 {
		t.Fatalf("level 0: got %d children, want 1", len(f0.Value.(ProtoMessage)))
	}
	f1 := f0.Value.(ProtoMessage)[0]
	if len(f1.Value.(ProtoMessage)) != 1 {
		t.Fatalf("level 1: got %d children, want 1", len(f1.Value.(ProtoMessage)))
	}
	f2 := f1.Value.(ProtoMessage)[0]
	if f2.Value != float64(42) {
		t.Errorf("level 2 value = %#v, want 42", f2.Value)
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Empty / Truncated / Group
// ---------------------------------------------------------------------------

func TestWalkProtoMessage_EmptyMessage(t *testing.T) {
	pm, err := WalkProtoMessage(nil)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 0 {
		t.Errorf("Fields length = %d, want 0", len(pm))
	}

	pm, err = WalkProtoMessage([]byte{})
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 0 {
		t.Errorf("Fields length = %d, want 0", len(pm))
	}
}

func TestWalkProtoMessage_TruncatedTag(t *testing.T) {
	// Incomplete varint tag (MSB set but no continuation)
	data := []byte{0x80}
	_, err := WalkProtoMessage(data)
	if err == nil {
		t.Fatal("expected error for truncated protobuf tag, got nil")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("error = %q, want error containing 'truncated'", err.Error())
	}
}

func TestWalkProtoMessage_TruncatedVarint(t *testing.T) {
	// Valid tag, then incomplete varint value
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = append(b, 0x80) // MSB set, expecting continuation
	_, err := WalkProtoMessage(b)
	if err == nil {
		t.Fatal("expected error for truncated varint, got nil")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("error = %q, want error containing 'truncated'", err.Error())
	}
}

func TestWalkProtoMessage_TruncatedFixed32(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.Fixed32Type)
	b = append(b, 0x01, 0x02) // only 2 bytes, need 4
	_, err := WalkProtoMessage(b)
	if err == nil {
		t.Fatal("expected error for truncated fixed32, got nil")
	}
}

func TestWalkProtoMessage_TruncatedFixed64(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.Fixed64Type)
	b = append(b, 0x01, 0x02, 0x03) // only 3 bytes, need 8
	_, err := WalkProtoMessage(b)
	if err == nil {
		t.Fatal("expected error for truncated fixed64, got nil")
	}
}

func TestWalkProtoMessage_TruncatedLengthDelimited(t *testing.T) {
	// Tag says bytes type, then a length varint of 5, but only 2 bytes follow
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	// Append length = 5 as a varint, then only 2 bytes
	b = append(b, 0x05, 0x01, 0x02)
	_, err := WalkProtoMessage(b)
	if err == nil {
		t.Fatal("expected error for truncated length-delimited, got nil")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("error = %q, want error containing 'truncated'", err.Error())
	}
}

func TestWalkProtoMessage_GroupWireType(t *testing.T) {
	b := protowire.AppendTag(nil, 1, protowire.StartGroupType)
	b = protowire.AppendTag(b, 2, protowire.VarintType)
	b = protowire.AppendVarint(b, 42)
	b = protowire.AppendTag(b, 1, protowire.EndGroupType)

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 1 {
		t.Fatalf("got %d fields, want 1", len(pm))
	}
	f := pm[0]
	if f.ID != 1 {
		t.Errorf("ID = %d, want 1", f.ID)
	}
	children, ok := f.Value.(ProtoMessage)
	if !ok {
		t.Fatalf("Value type = %T, want ProtoMessage", f.Value)
	}
	if len(children) != 1 || children[0].ID != 2 || children[0].Value != float64(42) {
		t.Errorf("children = %+v, want [{ID:2, Value:42}]", children)
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Depth limit
// ---------------------------------------------------------------------------

func TestWalkProtoMessage_DepthLimit(t *testing.T) {
	// Build a simple valid message (varint field 1 = 0).
	msg := protowire.AppendTag(nil, 1, protowire.VarintType)
	msg = protowire.AppendVarint(msg, 0)

	// At depth 63, walkFields should succeed.
	_, _, err := walkFields(msg, 63)
	if err != nil {
		t.Fatalf("walkFields at depth 63: unexpected error: %v", err)
	}

	// At depth 64, walkFields should return the depth limit error.
	_, _, err = walkFields(msg, 64)
	if err == nil {
		t.Fatal("walkFields at depth 64: expected depth limit error, got nil")
	}
	if !strings.Contains(err.Error(), "depth") {
		t.Errorf("error = %q, want error containing 'depth'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Any detection & auto-unwrap
// ---------------------------------------------------------------------------

func mustMarshalAny(m proto.Message) []byte {
	inner, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	a := &anypb.Any{
		TypeUrl: "type.googleapis.com/" + string(m.ProtoReflect().Descriptor().FullName()),
		Value:   inner,
	}
	b, err := proto.Marshal(a)
	if err != nil {
		panic(err)
	}
	return b
}

func TestWalkProtoMessage_AnyAutoUnwrap(t *testing.T) {
	anyBytes := mustMarshalAny(&wrapperspb.UInt64Value{Value: 42})

	pm, err := WalkProtoMessage(anyBytes)
	if err != nil {
		t.Fatalf("WalkProtoMessage(Any): %v", err)
	}
	// Top-level Any is not auto-unwrapped; returned as 2 raw fields.
	if len(pm) != 2 {
		t.Fatalf("got %d fields, want 2", len(pm))
	}
	// f0: type_url string field
	if pm[0].ID != 1 {
		t.Errorf("f0.ID = %d, want 1", pm[0].ID)
	}
	typeURL, ok := pm[0].Value.(string)
	if !ok {
		t.Fatalf("f0.Value type = %T, want string (type_url)", pm[0].Value)
	}
	if !strings.HasPrefix(typeURL, "type.googleapis.com/") {
		t.Errorf("typeURL = %q, want prefix type.googleapis.com/", typeURL)
	}
	// f1: value bytes, recursed into inner message
	if pm[1].ID != 2 {
		t.Errorf("f1.ID = %d, want 2", pm[1].ID)
	}
	children, ok := pm[1].Value.(ProtoMessage)
	if !ok {
		t.Fatalf("f1.Value type = %T, want ProtoMessage", pm[1].Value)
	}
	if len(children) == 0 {
		t.Fatal("Children empty; want recursed inner fields")
	}
	if children[0].ID != 1 || children[0].Value != float64(42) {
		t.Errorf("children[0] = %+v, want ID=1 Value=42", children[0])
	}
}

func TestWalkProtoMessage_AnyAutoUnwrap_NestedChildren(t *testing.T) {
	// Create an Any containing a wrapper with a larger value
	anyBytes := mustMarshalAny(&wrapperspb.UInt64Value{Value: 999})

	pm, err := WalkProtoMessage(anyBytes)
	if err != nil {
		t.Fatalf("WalkProtoMessage(Any): %v", err)
	}
	// Top-level Any is not auto-unwrapped.
	if len(pm) != 2 {
		t.Fatalf("got %d fields, want 2", len(pm))
	}
	// f1 contains the recursed inner message children
	children, ok := pm[1].Value.(ProtoMessage)
	if !ok {
		t.Fatalf("f1.Value type = %T, want ProtoMessage", pm[1].Value)
	}
	if len(children) == 0 {
		t.Error("Children not populated for Any inner message")
	}
}

func TestWalkProtoMessage_AnyNestedInRegularMessage(t *testing.T) {
	// Build a regular message that contains an Any as field 3 (bytes)
	anyBytes := mustMarshalAny(&wrapperspb.UInt64Value{Value: 42})

	outer := protowire.AppendTag(nil, 1, protowire.VarintType)
	outer = protowire.AppendVarint(outer, 1)
	outer = protowire.AppendTag(outer, 3, protowire.BytesType)
	outer = protowire.AppendBytes(outer, anyBytes)

	pm, err := WalkProtoMessage(outer)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 2 {
		t.Fatalf("got %d fields, want 2", len(pm))
	}
	anyField := pm[1]
	if anyField.ID != 3 {
		t.Errorf("ID = %d, want 3", anyField.ID)
	}
	if anyField.Type == "" {
		t.Error("Type not set on Any field nested in regular message")
	}
	// Registered type: Value is json.RawMessage (decoded), not ProtoMessage
	if anyField.Value == nil {
		t.Error("Value is nil; want decoded json.RawMessage for registered Any type")
	}
}

// ---------------------------------------------------------------------------
// RegisterProtoType
// ---------------------------------------------------------------------------

func clearRegistry() {
	registryMu.Lock()
	registryTypes = map[string]proto.Message{}
	registryMu.Unlock()
}

func TestRegisterProtoType_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("RegisterProtoType(nil) did not panic")
		}
	}()
	RegisterProtoType(nil)
}

func TestRegisterProtoType_WithMatchingAny(t *testing.T) {
	clearRegistry()

	// Register the UInt64 wrapper type
	RegisterProtoType(&wrapperspb.UInt64Value{Value: 0})

	anyBytes := mustMarshalAny(&wrapperspb.UInt64Value{Value: 42})

	// Wrap Any in a regular message (field 1=varint, field 3=bytes with Any)
	outer := protowire.AppendTag(nil, 1, protowire.VarintType)
	outer = protowire.AppendVarint(outer, 1)
	outer = protowire.AppendTag(outer, 3, protowire.BytesType)
	outer = protowire.AppendBytes(outer, anyBytes)

	pm, err := WalkProtoMessage(outer)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 2 {
		t.Fatalf("got %d fields, want 2", len(pm))
	}
	f := pm[1]
	if f.ID != 3 {
		t.Errorf("ID = %d, want 3", f.ID)
	}
	if f.Type == "" {
		t.Fatal("Type not populated")
	}
	// Registered type: Value is json.RawMessage (decoded), not ProtoMessage
	rawJSON, ok := f.Value.(json.RawMessage)
	if !ok {
		t.Fatalf("Value type = %T, want json.RawMessage (decoded for registered type)", f.Value)
	}

	// Validate the decoded JSON — wrapper types (UInt64Value) marshal as their
	// inner value via protojson, not as an object with a "value" key.
	var decoded string
	if err := json.Unmarshal(rawJSON, &decoded); err != nil {
		t.Fatalf("failed to unmarshal Decoded JSON: %v (raw: %s)", err, string(rawJSON))
	}
	if decoded != "0" {
		t.Errorf("decoded value = %q, want %q", decoded, "0")
	}
}

func TestRegisterProtoType_WithNonMatchingAny(t *testing.T) {
	clearRegistry()

	// Register a different type (StringValue instead of UInt64Value)
	RegisterProtoType(&wrapperspb.StringValue{Value: "ignored"})

	// Marshal an Any containing UInt64Value (doesn't match StringValue)
	anyBytes := mustMarshalAny(&wrapperspb.UInt64Value{Value: 42})

	// Wrap in a regular message
	outer := protowire.AppendTag(nil, 1, protowire.VarintType)
	outer = protowire.AppendVarint(outer, 1)
	outer = protowire.AppendTag(outer, 3, protowire.BytesType)
	outer = protowire.AppendBytes(outer, anyBytes)

	pm, err := WalkProtoMessage(outer)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	f := pm[1]
	if f.Type == "" {
		t.Error("Type should still be populated")
	}
	// Non-matching type: Value is ProtoMessage (children), not decoded JSON
	children, ok := f.Value.(ProtoMessage)
	if !ok {
		t.Fatalf("Value type = %T, want ProtoMessage (non-matching type)", f.Value)
	}
	if len(children) == 0 {
		t.Error("Children should still be present")
	}
}

func TestRegisterProtoType_ConcurrentSafety(t *testing.T) {
	clearRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RegisterProtoType(&wrapperspb.UInt64Value{Value: 0})
		}()
	}
	wg.Wait()

	// Verify it's registered
	_, ok := lookupRegisteredType("type.googleapis.com/google.protobuf.UInt64Value")
	if !ok {
		t.Fatal("type not found in registry after concurrent registration")
	}
}

// ---------------------------------------------------------------------------
// WalkProtoMessage  --  Multi-field smoke test
// ---------------------------------------------------------------------------

func TestWalkProtoMessage_MultipleFields(t *testing.T) {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, 10)
	b = protowire.AppendTag(b, 2, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, 0xABCD)
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("hello"))

	pm, err := WalkProtoMessage(b)
	if err != nil {
		t.Fatalf("WalkProtoMessage: %v", err)
	}
	if len(pm) != 3 {
		t.Fatalf("got %d fields, want 3", len(pm))
	}

	if pm[0].ID != 1 || pm[0].Value != float64(10) {
		t.Errorf("field 0: %+v", pm[0])
	}
	if pm[1].ID != 2 || pm[1].Value != float64(0xABCD) {
		t.Errorf("field 1: %+v", pm[1])
	}
	if pm[2].ID != 3 {
		t.Errorf("field 2: ID=%d, want 3", pm[2].ID)
	}
	if _, ok := pm[2].Value.(ProtoMessage); !ok {
		t.Errorf("field 2: Value type = %T, want ProtoMessage (printable bytes parsed as nested)", pm[2].Value)
	}
}
