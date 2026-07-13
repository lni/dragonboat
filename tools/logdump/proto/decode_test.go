package proto

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/golang/snappy"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

func TestDecodeCmd_SessionEntries(t *testing.T) {
	tests := []struct {
		name     string
		entry    pb.Entry
		wantKind CmdKind
		wantSum  string
	}{
		{
			name:     "new session",
			entry:    pb.Entry{Type: pb.ApplicationEntry, ClientID: 100, SeriesID: math.MaxUint64 - 1},
			wantKind: KindSession,
			wantSum:  "(new session)",
		},
		{
			name:     "end session",
			entry:    pb.Entry{Type: pb.ApplicationEntry, ClientID: 100, SeriesID: math.MaxUint64},
			wantKind: KindSession,
			wantSum:  "(end session)",
		},
		{
			name:     "no-op session",
			entry:    pb.Entry{Type: pb.ApplicationEntry, ClientID: 100, SeriesID: 0, Cmd: nil},
			wantKind: KindSession,
			wantSum:  "(no-op)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := DecodeCmd(tt.entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", d.Kind, tt.wantKind)
			}
			if d.Summary != tt.wantSum {
				t.Errorf("Summary = %q, want %q", d.Summary, tt.wantSum)
			}
		})
	}
}

func TestDecodeCmd_EmptyEntry(t *testing.T) {
	d, err := DecodeCmd(pb.Entry{Type: pb.MetadataEntry})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindEmpty {
		t.Errorf("Kind = %v, want KindEmpty", d.Kind)
	}
	if d.Summary != "(empty)" {
		t.Errorf("Summary = %q, want (empty)", d.Summary)
	}
}

func TestDecodeCmd_ConfigChange(t *testing.T) {
	cc := pb.ConfigChange{
		ConfigChangeId: 42,
		Type:           pb.AddNode,
		NodeID:         3,
		Address:        "node3:8080",
		Initialize:     true,
	}
	cmd, err := cc.Marshal()
	if err != nil {
		t.Fatalf("marshal ConfigChange: %v", err)
	}

	d, err := DecodeCmd(pb.Entry{Type: pb.ConfigChangeEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindConfigChange {
		t.Errorf("Kind = %v, want KindConfigChange", d.Kind)
	}
	if d.ConfigChange == nil {
		t.Fatal("ConfigChange is nil")
	}
	if d.ConfigChange.ConfigChangeID != 42 {
		t.Errorf("ConfigChangeID = %d, want 42", d.ConfigChange.ConfigChangeID)
	}
	if d.ConfigChange.Type != "AddNode" {
		t.Errorf("Type = %q, want AddNode", d.ConfigChange.Type)
	}
	if d.ConfigChange.NodeID != 3 {
		t.Errorf("NodeID = %d, want 3", d.ConfigChange.NodeID)
	}
	if d.ConfigChange.Address != "node3:8080" {
		t.Errorf("Address = %q, want node3:8080", d.ConfigChange.Address)
	}
	if !d.ConfigChange.Initialize {
		t.Error("Initialize should be true")
	}
}

func TestDecodeCmd_ConfigChangeTypes(t *testing.T) {
	types := []struct {
		ct   pb.ConfigChangeType
		name string
	}{
		{pb.AddNode, "AddNode"},
		{pb.RemoveNode, "RemoveNode"},
		{pb.AddObserver, "AddObserver"},
		{pb.AddWitness, "AddWitness"},
	}
	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			cc := pb.ConfigChange{Type: tt.ct, NodeID: 1}
			cmd, err := cc.Marshal()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			d, err := DecodeCmd(pb.Entry{Type: pb.ConfigChangeEntry, Cmd: cmd})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.ConfigChange.Type != tt.name {
				t.Errorf("Type = %q, want %q", d.ConfigChange.Type, tt.name)
			}
		})
	}
}

func TestDecodeCmd_EncodedEntry_NoCompression(t *testing.T) {
	payload := []byte("hello encoded")
	cmd := make([]byte, 1+len(payload))
	cmd[0] = 0 // V0, no compression, no session
	copy(cmd[1:], payload)

	d, err := DecodeCmd(pb.Entry{Type: pb.EncodedEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindEncodedEntry {
		t.Errorf("Kind = %v, want KindEncodedEntry", d.Kind)
	}
	if d.EncodedEntry.Compression != "none" {
		t.Errorf("Compression = %q, want none", d.EncodedEntry.Compression)
	}
	if d.EncodedEntry.Size != len(payload) {
		t.Errorf("Size = %d, want %d", d.EncodedEntry.Size, len(payload))
	}
	if d.EncodedEntry.PayloadText != string(payload) {
		t.Errorf("PayloadText = %q, want %q", d.EncodedEntry.PayloadText, string(payload))
	}
}

func TestDecodeCmd_EncodedEntry_Snappy(t *testing.T) {
	payload := []byte("compress me please compress me please")
	encoded := snappy.Encode(nil, payload)
	cmd := make([]byte, 1+len(encoded))
	cmd[0] = eeV0 | eeSnappy // V0, snappy compression
	copy(cmd[1:], encoded)

	d, err := DecodeCmd(pb.Entry{Type: pb.EncodedEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindEncodedEntry {
		t.Errorf("Kind = %v, want KindEncodedEntry", d.Kind)
	}
	if d.EncodedEntry.Compression != "snappy" {
		t.Errorf("Compression = %q, want snappy", d.EncodedEntry.Compression)
	}
	if d.EncodedEntry.Size != len(payload) {
		t.Errorf("Size = %d, want %d", d.EncodedEntry.Size, len(payload))
	}
	if d.EncodedEntry.PayloadText != string(payload) {
		t.Errorf("PayloadText = %q, want %q", d.EncodedEntry.PayloadText, string(payload))
	}
}

func TestDecodeCmd_Raw_Printable(t *testing.T) {
	cmd := []byte("hello world")
	d, err := DecodeCmd(pb.Entry{Type: pb.ApplicationEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindRaw {
		t.Errorf("Kind = %v, want KindRaw", d.Kind)
	}
	if d.Summary != "hello world" {
		t.Errorf("Summary = %q, want hello world", d.Summary)
	}
	if d.RawHex != hex.EncodeToString(cmd) {
		t.Errorf("RawHex mismatch")
	}
}

func TestDecodeCmd_Raw_Binary(t *testing.T) {
	cmd := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03}
	d, err := DecodeCmd(pb.Entry{Type: pb.ApplicationEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindRaw {
		t.Errorf("Kind = %v, want KindRaw", d.Kind)
	}
	if d.Summary != "deadbeef0001... (8 B)" {
		t.Errorf("Summary = %q, want deadbeef0001... (8 B)", d.Summary)
	}
}

func TestDecodeCmd_Raw_MetadataEntry(t *testing.T) {
	cmd := []byte{0x01, 0x02}
	d, err := DecodeCmd(pb.Entry{Type: pb.MetadataEntry, Cmd: cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Kind != KindRaw {
		t.Errorf("Kind = %v, want KindRaw", d.Kind)
	}
	if d.RawHex != "0102" {
		t.Errorf("RawHex = %q, want 0102", d.RawHex)
	}
}

func TestDecodeCmd_Errors(t *testing.T) {
	t.Run("malformed config change", func(t *testing.T) {
		_, err := DecodeCmd(pb.Entry{Type: pb.ConfigChangeEntry, Cmd: []byte{0xFF, 0xFF, 0xFF}})
		if err == nil {
			t.Error("expected error for malformed ConfigChange")
		}
	})

	t.Run("truncated encoded entry", func(t *testing.T) {
		_, err := DecodeCmd(pb.Entry{Type: pb.EncodedEntry, Cmd: nil})
		if err == nil {
			t.Error("expected error for nil Cmd on EncodedEntry")
		}
	})

	t.Run("bad snappy data", func(t *testing.T) {
		cmd := make([]byte, 5)
		cmd[0] = eeV0 | eeSnappy
		cmd[1] = 0xFF // not valid snappy
		_, err := DecodeCmd(pb.Entry{Type: pb.EncodedEntry, Cmd: cmd})
		if err == nil {
			t.Error("expected error for bad snappy data")
		}
	})
}

func TestDecodeCmd_MarshalJSON_RoundTrip(t *testing.T) {
	cc := pb.ConfigChange{Type: pb.RemoveNode, NodeID: 5}
	cmd, _ := cc.Marshal()
	d, _ := DecodeCmd(pb.Entry{Type: pb.ConfigChangeEntry, Cmd: cmd})

	b, err := d.Kind.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(b) != `"config_change"` {
		t.Errorf("Kind JSON = %s, want \"config_change\"", b)
	}
}

func TestDecodeValue_PlainEntry(t *testing.T) {
	e := pb.Entry{
		Term:  7,
		Index: 100,
		Type:  pb.ApplicationEntry,
		Cmd:   []byte("test"),
	}
	raw, err := e.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	entries, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if entries[0].Term != 7 || entries[0].Index != 100 {
		t.Errorf("Term/Index mismatch: %d/%d", entries[0].Term, entries[0].Index)
	}
	if string(entries[0].Cmd) != "test" {
		t.Errorf("Cmd = %q, want test", entries[0].Cmd)
	}
}

func TestDecodeValue_BatchEntry(t *testing.T) {
	eb := pb.EntryBatch{
		Entries: []pb.Entry{
			{Term: 5, Index: 10, Type: pb.ApplicationEntry, Cmd: []byte("a")},
			{Term: 5, Index: 11, Type: pb.ApplicationEntry, Cmd: []byte("b")},
			{Term: 5, Index: 12, Type: pb.ApplicationEntry, Cmd: []byte("c")},
		},
	}
	raw, err := eb.Marshal()
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	entries, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	for i, e := range entries {
		wantIdx := uint64(10 + i)
		if e.Index != wantIdx {
			t.Errorf("entry %d: Index = %d, want %d", i, e.Index, wantIdx)
		}
		if e.Term != 5 {
			t.Errorf("entry %d: Term = %d, want 5", i, e.Term)
		}
	}
}

func TestDecodeValue_BatchWithCompactedFields(t *testing.T) {
	eb := pb.EntryBatch{
		Entries: []pb.Entry{
			{Term: 5, Index: 10, Type: pb.ApplicationEntry, Cmd: []byte("a")},
			{Term: 0, Index: 0, Type: pb.ApplicationEntry, Cmd: []byte("b")},
			{Term: 0, Index: 0, Type: pb.ApplicationEntry, Cmd: []byte("c")},
		},
	}
	raw, err := eb.Marshal()
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	entries, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	for i, e := range entries {
		wantIdx := uint64(10 + i)
		if e.Index != wantIdx {
			t.Errorf("entry %d: Index = %d, want %d (fields should be restored)", i, e.Index, wantIdx)
		}
		if e.Term != 5 {
			t.Errorf("entry %d: Term = %d, want 5 (fields should be restored)", i, e.Term)
		}
	}
}

func TestDecodeValue_Error_Empty(t *testing.T) {
	_, err := DecodeValue(nil)
	if err == nil {
		t.Error("expected error for nil input")
	}
	_, err = DecodeValue([]byte{})
	if err == nil {
		t.Error("expected error for empty input")
	}
}
