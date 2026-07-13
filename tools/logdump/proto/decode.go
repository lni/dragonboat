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
	"encoding/hex"
	"fmt"

	"github.com/golang/snappy"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

// CmdKind classifies a decoded Entry.Cmd payload.
type CmdKind int

const (
	KindEmpty        CmdKind = iota // Cmd is nil or zero-length
	KindSession                     // Session-managed (new/end/no-op), identified by SeriesID
	KindConfigChange                // pb.ConfigChange unmarshaled successfully
	KindEncodedEntry                // EncodedEntry decompressed
	KindRaw                         // Raw application bytes (fallback)
)

func (k CmdKind) String() string {
	switch k {
	case KindEmpty:
		return "empty"
	case KindSession:
		return "session"
	case KindConfigChange:
		return "config_change"
	case KindEncodedEntry:
		return "encoded_entry"
	case KindRaw:
		return "raw"
	default:
		return "unknown"
	}
}

// MarshalJSON implements json.Marshaler for human-readable kind strings.
func (k CmdKind) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", k.String())), nil
}

// DecodedCmd holds the structured result of decoding an Entry's Cmd field.
// Exactly one detail field is populated depending on Kind.
type DecodedCmd struct {
	Kind         CmdKind             `json:"kind"`
	Summary      string              `json:"-"` // one-line for table CMD column
	ConfigChange *ConfigChangeDetail `json:"config_change,omitempty"`
	EncodedEntry *EncodedEntryDetail `json:"encoded_entry,omitempty"`
	RawHex       string              `json:"raw_hex,omitempty"`
	ProtoMessage ProtoMessage        `json:"proto_message,omitempty"`
}

type decodeConfig struct {
	decodeEntryCmd bool

	cmdDecoder CmdDecoderFunc
}

type CmdDecoderFunc func([]byte) (msg ProtoMessage, sum string, err error)

// DecodeOption configures DecodeCmd behavior.
type DecodeOption func(*decodeConfig)

// WithEntryHeaderDecode enables decoding of entry headers.
func WithEntryHeaderDecode() DecodeOption {
	return func(c *decodeConfig) {
		// stub
	}
}

// WithEntryCmdDecode enables protobuf payload walking on EncodedEntry
// and raw Cmd fields via WalkProtoMessage.
func WithEntryCmdDecode() DecodeOption {
	return func(c *decodeConfig) {
		c.decodeEntryCmd = true
	}
}

// WithCmdDecoder sets the function to decode a cmd field.
func WithCmdDecoder(f CmdDecoderFunc) DecodeOption {
	return func(c *decodeConfig) {
		c.cmdDecoder = f
	}
}

// ConfigChangeDetail holds the decoded fields of a pb.ConfigChange.
type ConfigChangeDetail struct {
	ConfigChangeID uint64 `json:"config_change_id"`
	Type           string `json:"type"`
	NodeID         uint64 `json:"node_id"`
	Address        string `json:"address,omitempty"`
	Initialize     bool   `json:"initialize"`
}

// EncodedEntryDetail holds the decoded fields of an EncodedEntry payload.
type EncodedEntryDetail struct {
	Version      uint8        `json:"version"`
	Compression  string       `json:"compression"`
	Size         int          `json:"size"`
	PayloadHex   string       `json:"payload_hex,omitempty"`
	PayloadText  string       `json:"payload_text,omitempty"`
	ProtoMessage ProtoMessage `json:"proto_message,omitempty"`
}

func configChangeTypeName(t pb.ConfigChangeType) string {
	switch t {
	case pb.AddNode:
		return "AddNode"
	case pb.RemoveNode:
		return "RemoveNode"
	case pb.AddObserver:
		return "AddObserver"
	case pb.AddWitness:
		return "AddWitness"
	default:
		return fmt.Sprintf("ConfigChangeType(%d)", t)
	}
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

// Encoded entry header constants (mirrors internal/rsm/encoded.go).
const (
	eeV0            uint8 = 0 << 4
	eeVersionMask   uint8 = 15 << 4
	eeCompMask      uint8 = 7 << 1
	eeNoCompression uint8 = 0 << 1
	eeSnappy        uint8 = 1 << 1
)

// DecodeCmd interprets a single Entry's Cmd field based on Entry.Type
// and Entry.SeriesID. Returns nil error even for KindRaw; errors are
// reserved for actual decode failures (malformed protobuf, bad snappy).
//
// Use WithEntryProto() to enable protobuf payload walking.
func DecodeCmd(e pb.Entry, opts ...DecodeOption) (*DecodedCmd, error) {
	cfg := &decodeConfig{}
	for _, o := range opts {
		o(cfg)
	}

	// 1. Session gating (runs first — session entries can have arbitrary Cmd).
	if e.IsNewSessionRequest() {
		return &DecodedCmd{Kind: KindSession, Summary: "(new session)"}, nil
	}
	if e.IsEndOfSessionRequest() {
		return &DecodedCmd{Kind: KindSession, Summary: "(end session)"}, nil
	}
	if e.IsSessionManaged() && e.IsNoOPSession() && len(e.Cmd) == 0 {
		return &DecodedCmd{Kind: KindSession, Summary: "(no-op)"}, nil
	}

	// 2. Type-specific decode before empty check —
	// EncodedEntry/ConfigChangeEntry with nil/empty Cmd is an error, not KindEmpty.
	switch e.Type {
	case pb.ConfigChangeEntry:
		return decodeConfigChange(e.Cmd)
	case pb.EncodedEntry:
		return decodeEncodedEntry(e.Cmd, cfg)
	}

	// 3. Empty.
	if e.IsEmpty() {
		return &DecodedCmd{Kind: KindEmpty, Summary: "(empty)"}, nil
	}
	if len(e.Cmd) == 0 {
		return &DecodedCmd{Kind: KindEmpty, Summary: "(empty)"}, nil
	}

	// 4. Raw fallback (ApplicationEntry, MetadataEntry, etc.).
	return decodeRaw(e.Cmd, cfg), nil
}

func decodeConfigChange(cmd []byte) (*DecodedCmd, error) {
	var cc pb.ConfigChange
	if err := cc.Unmarshal(cmd); err != nil {
		return nil, fmt.Errorf("config change unmarshal: %w", err)
	}

	detail := &ConfigChangeDetail{
		ConfigChangeID: cc.ConfigChangeId,
		Type:           configChangeTypeName(cc.Type),
		NodeID:         cc.NodeID,
		Address:        cc.Address,
		Initialize:     cc.Initialize,
	}

	summary := fmt.Sprintf("ConfigChange: %s node=%d", detail.Type, detail.NodeID)
	if detail.Address != "" {
		summary += fmt.Sprintf(" addr=%s", detail.Address)
	}

	return &DecodedCmd{
		Kind:         KindConfigChange,
		Summary:      summary,
		ConfigChange: detail,
	}, nil
}

func decodeEncodedEntry(cmd []byte, cfg *decodeConfig) (*DecodedCmd, error) {
	if len(cmd) < 1 {
		return nil, fmt.Errorf("encoded entry too short: %d bytes", len(cmd))
	}

	header := cmd[0]
	ver := header & eeVersionMask
	comp := header & eeCompMask

	if ver != eeV0 {
		// Unknown version — fall back to raw with note.
		d := decodeRaw(cmd, cfg)
		d.Summary = fmt.Sprintf("[v%d unknown] %s", ver>>4, d.Summary)
		return d, nil
	}

	detail := &EncodedEntryDetail{Version: 0}

	switch comp {
	case eeNoCompression:
		var summary string
		detail.Compression = "none"
		payload := cmd[1:]
		detail.Size = len(payload)
		if cfg.decodeEntryCmd {
			var err error
			if cfg.cmdDecoder != nil {
				detail.ProtoMessage, summary, err = cfg.cmdDecoder(payload)
			} else {
				detail.ProtoMessage, err = WalkProtoMessage(payload)
			}
			if err != nil {
				return nil, err
			}
		}
		if detail.ProtoMessage == nil {
			detail.PayloadHex = hex.EncodeToString(payload)
			if isPrintable(string(payload)) {
				detail.PayloadText = string(payload)
			}
		}
		if summary == "" {
			summary = fmt.Sprintf("[v0 uncompressed] %d B", detail.Size)
		}
		return &DecodedCmd{
			Kind:         KindEncodedEntry,
			Summary:      summary,
			EncodedEntry: detail,
		}, nil

	case eeSnappy:
		var summary string
		detail.Compression = "snappy"
		decodedLen, err := snappy.DecodedLen(cmd[1:])
		if err != nil {
			return nil, fmt.Errorf("snappy decoded length: %w", err)
		}
		dst := make([]byte, decodedLen)
		payload, err := snappy.Decode(dst, cmd[1:])
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		detail.Size = len(payload)
		if cfg.decodeEntryCmd {
			var err error
			if cfg.cmdDecoder != nil {
				detail.ProtoMessage, summary, err = cfg.cmdDecoder(payload)
			} else {
				detail.ProtoMessage, err = WalkProtoMessage(payload)
			}
			if err != nil {
				return nil, err
			}
		}
		if detail.ProtoMessage == nil {
			detail.PayloadHex = hex.EncodeToString(payload)
			if isPrintable(string(payload)) {
				detail.PayloadText = string(payload)
			}
		}
		if summary == "" {
			summary = fmt.Sprintf("[v0 snappy] %d B", detail.Size)
		}
		return &DecodedCmd{
			Kind:         KindEncodedEntry,
			Summary:      summary,
			EncodedEntry: detail,
		}, nil

	default:
		d := decodeRaw(cmd, cfg)
		d.Summary = fmt.Sprintf("[compression=%d unknown] %s", comp>>1, d.Summary)
		return d, nil
	}
}

func decodeRaw(cmd []byte, cfg *decodeConfig) *DecodedCmd {
	summary := formatRaw(cmd)
	d := &DecodedCmd{
		Kind:    KindRaw,
		Summary: summary,
		RawHex:  hex.EncodeToString(cmd),
	}
	if cfg.decodeEntryCmd && len(cmd) > 0 {
		if pm, err := WalkProtoMessage(cmd); err == nil {
			d.ProtoMessage = pm
		}
	}
	return d
}

// formatRaw mirrors the existing formatCmd heuristic for raw bytes.
func formatRaw(cmd []byte) string {
	if len(cmd) <= 40 && isPrintable(string(cmd)) {
		return string(cmd)
	}
	n := len(cmd)
	if n > 6 {
		return fmt.Sprintf("%x... (%d B)", cmd[:6], n)
	}
	return fmt.Sprintf("%x (%d B)", cmd, n)
}

// DecodeValue parses raw LogDB value bytes into entries.
// Auto-detects plain (Colfer) vs batched (protobuf framing + Colfer entries).
func DecodeValue(raw []byte) ([]pb.Entry, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty value")
	}

	// Detection: first byte 0x0a = protobuf field 1, wire type 2 (EntryBatch).
	// Colfer field indices for Entry are 0-7; 0x0a is never a valid index byte.
	if raw[0] == 0x0a {
		return decodeBatch(raw)
	}
	return decodePlain(raw)
}

func decodePlain(raw []byte) ([]pb.Entry, error) {
	var e pb.Entry
	if err := e.Unmarshal(raw); err != nil {
		return nil, fmt.Errorf("plain entry unmarshal: %w", err)
	}
	return []pb.Entry{e}, nil
}

func decodeBatch(raw []byte) ([]pb.Entry, error) {
	var eb pb.EntryBatch
	if err := eb.Unmarshal(raw); err != nil {
		return nil, fmt.Errorf("batch unmarshal: %w", err)
	}
	restoreBatchFields(&eb)
	return eb.Entries, nil
}

// restoreBatchFields reconstructs zeroed Term/Index for entries[1:]
// when the batch used field compaction (detected via last entry Term==0).
func restoreBatchFields(eb *pb.EntryBatch) {
	if len(eb.Entries) <= 1 {
		return
	}
	if eb.Entries[len(eb.Entries)-1].Term == 0 {
		term := eb.Entries[0].Term
		idx := eb.Entries[0].Index
		for i := 1; i < len(eb.Entries); i++ {
			eb.Entries[i].Term = term
			eb.Entries[i].Index = idx + uint64(i)
		}
	}
}
