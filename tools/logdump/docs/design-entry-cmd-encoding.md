# Design: Entry.Cmd Encoding in Dragonboat LogDB

## Context

Dragonboat stores RAFT log entries in LogDB (Pebble/RocksDB). The `Entry.Cmd`
field holds the actual payload proposed by clients and must be efficiently
encoded for persistence and decoded for retrieval. This document describes the
exact encoding mechanism to enable tooling (e.g., `logdump`) to correctly
interpret entry payloads.

## Key Data Structures

### Entry Structure

The `pb.Entry` protobuf struct (defined in `raftpb/raft.proto`) contains:

```
message Entry {
    optional uint64 Term        = 1;
    optional uint64 Index       = 2;
    optional EntryType Type     = 3;
    optional uint64 Key         = 4;
    optional uint64 ClientID    = 5;
    optional uint64 SeriesID    = 6;
    optional uint64 RespondedTo = 7;
    optional bytes  Cmd         = 8;  // ← Payload field
}
```

**Entry types** (`EntryType` enum):
- `ApplicationEntry` (0): User proposal data or session-managed operations
- `ConfigChangeEntry` (1): RAFT membership changes
- `EncodedEntry` (2): Pre-encoded/compressed entry payloads
- `MetadataEntry` (3): Internal RAFT metadata

### Cmd Payload Types

`Entry.Cmd` is a raw `[]byte` field interpreted based on `Entry.Type`:

| Entry.Type | Cmd Content | Decoding |
|------------|-------------|----------|
| `ApplicationEntry` | User proposal data | Raw bytes (application-defined) |
| `ConfigChangeEntry` | `pb.ConfigChange` protobuf | `ConfigChange.Unmarshal(Cmd)` |
| `EncodedEntry` | Version + compression + payload | 1-byte header + compressed bytes |
| `MetadataEntry` | Metadata payload | Raw bytes |

## Encoding Mechanism

### Write Path: Persisting Entry.Cmd

**Location**: `internal/logdb/plain.go` and `internal/logdb/batch.go`

Two encoding modes exist:

#### 1. Plain Encoding (Single Entry)

**Method**: `ent.MarshalTo(buf []byte)` in `plain.go` (Colfer binary codec, generated in `raftpb/raft_optimized.go`)

Process:
1. Allocate buffer with `SizeUpperLimit()` to determine needed capacity
2. Call `MarshalTo()` which performs Colfer binary serialization
3. Write `Entry` struct fields including `Cmd` as sequential index byte + varint length + raw bytes
4. Store at LogDB key: 28-byte key header + `clusterID` + `nodeID` + `index`

**Key structure** (`internal/logdb/key.go`):

```
[header: 2B][padding: 2B][clusterID: 8B][nodeID: 8B][index: 8B]
```
Header bytes `[0x1, 0x1]` mark entry keys. Total: 28 bytes (`entryKeySize` constant).

#### 2. Batched Encoding (Multiple Entries)

**Method**: `EntryBatch.MarshalTo(buf []byte)` in `raftpb/raft.pb.go` (protobuf wrapper), called from `batch.go`

Process:
1. Group consecutive entries for compaction
2. Remove redundant `Term` and `Index` values (store first entry's values once)
3. `EntryBatch.MarshalTo()` serializes the `repeated Entry` array using **protobuf framing**:
   - Each entry is wrapped in a protobuf length-delimited field: tag `0x0a` (field 1, wire type 2) + varint length
   - Inside each frame, `Entry.MarshalTo()` encodes the entry using **Colfer** (colfer index bytes + varints + terminator `0x7f`)
   (the first entry carries Term/Index; subsequent entries have these zeroed
   by `compactBatchFields` before marshal, then reconstructed by
   `restoreBatchFields` on Unmarshal)
4. Store at LogDB key with batch flag set

**Optimization**: Batch encoding compacted entry array by:
- Sharing single `Term` and `Index` for consecutive entries with same values
- Storing `Cmd` bytes in varint length + payload format (uncompressed)

### Cmd Field Encoding Details

**Colfer encoding** for `Entry.Cmd` field (colfer index 7, the 8th field in sequence):

1. Index byte: `0x07`
2. Varint length of `Cmd` bytes
3. Raw Cmd bytes (as-is, no further transformation)
4. End-of-struct terminator: `0x7f` (marks end of the Entry record)

Example: `Cmd = []byte{0x01, 0x02, 0x03}` within an Entry:
```
[...][0x07][0x03][0x01][0x02][0x03][0x7f]
       idx   len   payload             term
```

Note: Colfer encodes fields sequentially (0–7), not by protobuf field number.
The Cmd field is index 7, NOT protobuf wire tag `0x42`. The generated code
lives in `raftpb/raft_optimized.go`; `raftpb/raft.proto` defines the protobuf
schema used for gRPC/API boundaries, but LogDB persistence uses the Colfer
codec.

**Special payload encodings**:

#### ConfigChangeEncoding

When `Entry.Type == ConfigChangeEntry`:
- `Cmd` contains `pb.ConfigChange` marshaled bytes
- `ConfigChange` structure:
  ```
  message ConfigChange {
    uint64            config_change_id = 1;
    ConfigChangeType  Type             = 2;
    uint64            NodeID           = 3;
    string            Address          = 4;
    bool              Initialize       = 5;
  }
  ```
- `ConfigChangeType` values: `AddNode`, `RemoveNode`, `AddObserver`, `AddWitness`

#### EncodedEntry Encoding (Version 0)

When `Entry.Type == EncodedEntry`:
- `Cmd` format: `[header: 1B][payload: N bytes]`
  - **No compression**: `payload` is raw data
  - **Snappy compression**: `payload` is a snappy stream (starts with
    uvarint-encoded uncompressed size, followed by snappy-compressed data)
- Header bits:
  ```
  [Version: 4 bits][Compression: 3 bits][Session: 1 bit]
    Bits 7-4           Bits 3-1            Bit 0
  ```
- Version 0 (bits 7-4 = 0) is current
- Compression: 0=none (`EENoCompression = 0 << 1`), 1=snappy (`EESnappy = 1 << 1`)
- Session bit: reserved for future session info (always 0 in Version 0)

**Note**: The header constants are:
- `EEHeaderSize = 1`: header byte size
- `EEV0 = 0 << 4`: version 0
- `EENoCompression = 0 << 1`: no compression flag
- `EESnappy = 1 << 1`: snappy compression flag
- `EENoSession = 0`: no session flag
- `EEHasSession = 1`: has session flag

## Decoding Mechanism

### Read Path: Loading Entry.Cmd

**Location**: `internal/logdb/db.go` → `iterateEntries()` → `r.entries.iterate()`

Two decoding modes:

#### 1. Plain Decoding

**Method**: `Entry.Unmarshal(data []byte)` in `raftpb/raft_optimized.go` (Colfer codec)

Process:
1. Read LogDB value bytes at entry key
2. Call `Entry.Unmarshal()` which:
   - Reads colfer index bytes to identify fields
   - For index 7 (Cmd): read varint length, then bytes
   - Populates `Entry.Cmd` with raw byte slice
3. Return decoded `Entry` struct

#### 2. Batched Decoding

**Method**: `EntryBatch.Unmarshal(data []byte)` in `raftpb/raft.pb.go` → `restoreBatchFields()` in `batch.go`

Process:
1. Read LogDB value bytes at batch key
2. `EntryBatch.Unmarshal()` parses the protobuf framing:
   - Reads field tags and varint lengths to extract each entry frame
   - For each frame, calls `Entry.Unmarshal()` (colfer) to decode the entry
3. `restoreBatchFields()` post-processes the decoded entries:
   - If entries[1:] have zeroed Term/Index, fills them from entries[0] values
   - `Cmd` field: already decoded during step 2's colfer unmarshal
4. Return array of decoded `Entry` structs

### Cmd Field Decoding Details

**Colfer decoding** for the Cmd field:
1. Read index byte `0x07`, identifying the Cmd field
2. Read varint length `L`
3. Read next `L` bytes into `Entry.Cmd`

**Special payload decoding**:

- **ConfigChange**: When `Entry.Type == ConfigChangeEntry`, call `ConfigChange.Unmarshal(Entry.Cmd)` to parse membership change details

- **EncodedEntry**: When `Entry.Type == EncodedEntry`:
  1. Parse header byte: extract version (bits 7-4), compression (bits 3-1), session (bit 0)
  2. If compression == none: `payload = Cmd[1:]` (skip header)
  3. If compression == snappy:
     a. `Cmd[1:]` is a snappy stream (starts with a uvarint-encoded uncompressed size)
     b. Read uncompressed size via `binary.Uvarint(Cmd[1:])` to size the output buffer
     c. Call `dio.DecompressSnappyBlock(Cmd[1:], dst)` — returns `error`;
        `dst` must be pre-allocated to exactly the uncompressed size

- **ApplicationEntry**: Treat `Entry.Cmd` as opaque user bytes (no standard interpretation)


## Session Management Encoding

**Critical**: Session-related operations are **NOT** encoded in `Entry.Cmd` payload. They are identified via `Entry.SeriesID` values:

| SeriesID Value | Operation | Entry.Type | Cmd Content |
|---------------|-----------|------------|-------------|
| `math.MaxUint64 - 1` | New session request | `ApplicationEntry` | `nil` |
| `math.MaxUint64` | End session request | `ApplicationEntry` | `nil` |
| `0` | NoOP session operation | `ApplicationEntry` | `nil` |
| `Other` | Session-managed user proposal | `ApplicationEntry` | User data |

**Helper methods** in `raftpb/raft.go`:
- `IsNewSessionRequest()`: checks `SeriesID == client.SeriesIDForRegister`
- `IsEndOfSessionRequest()`: checks `SeriesID == client.SeriesIDForUnregister`
- `IsNoOPSession()`: checks `SeriesID == client.NoOPSeriesID`

**Non-session entries**: `Entry.ClientID == NotSessionManagedClientID` (0).
These bypass all session logic regardless of `SeriesID` value.

## Implementation File Reference

| File | Purpose |
|------|---------|
| `raftpb/raft.proto` | Entry protobuf definition with Cmd field |
| `internal/logdb/plain.go` | Plain encode/decode for single entries |
| `internal/logdb/batch.go` | Batched encode/decode with compaction |
| `internal/logdb/key.go` | LogDB key structure constants |
| `internal/logdb/db.go` | `saveEntries()` and `iterateEntries()` orchestration |
| `raftpb/raft.go` | Session management helpers (SeriesID checks) |
| `internal/rsm/encoded.go` | **EncodedEntry compression/decompression implementation** |
| `raftpb/raft_optimized.go` | Colfer binary codec for Entry |

## Assumptions & Contingencies

- **Wire format**: LogDB persistence uses the Colfer binary codec
  (`raftpb/raft_optimized.go`), not protobuf. The protobuf schema in
  `raft.proto` defines the data model and is used for gRPC transport, but
  `Entry.MarshalTo()` / `Entry.Unmarshal()` in the LogDB path dispatch to the
  Colfer-generated code.
- **Encoding stability**: Colfer field indices and header format are stable
  across dragonboat v3.x versions; breaks may occur with major version bumps.
- **Contingency**: If entry decoding fails, verify the colfer field ordering in
  `raft_optimized.go:marshalTo()` against the expected indices — field
  reordering would break the format.
