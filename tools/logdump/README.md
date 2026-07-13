# logdump — Dragonboat RAFT Log Inspection Tool

A read-only CLI forensics tool for inspecting Dragonboat v3 LogDB (Pebble) data
on disk. Safely reads live data alongside a running NodeHost — no LOCK file
acquisition, no writes to the data directory.

## Build

```bash
go build -o logdump ./tools/logdump/
```

No CGO required. Pebble-only.

## Commands

### `list` — show available clusters and nodes

Scans the LogDB and prints a summary table with RAFT state, entry counts,
snapshot counts, and bootstrap info for every (cluster, node) pair.

```
logdump list <data-dir> [--cluster N] [--node N]
```

#### Example

```bash
$ logdump list /var/dragonboat/nodehost
```

```
NODES IN: /var/dragonboat/nodehost

CLUSTER  NODE  TERM  VOTE  COMMIT  ENTRIES  RANGE        SNAPSHOTS  BOOTSTRAP
1        1     7     1     1523    1523     [1..1523]    1          join=false, 3 addrs, sm=RegularStateMachine
1        2     7     1     1523    1520     [4..1523]    0          join=true, 3 addrs, sm=RegularStateMachine
2        1     3     1     89      89       [1..89]      1          join=false, 1 addrs, sm=OnDiskStateMachine
```

**Columns:**

| Column | Description |
|--------|-------------|
| `CLUSTER` | Cluster ID |
| `NODE` | Node ID within the cluster |
| `TERM` | Current RAFT term |
| `VOTE` | Node this node voted for in the current term |
| `COMMIT` | Commit index |
| `ENTRIES` | Number of log entries stored |
| `RANGE` | Entry index range `[first..last]` — a gap at the start (first > 1) means entries were compacted by snapshots |
| `SNAPSHOTS` | Count of stored snapshots |
| `BOOTSTRAP` | Bootstrap summary: whether the node joined, address count, and state machine type |

Filter with `--cluster` and/or `--node` to narrow the output:

```bash
$ logdump list /var/dragonboat/nodehost --cluster 1
$ logdump list /var/dragonboat/nodehost --cluster 1 --node 2
```

---

### `get` — pretty-print RAFT log entries

Fetches and displays the RAFT state, snapshots, and log entries for a specific
node in the requested index range.

```
logdump get <data-dir> --cluster N --node N [flags]
```

#### Table format (default)

```bash
$ logdump get /var/dragonboat/nodehost --cluster 1 --node 2 --from 100 --to 110
```

```
── state: cluster=1 node=2 ──
Term: 7, Vote: 1, Commit: 1523

── snapshots ──
INDEX  TERM  MEMBERS
1500   7     3 addrs

── entries ──
INDEX  TERM  TYPE               KEY  CLIENT  SERIES  RESPTO  CMD
100    7     ApplicationEntry   42   1001    5       99      a3f2c801a9b7... (16 B)
101    7     ConfigChangeEntry  0    0       0       0       node_id:3 address:"node3:63000"
102    7     ApplicationEntry   43   1001    6       99      b4e1d92f3c01... (28 B)
103    8     ApplicationEntry   44   1002    1       0       hello world
──     8     MetadataEntry      0    0       0       0       (empty)
──     8     EncodedEntry       0    0       0       0       <encoded, 64 B>

6 entries shown (limit 100, range [100, 110)), total entries in log: 1523
```

**Section layout:**

1. **`── state ──`** — current term, vote, and commit index
2. **`── snapshots ──`** — snapshots within the range (hidden when empty)
3. **`── entries ──`** — log entries table

**Entry table columns:**

| Column | Description |
|--------|-------------|
| `INDEX` | Log index |
| `TERM` | RAFT term when the entry was proposed |
| `TYPE` | Entry type: `ApplicationEntry`, `ConfigChangeEntry`, `MetadataEntry`, `EncodedEntry` |
| `KEY` | Session key for session-managed entries |
| `CLIENT` | Client ID |
| `SERIES` | Series ID (proposal sequence number) |
| `RESPTO` | RespondedTo (last index client has seen applied) |
| `CMD` | Payload — see format rules below |

**CMD formatting (auto-detected):**

| Condition | Display |
|-----------|---------|
| Config change with printable text | Decoded inline: `node_id:3 address:"node3:63000"` |
| New session request | `(new session)` |
| End of session request | `(end session)` |
| No-op session (empty payload) | `(no-op)` |
| Truly empty entry | `(empty)` |
| Short printable text (≤ 40 B) | Shown inline: `hello world` |
| Binary payload | Hex dump of first 6 bytes + total size: `a3f2c801a9b7... (16 B)` |

**Ranges** are half-open `[from, to)` — `from` is inclusive, `to` is exclusive.

**Internal entries** (`MetadataEntry`, `EncodedEntry`) are visually dimmed with
a `── ` prefix.

**Footer** reports how many entries were shown, the limit, the requested range,
and total entries in the log.

#### Single entry shortcut

Use `--index N` as a shortcut for `--from N --to N+1`:

```bash
$ logdump get /var/dragonboat/nodehost --cluster 1 --node 2 --index 42
```

#### JSON format

Use `--format json` for machine-readable output:

```bash
$ logdump get /var/dragonboat/nodehost --cluster 1 --node 2 --index 42 --format json
```

```json
{"cluster_id":1,"node_id":2,"state":{"Term":7,"Vote":1,"Commit":1523},"snapshots":[],"entries":[{"index":42,"term":7,"type":"ApplicationEntry","key":15,"client_id":1001,"series_id":3,"responded_to":41,"cmd":"48656c6c6f2c20576f726c6421"}],"from":42,"to":43,"limit":100,"first_index":4,"last_index":1523,"entry_count":1520}
```

## Flags

### `list` flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--cluster` | uint64 | `0` (all) | Filter by cluster ID |
| `--node` | uint64 | `0` (all) | Filter by node ID |

### `get` flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--cluster` | uint64 | `0` | **Required.** Cluster ID |
| `--node` | uint64 | `0` | **Required.** Node ID |
| `--from` | uint64 | first available | Start index (inclusive) |
| `--to` | uint64 | last+1 | End index (exclusive) |
| `--index` | uint64 | — | Single entry shortcut (`--from N --to N+1`) |
| `--limit` | uint64 | `100` | Max entries to display (`0` = unlimited) |
| `--format` | string | `table` | Output format: `table` or `json` |

## Safety

- **Read-only** — opens Pebble databases with `Options{ReadOnly: true}`, no LOCK
  file is acquired. Safe to run alongside a live NodeHost.
- **No writes** — all write methods panic. The tool cannot modify the data
  directory.
- **Panic recovery** — unexpected internal panics are caught at the top level
  with a stack trace printed to stderr (exit code 2).

## Limitations

- **Pebble only.** RocksDB-based LogDB stores are not supported.
- **Shard partitioner mismatch.** The read path uses `FixedPartitioner`
  (`clusterID % logDBShards`) while production writes may use
  `DoubleFixedPartitioner` (`(clusterID % execShards) % logDBShards`). This
  only affects deployments where `ExecShards < LogDBShards` (rare).
- **`--index 0` is silently ignored** — the zero value is indistinguishable from
  "not set". Use `--from 0 --to 1` if you need index 0.
