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

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/dragonboat/v3/tools/logdump/proto"
)

// isPrintable returns true when s contains only printable ASCII characters
// (bytes 0x20-0x7E inclusive).
func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

// formatCmd returns a human-friendly representation of an entry's Cmd field.
//
//   - ConfigChange entries with printable Cmd are shown inline.
//   - Session-management entries get descriptive labels.
//   - Short printable payloads are shown as text.
//   - Binary payloads are hex-dumped (first 6 bytes).
func formatCmd(e pb.Entry) string {
	// ConfigChange with printable Cmd — show as text.
	if e.IsConfigChange() && len(e.Cmd) > 0 && isPrintable(string(e.Cmd)) {
		return string(e.Cmd)
	}
	// Client session management.
	if e.IsNewSessionRequest() {
		return "(new session)"
	}
	if e.IsEndOfSessionRequest() {
		return "(end session)"
	}
	// NoOP session with empty payload.
	if e.IsNoOPSession() && len(e.Cmd) == 0 {
		return "(no-op)"
	}
	// Truly empty entry (not config change, not session managed).
	if e.IsEmpty() {
		return "(empty)"
	}
	// Short printable payload — show inline.
	if len(e.Cmd) <= 40 && isPrintable(string(e.Cmd)) {
		return string(e.Cmd)
	}
	// Binary payload — hex dump first 6 bytes.
	n := len(e.Cmd)
	if n > 6 {
		return fmt.Sprintf("%x... (%d B)", e.Cmd[:6], n)
	}
	return fmt.Sprintf("%x (%d B)", e.Cmd, n)
}

func ManifestString(m *pb.RaftDataStatus) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Address=%s, ", m.Address)
	fmt.Fprintf(&buf, "BinVer=%d, ", m.BinVer)
	fmt.Fprintf(&buf, "HardHash=%d, ", m.HardHash)
	fmt.Fprintf(&buf, "LogdbType=%s, ", m.LogdbType)
	fmt.Fprintf(&buf, "Hostname=%s, ", m.Hostname)
	fmt.Fprintf(&buf, "DeploymentId=%d, ", m.DeploymentId)
	fmt.Fprintf(&buf, "StepWorkerCount=%d, ", m.StepWorkerCount)
	fmt.Fprintf(&buf, "LogdbShardCount=%d, ", m.LogdbShardCount)
	fmt.Fprintf(&buf, "MaxSessionCount=%d, ", m.MaxSessionCount)
	fmt.Fprintf(&buf, "EntryBatchSize=%d, ", m.EntryBatchSize)
	fmt.Fprintf(&buf, "AddressByNodeHostId=%v", m.AddressByNodeHostId)
	return buf.String()
}

// PrintSummary prints a table of NodeSummary rows to stdout using aligned
// columns.
func PrintSummary(w io.Writer, summaries []NodeSummary) {
	tabw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tabw, "CLUSTER\tNODE\tTERM\tVOTE\tCOMMIT\tENTRIES\tRANGE\tSNAPSHOTS\tBOOTSTRAP")
	for _, s := range summaries {
		rng := ""
		if s.EntryCount > 0 {
			rng = fmt.Sprintf("[%d..%d]", s.FirstIndex, s.LastIndex)
		} else {
			rng = "(empty)"
		}
		fmt.Fprintf(tabw, "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%s\n",
			s.ClusterID, s.NodeID,
			s.Term, s.Vote, s.Commit,
			s.EntryCount, rng, s.SnapshotCnt, s.Bootstrap)
	}
	tabw.Flush()
}

// PrintScanTable prints the state, snapshots, and entries for a node in a
// human-readable kebab-section format.
func PrintScanTable(w io.Writer, r *ScanResult, clusterID, nodeID uint64, opts ...proto.DecodeOption) {
	// ── state ──
	fmt.Fprintf(w, "── state: cluster=%d node=%d ──\n", clusterID, nodeID)
	fmt.Fprintf(w, "Term: %d, Vote: %d, Commit: %d\n\n", r.State.Term, r.State.Vote, r.State.Commit)

	// ── snapshots ──
	if len(r.Snapshots) > 0 {
		fmt.Fprintf(w, "── snapshots ──\n")
		tabw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		fmt.Fprintln(tabw, "INDEX\tTERM\tMEMBERS")
		for _, snap := range r.Snapshots {
			n := len(snap.Membership.Addresses)
			fmt.Fprintf(tabw, "%d\t%d\t%d addrs\n", snap.Index, snap.Term, n)
		}
		tabw.Flush()
		fmt.Fprintln(w)
	}

	// ── entries ──
	fmt.Fprintf(w, "── entries ──\n")
	if len(r.Entries) == 0 {
		fmt.Fprintln(w, "(no entries)")
	} else {
		tabw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		fmt.Fprintln(tabw, "INDEX\tTERM\tTYPE\tKEY\tCLIENT\tSERIES\tRESPTO\tCMD")
		for _, e := range r.Entries {
			typeName := e.Type.String()
			// Visually dim entries from internal Raft layers.
			if e.Type == pb.MetadataEntry || e.Type == pb.EncodedEntry {
				typeName = "── " + typeName
			}
			cmdStr := formatCmd(e)
			if len(opts) > 0 {
				if d, err := proto.DecodeCmd(e, opts...); err != nil {
					cmdStr = fmt.Sprintf("<decode error: %v>", err)
				} else {
					cmdStr = d.Summary
				}
			}
			keyHex := fmt.Sprintf("%016x", e.Key)
			clientHex := fmt.Sprintf("%016x", e.ClientID)
			fmt.Fprintf(tabw, "%d\t%d\t%s\t0x%s\t0x%s\t%d\t%d\t%s\n",
				e.Index, e.Term, typeName,
				keyHex, clientHex, e.SeriesID, e.RespondedTo,
				cmdStr)
		}
		tabw.Flush()
	}
}

type jsonSnapshot struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Members int    `json:"members"`
}

type jsonEntry struct {
	Index       uint64      `json:"index"`
	Term        uint64      `json:"term"`
	Type        string      `json:"type"`
	Key         uint64      `json:"key"`
	ClientID    uint64      `json:"client_id"`
	SeriesID    uint64      `json:"series_id"`
	RespondedTo uint64      `json:"responded_to"`
	Cmd         interface{} `json:"cmd"`
}

type jsonResult struct {
	ClusterID  uint64         `json:"cluster_id"`
	NodeID     uint64         `json:"node_id"`
	State      pb.State       `json:"state"`
	Snapshots  []jsonSnapshot `json:"snapshots,omitempty"`
	Entries    []jsonEntry    `json:"entries,omitempty"`
	From       uint64         `json:"from"`
	To         uint64         `json:"to"`
	Limit      uint64         `json:"limit"`
	FirstIndex uint64         `json:"first_index"`
	LastIndex  uint64         `json:"last_index"`
	EntryCount uint64         `json:"entry_count"`
}

// PrintScanJSON prints the GetEntries result as compact JSON to stdout.
func PrintScanJSON(w io.Writer, r *ScanResult, clusterID, nodeID uint64, opts ...proto.DecodeOption) {
	out := jsonResult{
		ClusterID:  clusterID,
		NodeID:     nodeID,
		State:      r.State,
		From:       r.From,
		To:         r.To,
		Limit:      r.Limit,
		FirstIndex: r.FirstIndex,
		LastIndex:  r.LastIndex,
		EntryCount: r.EntryCount,
	}

	for _, snap := range r.Snapshots {
		out.Snapshots = append(out.Snapshots, jsonSnapshot{
			Index:   snap.Index,
			Term:    snap.Term,
			Members: len(snap.Membership.Addresses),
		})
	}

	for _, e := range r.Entries {
		var cmdField interface{}

		if len(opts) > 0 {
			d, err := proto.DecodeCmd(e, opts...)
			if err != nil {
				cmdField = map[string]string{"error": err.Error()}
			} else {
				cmdField = d
			}
		} else {
			cmdField = hex.EncodeToString(e.Cmd)
		}

		out.Entries = append(out.Entries, jsonEntry{
			Index:       e.Index,
			Term:        e.Term,
			Type:        e.Type.String(),
			Key:         e.Key,
			ClientID:    e.ClientID,
			SeriesID:    e.SeriesID,
			RespondedTo: e.RespondedTo,
			Cmd:         cmdField,
		})
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "")
	enc.Encode(out)
}
