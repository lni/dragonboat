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
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v3/tools/logdump/proto"
)

type IOConfig struct {
	Stdout io.Writer
	Stderr io.Writer
}

func NewIOConfig() *IOConfig {
	return &IOConfig{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

type DescribeConfig struct {
	Dir     string
	Cluster uint64
	Node    uint64

	IO *IOConfig
}

type ScanConfig struct {
	Dir     string
	Cluster uint64
	Node    uint64

	From  uint64
	To    uint64
	Index uint64
	Limit uint64

	Format string

	DecodeEntryHeader bool
	DecodeEntryCmd    bool

	IO         *IOConfig
	CmdDecoder proto.CmdDecoderFunc
}

func main() {
	if len(os.Args) < 2 {
		PrintUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	if len(os.Args) >= 3 {
		arg := os.Args[2]
		if arg == "-h" || arg == "--help" || arg == "help" {
			switch cmd {
			case "describe", "scan":
				PrintUsage()
				return
			}
		}
	}

	var err error
	switch cmd {
	case "describe":
		err = runDescribe(os.Args[2:])
	case "scan":
		err = runScan(os.Args[2:])
	case "-h", "--help", "help":
		PrintUsage()
	default:
		err = errors.Newf("unknown command: %s", cmd)
		PrintUsage()
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func runDescribe(args []string) error {
	if len(args) == 0 {
		return errors.New("error: data directory is required")
	}

	ioConfig := NewIOConfig()
	cfg, err := ParseDescribeFlags(ioConfig, args[0], args[1:])
	if err != nil {
		return err
	}

	return DescribeCmd(cfg)
}

func runScan(args []string) error {
	if len(args) == 0 {
		return errors.New("error: data directory is required")
	}

	ioConfig := NewIOConfig()
	cfg, err := ParseScanFlags(ioConfig, args[0], args[1:])
	if err != nil {
		return err
	}

	return ScanCmd(cfg)
}

func DescribeCmd(cfg *DescribeConfig) error {
	ld, err := OpenLogDumpReadOnly(cfg.Dir, true)
	if err != nil {
		return errors.Wrap(err, "error opening database")
	}

	s, err := ld.GetRaftDataStatus()
	if err != nil {
		return errors.Wrapf(err, "error reading %s", path.Join(cfg.Dir, flagFilename))
	}

	fmt.Fprintln(cfg.IO.Stdout, "MANIFEST:", ManifestString(&s))

	d := ld.Dumper()
	defer d.Close()

	summaries, err := d.Describe(cfg.Cluster, cfg.Node)
	if err != nil {
		return errors.Wrap(err, "error describing nodes")
	}

	PrintSummary(cfg.IO.Stdout, summaries)

	return nil
}

func ScanCmd(cfg *ScanConfig) error {
	ld, err := OpenLogDumpReadOnly(cfg.Dir, true)
	if err != nil {
		return errors.Wrap(err, "error opening database")
	}

	d := ld.Dumper()
	defer d.Close()

	result, err := d.Scan(cfg.Cluster, cfg.Node, cfg.From, cfg.To, cfg.Limit)
	if err != nil {
		return errors.Wrap(err, "error getting entries")
	}

	var opts []proto.DecodeOption
	if cfg.DecodeEntryHeader {
		opts = append(opts, proto.WithEntryHeaderDecode())
	}
	if cfg.DecodeEntryCmd {
		opts = append(opts, proto.WithEntryCmdDecode())
	}
	if cfg.CmdDecoder != nil {
		opts = append(opts, proto.WithCmdDecoder(cfg.CmdDecoder))
	}

	switch cfg.Format {
	case "json":
		PrintScanJSON(cfg.IO.Stdout, result, cfg.Cluster, cfg.Node, opts...)
	default:
		PrintScanTable(cfg.IO.Stdout, result, cfg.Cluster, cfg.Node, opts...)
	}

	return nil
}

func ParseDescribeFlags(io *IOConfig, dir string, flagArgs []string) (*DescribeConfig, error) {
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return nil, errors.Wrapf(err, "invalid data directory %q", dir)
	}

	fs := flag.NewFlagSet("describe", flag.ExitOnError)
	cluster := fs.Uint64("cluster", 0, "Filter by cluster ID (optional).")
	node := fs.Uint64("node", 0, "Filter by node ID (optional).")
	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}

	return &DescribeConfig{
		IO:      io,
		Dir:     dir,
		Cluster: *cluster,
		Node:    *node,
	}, nil
}

func ParseScanFlags(io *IOConfig, dir string, flagArgs []string) (*ScanConfig, error) {
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return nil, errors.Wrapf(err, "invalid data directory %q", dir)
	}

	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	cluster := fs.Uint64("cluster", 0, "Cluster ID (required).")
	node := fs.Uint64("node", 0, "Node ID (required).")
	from := fs.Uint64("from", 0, "Start index (inclusive, default: first available).")
	to := fs.Uint64("to", 0, "End index (exclusive, default: last+1).")
	index := fs.Uint64("index", 0, "Single entry shortcut (--from N --to N+1).")
	limit := fs.Uint64("limit", 100, "Max entries to display (default: 100, 0 = unlimited).")
	format := fs.String("format", "table", "Output format: table (default) or json.")
	decodeEntryHeader := fs.Bool("decode-entry-header", false, "Structurally decode Cmd payloads (protobuf, snappy).")
	decodeEntryCmd := fs.Bool("decode-entry-cmd", false, "Walk protobuf wire format in Cmd payloads (implies --decode-value).")

	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}

	if *decodeEntryCmd {
		*decodeEntryHeader = true
	}

	if *cluster == 0 {
		return nil, errors.New("--cluster is required and must be > 0")
	}
	if *node == 0 {
		return nil, errors.New("--node is required and must be > 0")
	}

	if *format != "table" && *format != "json" {
		return nil, errors.Errorf("--format must be \"table\" or \"json\", got %q", *format)
	}

	// --index is a shortcut for --from N --to N+1.
	if *index > 0 {
		*from = *index
		*to = *index + 1
	}

	return &ScanConfig{
		IO:                io,
		Dir:               dir,
		Cluster:           *cluster,
		Node:              *node,
		From:              *from,
		To:                *to,
		Index:             *index,
		Limit:             *limit,
		Format:            *format,
		DecodeEntryHeader: *decodeEntryHeader,
		DecodeEntryCmd:    *decodeEntryCmd,
	}, nil
}

func PrintUsage() {
	fmt.Print(`logdump — Dragonboat RAFT log inspection tool

USAGE:
  logdump describe <data-dir> [flags]
  logdump scan     <data-dir> --cluster N --node N [flags]

COMMANDS:
  describe  Show available clusters, nodes, and their RAFT state.
  scan      Pretty-print RAFT log entries with metadata.

LIST FLAGS:
  --cluster N   Filter by cluster ID (optional).
  --node N      Filter by node ID (optional).

GET FLAGS:
  --cluster N   Cluster ID (required).
  --node N      Node ID (required).

  --from N      Start index (inclusive, default: first available).
  --to N        End index (exclusive, default: last+1).
  --index N     Single entry shortcut (--from N --to N+1).
  --limit N     Max entries to display (default: 100, 0 = unlimited).

  --format fmt  Output format: table (default) or json.

  --decode-entry-header Structurally decode Cmd payloads (raftpb.Entry).
  --decode-entry-cmd Walk protobuf wire format in Cmd payloads (implies --decode-entry-header).

EXAMPLES:
  logdump describe /var/dragonboat/nodehost
  logdump describe /var/dragonboat/nodehost --cluster 1
  logdump scan  /var/dragonboat/nodehost --cluster 1 --node 2 --from 100 --to 200
  logdump scan  /var/dragonboat/nodehost --cluster 1 --node 2 --index 42
  logdump scan  /var/dragonboat/nodehost --cluster 1 --node 2 --format json
  logdump scan  /var/dragonboat/nodehost --cluster 1 --node 2 --index 42 --format json --decode-entry-header --decode-entry-cmd
`)
}
