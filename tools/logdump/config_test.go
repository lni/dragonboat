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
	"os"
	"testing"
)

func TestNewIOConfig(t *testing.T) {
	ioConfig := NewIOConfig()
	if ioConfig.Stdout != os.Stdout {
		t.Errorf("Expected Stdout to be os.Stdout, got %v", ioConfig.Stdout)
	}
	if ioConfig.Stderr != os.Stderr {
		t.Errorf("Expected Stderr to be os.Stderr, got %v", ioConfig.Stderr)
	}
}

func TestDescribeConfig(t *testing.T) {
	ioConfig := NewIOConfig()
	cfg := &DescribeConfig{
		IO:      ioConfig,
		Dir:     "/test",
		Cluster: 1,
		Node:    2,
	}
	if cfg.IO != ioConfig {
		t.Errorf("Expected IO to be set")
	}
	if cfg.Dir != "/test" {
		t.Errorf("Expected Dir to be /test, got %s", cfg.Dir)
	}
	if cfg.Cluster != 1 {
		t.Errorf("Expected Cluster to be 1, got %d", cfg.Cluster)
	}
	if cfg.Node != 2 {
		t.Errorf("Expected Node to be 2, got %d", cfg.Node)
	}
}

func TestScanConfig(t *testing.T) {
	ioConfig := NewIOConfig()
	cfg := &ScanConfig{
		IO:                ioConfig,
		Dir:               "/test",
		Cluster:           1,
		Node:              2,
		From:              10,
		To:                20,
		Format:            "json",
		DecodeEntryHeader: true,
		DecodeEntryCmd:    false,
	}
	if cfg.IO != ioConfig {
		t.Errorf("Expected IO to be set")
	}
	if cfg.Dir != "/test" {
		t.Errorf("Expected Dir to be /test, got %s", cfg.Dir)
	}
	if cfg.Format != "json" {
		t.Errorf("Expected Format to be json, got %s", cfg.Format)
	}
	if !cfg.DecodeEntryHeader {
		t.Errorf("Expected DecodeEntryHeader to be true")
	}
	if cfg.DecodeEntryCmd {
		t.Errorf("Expected DecodeEntryCmd to be false")
	}
}

func TestCustomIOConfig(t *testing.T) {
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	ioConfig := &IOConfig{
		Stdout: stdoutBuf,
		Stderr: stderrBuf,
	}

	if ioConfig.Stdout != stdoutBuf {
		t.Errorf("Expected custom Stdout")
	}
	if ioConfig.Stderr != stderrBuf {
		t.Errorf("Expected custom Stderr")
	}
}

func TestDescribeConfigWithCustomIO(t *testing.T) {
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	ioConfig := &IOConfig{
		Stdout: stdoutBuf,
		Stderr: stderrBuf,
	}

	cfg := &DescribeConfig{
		IO:  ioConfig,
		Dir: "/test",
	}

	if cfg.IO.Stdout != stdoutBuf {
		t.Errorf("Expected custom Stdout in config")
	}
	if cfg.IO.Stderr != stderrBuf {
		t.Errorf("Expected custom Stderr in config")
	}
}

func TestScanConfigWithCustomIO(t *testing.T) {
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	ioConfig := &IOConfig{
		Stdout: stdoutBuf,
		Stderr: stderrBuf,
	}

	cfg := &ScanConfig{
		IO:  ioConfig,
		Dir: "/test",
	}

	if cfg.IO.Stdout != stdoutBuf {
		t.Errorf("Expected custom Stdout in config")
	}
	if cfg.IO.Stderr != stderrBuf {
		t.Errorf("Expected custom Stderr in config")
	}
}

func TestParseDescribeFlagsInvalidDir(t *testing.T) {
	ioConfig := NewIOConfig()
	_, err := ParseDescribeFlags(ioConfig, "/nonexistent", []string{})
	if err == nil {
		t.Errorf("Expected error for invalid directory")
	}
}

func TestParseScanFlagsInvalidDir(t *testing.T) {
	ioConfig := NewIOConfig()
	_, err := ParseScanFlags(ioConfig, "/nonexistent", []string{})
	if err == nil {
		t.Errorf("Expected error for invalid directory")
	}
}

func TestParseSetsIOConfig(t *testing.T) {
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	ioConfig := &IOConfig{
		Stdout: stdoutBuf,
		Stderr: stderrBuf,
	}

	tmpDir, err := os.MkdirTemp("", "logdump-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	descCfg, err := ParseDescribeFlags(ioConfig, tmpDir, []string{})
	if err != nil {
		t.Fatalf("Failed to parse describe flags: %v", err)
	}

	if descCfg.IO != ioConfig {
		t.Errorf("Expected DescribeConfig.IO to be set to provided ioConfig")
	}

	scanCfg, err := ParseScanFlags(ioConfig, tmpDir, []string{"--cluster", "1", "--node", "1"})
	if err != nil {
		t.Fatalf("Failed to parse scan flags: %v", err)
	}

	if scanCfg.IO != ioConfig {
		t.Errorf("Expected ScanConfig.IO to be set to provided ioConfig")
	}
}
