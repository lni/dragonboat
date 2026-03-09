// Copyright 2024 Specula contributors.
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

package raft

import (
	"encoding/json"
	"os"
	"sync"
)

// NDJSONTraceLogger writes trace events as newline-delimited JSON to a file.
// It is safe for concurrent use from multiple goroutines.
type NDJSONTraceLogger struct {
	mu   sync.Mutex
	f    *os.File
	enc  *json.Encoder
}

// NewNDJSONTraceLogger creates a new NDJSON logger that appends to the given
// file path. The file is created if it does not exist.
func NewNDJSONTraceLogger(path string) (*NDJSONTraceLogger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &NDJSONTraceLogger{
		f:   f,
		enc: json.NewEncoder(f),
	}, nil
}

// Log writes one TraceEvent as a single JSON line.
func (l *NDJSONTraceLogger) Log(event TraceEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(event)
}

// Close flushes and closes the underlying file.
func (l *NDJSONTraceLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// globalTraceLogger is a process-wide trace logger, shared by all raft
// instances. Initialized once from the DRAGONBOAT_TRACE environment variable.
var (
	globalTraceLogger     TraceLogger
	globalTraceLoggerOnce sync.Once
)

// getGlobalTraceLogger returns the process-wide trace logger, creating it
// on first call if DRAGONBOAT_TRACE is set to a file path.
func getGlobalTraceLogger() TraceLogger {
	globalTraceLoggerOnce.Do(func() {
		path := os.Getenv("DRAGONBOAT_TRACE")
		if path == "" {
			return
		}
		l, err := NewNDJSONTraceLogger(path)
		if err != nil {
			plog.Warningf("failed to create trace logger at %s: %v", path, err)
			return
		}
		globalTraceLogger = l
		plog.Infof("trace logging enabled, writing to %s", path)
	})
	return globalTraceLogger
}
