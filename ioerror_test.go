// +build dragonboat_errorinjectiontest

// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package dragonboat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/v3/internal/tests/charybdefs/server"
	"github.com/lni/dragonboat/v3/internal/tests/kvpb"
	"github.com/lni/dragonboat/v3/internal/utils/random"
)

const (
	injectionTestWorkingDir = "multiraft_error_injection_test_safe_to_delete"
)

//
// tests here are used to see whether dragonboat will crash as expected when
// there are disk io related errors, run out of space, disk died etc. tests
// here are expected to fail with specific error messages. they are launched
// by a simple python script which will check the error messages.
//

// environmental variable EITHOME defines where is the mounted charybdefs fs
// EITDATAHOME points to the actual directory for storing the data
// e.g.
// ./charybdefs /home/lni/fifs -omodules=subdir,subdir=/media/ramdisk
//
// EITHOME is /home/lni/fifs
// EITDATAHOME is /media/ramdisk
func getInjectionTestDirs() (string, string, error) {
	home := os.Getenv("EITHOME")
	datahome := os.Getenv("EITDATAHOME")
	if len(home) == 0 {
		return "", "", errors.New("EITHOME not set")
	}
	if len(datahome) == 0 {
		return "", "", errors.New("EITDATAHOME not set")
	}
	return home, datahome, nil
}

func removeInjectionTestDirs() {
	home, _, err := getInjectionTestDirs()
	if err != nil {
		panic(err)
	}
	top := filepath.Join(home, injectionTestWorkingDir)
	os.RemoveAll(top)
}

func prepareInjectionTestDirs() []string {
	removeInjectionTestDirs()
	dl := getMultiraftMonkeyTestAddrList()
	home, _, err := getInjectionTestDirs()
	if err != nil {
		panic(err)
	}
	top := filepath.Join(home, injectionTestWorkingDir)
	dirList := make([]string, 0)
	for i := uint64(1); i <= dl.Size(); i++ {
		nn := fmt.Sprintf("nodehost-%d", i)
		nd := filepath.Join(top, nn)
		if err := os.MkdirAll(nd, 0755); err != nil {
			panic(err)
		}
		dirList = append(dirList, nd)
	}

	return dirList
}

func createInjectionTestNodeHostList() []*mtNodeHost {
	dirList := prepareInjectionTestDirs()
	dl := getMultiraftMonkeyTestAddrList()
	result := make([]*mtNodeHost, dl.Size())
	for i := uint64(0); i < dl.Size(); i++ {
		result[i] = &mtNodeHost{
			listIndex: i,
			stopped:   true,
			dir:       dirList[i],
			addresses: dl.Addresses(),
		}
	}

	return result
}

// The probability argument is the probability over 100,000, e.g. 1% should
// set probability to 1000
func setFault(t *testing.T, methods []string, rand bool, errno syscall.Errno,
	probability int32, regexp string) {
	client, conn, err := server.Connect()
	if err != nil {
		t.Fatalf("failed to connect to RPC server %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = client.SetFault(ctx,
		methods, rand, int32(errno), probability, regexp, false, 0, false)
	if err != nil {
		t.Fatalf("failed to set random fault %v", err)
	}
}

func clearAllIOFaults(t *testing.T) {
	client, conn, err := server.Connect()
	if err != nil {
		t.Fatalf("failed to connect to RPC server %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = client.ClearAllFaults(ctx)
	if err != nil {
		t.Fatalf("failed to clear all faults %v", err)
	}
}

func TestCanConnectToCharybdefs(t *testing.T) {
	clearAllIOFaults(t)
}

func testFailedIOAreReported(t *testing.T,
	methods []string, rand bool, errno syscall.Errno,
	probability int32, regex string) {
	clearAllIOFaults(t)
	defer removeInjectionTestDirs()
	nhList := createInjectionTestNodeHostList()
	setFault(t, methods, rand, errno, probability, regex)
	startAllClusters(nhList)
	defer stopMTNodeHosts(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	defer clearAllIOFaults(t)
	key := "test-key"
	val := random.RandomString(32)
	kv := &kvpb.PBKV{
		Key: &key,
		Val: &val,
	}
	rec, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 200; i++ {
		testProposalCanBeMade(t, nhList[1], rec)
	}
}

func testFailedRDBWritesAreReported(t *testing.T, errno syscall.Errno) {
	testFailedIOAreReported(t,
		[]string{"fsync", "write"}, true, errno, 5000, ".*\\.log")
}

func testFailedSnapshotIOIsReported(t *testing.T,
	methods []string, errno syscall.Errno, regex string) {
	clearAllIOFaults(t)
	defer removeInjectionTestDirs()
	nhList := createInjectionTestNodeHostList()
	startAllClusters(nhList)
	defer stopMTNodeHosts(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	defer clearAllIOFaults(t)
	key := "test-key"
	val := random.RandomString(32)
	kv := &kvpb.PBKV{
		Key: &key,
		Val: &val,
	}
	rec, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 30; i++ {
		testProposalCanBeMade(t, nhList[1], rec)
	}
	nhList[0].Stop()
	nhList[1].Stop()
	for i := 0; i < 50; i++ {
		testProposalCanBeMade(t, nhList[2], rec)
	}
	time.Sleep(5 * time.Second)
	setFault(t, methods, false, errno, 0, regex)
	nhList[0].Start()
	nhList[1].Start()
	waitForStableClusters(t, nhList, 30)
	time.Sleep(10 * time.Second)
}

func testFailedSnapshotChunkSaveIsReported(t *testing.T, methods []string) {
	testFailedSnapshotIOIsReported(t, methods, syscall.ENOSPC, ".*\\.gbsnap")
}

// expected to fail
func TestFailedRDBReadIsReported(t *testing.T) {
	testFailedSnapshotIOIsReported(t,
		[]string{"read", "read_buf"}, syscall.EIO, ".*\\.sst")
}

// expected to fail
func TestFailedSnapshotTempFileRenameIsReported(t *testing.T) {
	testFailedSnapshotIOIsReported(t,
		[]string{"rename"}, syscall.ENOSPC, ".*snapshot-.*")
}

// expected to fail
func TestFailedSnapshotChunkSaveIsReported(t *testing.T) {
	testFailedSnapshotChunkSaveIsReported(t, []string{"write_buf", "write"})
}

// expected to fail
func TestFailedSnapshotReadIsReported(t *testing.T) {
	testFailedSnapshotIOIsReported(t,
		[]string{"read", "read_buf"}, syscall.EIO, ".*\\.gbsnap")
}

// expected to fail
func TestFailedNodeHostIDFileWriteIsReported(t *testing.T) {
	testFailedIOAreReported(t,
		[]string{"write_buf"}, false, syscall.ENOSPC, 0, ".*\\.address")
}

// expected to fail
func TestFailedNodeHostIDFileReadIsReported(t *testing.T) {
	testFailedIOAreReported(t,
		[]string{"read_buf"}, false, syscall.EIO, 0, ".*\\.address")
}

// expected to fail
func TestFailedRDBWritesAreReportedEIO(t *testing.T) {
	testFailedRDBWritesAreReported(t, syscall.EIO)
}

// expected to fail
func TestFailedRDBWritesAreReportedENOSPC(t *testing.T) {
	testFailedRDBWritesAreReported(t, syscall.ENOSPC)
}

// expected to fail
func TestFailedRDBWritesAreReportedEACCESS(t *testing.T) {
	testFailedRDBWritesAreReported(t, syscall.EACCES)
}

// expected to fail
func TestFailedSnapshotWritesAreReported(t *testing.T) {
	// gbsnap
	testFailedIOAreReported(t,
		[]string{"write_buf", "fsync"}, false, syscall.ENOSPC, 0, ".*\\.gbsnap")
}
