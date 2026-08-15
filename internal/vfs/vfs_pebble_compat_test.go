// Copyright 2017-2025 Lei Ni (nilei81@gmail.com) and other contributors.
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

package vfs

import (
	"os"
	"path/filepath"
	"testing"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"
)

// TestPebbleFileWrapper tests the pebbleFile wrapper that adds
// PebbleDB v1.1.5 compatibility methods to lni/vfs.File
func TestPebbleFileWrapper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-compat-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")

	// Test Create returns pebbleFile wrapper
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	require.NotNil(t, f)

	// Test that file implements pvfs.File interface
	var _ pvfs.File = f

	// Test Write
	data := []byte("test data")
	n, err := f.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// Test Sync
	require.NoError(t, f.Sync())

	// Test Close
	require.NoError(t, f.Close())
}

// TestPebbleFileFdMethod tests the Fd() method returns invalid fd
func TestPebbleFileFdMethod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-fd-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// pebbleFile wrapper should implement Fd()
	pf, ok := f.(interface{ Fd() uintptr })
	require.True(t, ok, "pebbleFile should implement Fd() method")

	// Fd() should return -1 (invalid fd) as lni/vfs doesn't support file descriptors
	fd := pf.Fd()
	require.Equal(t, ^uintptr(0), fd, "Fd() should return -1 (invalid fd)")
}

// TestPebbleFilePreallocate tests the Preallocate() no-op method
func TestPebbleFilePreallocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-preallocate-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// pebbleFile wrapper should implement Preallocate()
	pf, ok := f.(interface{ Preallocate(offset, length int64) error })
	require.True(t, ok, "pebbleFile should implement Preallocate() method")

	// Preallocate() should succeed (no-op is acceptable)
	err = pf.Preallocate(0, 1024*1024)
	require.NoError(t, err, "Preallocate() should succeed as no-op")
}

// TestPebbleFilePrefetch tests the Prefetch() no-op method
func TestPebbleFilePrefetch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-prefetch-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)

	// Write some data first
	data := []byte("test data for prefetch")
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen for reading
	f, err = pfs.Open(testFile)
	require.NoError(t, err)
	defer f.Close()

	// pebbleFile wrapper should implement Prefetch()
	pf, ok := f.(interface{ Prefetch(offset, length int64) error })
	require.True(t, ok, "pebbleFile should implement Prefetch() method")

	// Prefetch() should succeed (no-op is acceptable)
	err = pf.Prefetch(0, int64(len(data)))
	require.NoError(t, err, "Prefetch() should succeed as no-op")
}

// TestPebbleFileSyncData tests the SyncData() method
func TestPebbleFileSyncData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-syncdata-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// Write some data
	data := []byte("test data for sync")
	_, err = f.Write(data)
	require.NoError(t, err)

	// pebbleFile wrapper should implement SyncData()
	pf, ok := f.(interface{ SyncData() error })
	require.True(t, ok, "pebbleFile should implement SyncData() method")

	// SyncData() should succeed (delegates to Sync())
	err = pf.SyncData()
	require.NoError(t, err, "SyncData() should succeed")
}

// TestPebbleFileSyncTo tests the SyncTo() method
func TestPebbleFileSyncTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-syncto-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// Write some data
	data := []byte("test data for sync to")
	_, err = f.Write(data)
	require.NoError(t, err)

	// pebbleFile wrapper should implement SyncTo()
	pf, ok := f.(interface{ SyncTo(length int64) (bool, error) })
	require.True(t, ok, "pebbleFile should implement SyncTo() method")

	// SyncTo() should succeed (delegates to Sync())
	fullSync, err := pf.SyncTo(int64(len(data)))
	require.NoError(t, err, "SyncTo() should succeed")
	require.False(t, fullSync, "SyncTo() should return false (syncs entire file)")
}

// TestPebbleFSOpenReadWrite tests the OpenReadWrite() method
func TestPebbleFSOpenReadWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-openrw-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")

	// OpenReadWrite should create the file if it doesn't exist
	f, err := pfs.OpenReadWrite(testFile)
	require.NoError(t, err)
	require.NotNil(t, f)

	// Should be able to write
	data := []byte("test data")
	n, err := f.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	require.NoError(t, f.Close())
}

// TestPebbleFSOpenWithOptions tests Open() with PebbleDB options
func TestPebbleFSOpenWithOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-openopts-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "test.dat")

	// Create file first
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	_, err = f.Write([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open with random read option (should not fail)
	f, err = pfs.Open(testFile, pvfs.RandomReadsOption)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.NoError(t, f.Close())
}

// TestAllPebbleFSMethods tests that PebbleFS implements all required pvfs.FS methods
func TestAllPebbleFSMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	// Verify PebbleFS implements pvfs.FS interface
	var _ pvfs.FS = pfs

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-allmethods-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	// Test GetDiskUsage (MemFS doesn't support this, so we just check the API exists)
	_, err := pfs.GetDiskUsage(testDir)
	// MemFS returns "not supported" error, which is acceptable
	_ = err

	// Test Create
	testFile := filepath.Join(testDir, "create-test.dat")
	f, err := pfs.Create(testFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Test Open
	f, err = pfs.Open(testFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Test Link
	linkFile := filepath.Join(testDir, "link-test.dat")
	err = pfs.Link(testFile, linkFile)
	require.NoError(t, err)

	// Test Stat
	info, err := pfs.Stat(testFile)
	require.NoError(t, err)
	require.NotNil(t, info)

	// Test List
	files, err := pfs.List(testDir)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	// Test Rename
	newFile := filepath.Join(testDir, "renamed.dat")
	err = pfs.Rename(linkFile, newFile)
	require.NoError(t, err)

	// Test ReuseForWrite
	reusedFile := filepath.Join(testDir, "reused.dat")
	f, err = pfs.ReuseForWrite(newFile, reusedFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Test OpenDir
	f, err = pfs.OpenDir(testDir)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Test Lock
	lockFile := filepath.Join(testDir, "lock")
	closer, err := pfs.Lock(lockFile)
	require.NoError(t, err)
	require.NotNil(t, closer)
	require.NoError(t, closer.Close())

	// Test Remove
	err = pfs.Remove(reusedFile)
	require.NoError(t, err)

	// Test RemoveAll
	err = pfs.RemoveAll(testDir)
	require.NoError(t, err)

	// Test Path methods
	require.Equal(t, "test.dat", pfs.PathBase("/path/to/test.dat"))
	require.Equal(t, "/path/to", pfs.PathDir("/path/to/test.dat"))
	require.Equal(t, filepath.Join("path", "to", "file"), pfs.PathJoin("path", "to", "file"))
}

// TestPebbleFileIntegration tests pebbleFile wrapper in a realistic scenario
func TestPebbleFileIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := NewMemFS()
	pfs := NewPebbleFS(fs)

	testDir := filepath.Join(os.TempDir(), "vfs-pebble-integration-test")
	require.NoError(t, pfs.MkdirAll(testDir, 0755))
	defer func() {
		_ = pfs.RemoveAll(testDir)
	}()

	testFile := filepath.Join(testDir, "integration.dat")

	// Create and write
	f, err := pfs.Create(testFile)
	require.NoError(t, err)

	// Get pebbleFile interface
	pf, ok := f.(interface {
		Fd() uintptr
		Preallocate(offset, length int64) error
		Prefetch(offset, length int64) error
		SyncData() error
		SyncTo(length int64) (bool, error)
	})
	require.True(t, ok, "file should implement all pebbleFile methods")

	// Test all new methods work together
	data := []byte("integration test data")

	// Preallocate space (no-op, but should not fail)
	require.NoError(t, pf.Preallocate(0, 1024))

	// Write data
	n, err := f.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// Sync data
	require.NoError(t, pf.SyncData())

	// Sync to specific length
	fullSync, err := pf.SyncTo(int64(len(data)))
	require.NoError(t, err)
	require.False(t, fullSync)

	// Verify Fd returns invalid fd
	fd := pf.Fd()
	require.Equal(t, ^uintptr(0), fd)

	require.NoError(t, f.Close())

	// Reopen and read
	f, err = pfs.Open(testFile)
	require.NoError(t, err)

	pf2, ok := f.(interface {
		Prefetch(offset, length int64) error
	})
	require.True(t, ok)

	// Prefetch (no-op, but should not fail)
	require.NoError(t, pf2.Prefetch(0, int64(len(data))))

	// Read data back
	readData := make([]byte, len(data))
	n, err = f.Read(readData)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, readData)

	require.NoError(t, f.Close())
}
