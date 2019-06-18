package levigo

/*
#include <stdlib.h>
#include "leveldb/c.h"

// This function exists only to clean up lack-of-const warnings when
// leveldb_approximate_sizes is called from Go-land.
void levigo_leveldb_approximate_sizes(
    leveldb_t* db,
    int num_ranges,
    char** range_start_key, const size_t* range_start_key_len,
    char** range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  leveldb_approximate_sizes(db,
                            num_ranges,
                            (const char* const*)range_start_key,
                            range_start_key_len,
                            (const char* const*)range_limit_key,
                            range_limit_key_len,
                            sizes);
}
*/
import "C"

import (
	"errors"
	"unsafe"
)

// DatabaseError ...
type DatabaseError string

// Error ...
func (e DatabaseError) Error() string {
	return string(e)
}

// ErrDBClosed ...
var ErrDBClosed = errors.New("database is closed")

// DB is a reusable handle to a LevelDB database on disk, created by Open.
//
// To avoid memory and file descriptor leaks, call Close when the process no
// longer needs the handle. Calls to any DB method made after Close will
// panic.
//
// The DB instance may be shared between goroutines. The usual data race
// conditions will occur if the same key is written to from more than one, of
// course.
type DB struct {
	Ldb *C.leveldb_t

	// TLDR: Closed is not racey, it's a best attempt. If `-race` says it's racey,
	// then it's your code that is racey, not levigo.
	//
	// This indicates if the DB is closed or not. LevelDB provides it's own closed
	// detection that appears in the form of a sigtrap panic, which can be quite
	// confusing. So rather than users hit that, we attempt to give them a better
	// panic by checking this flag in functions that LevelDB would have done
	// it's own panic. This is not protected by a mutex because it's a best case
	// attempt to catch invalid usage. If access in racey and gets to LevelDB,
	// so be it, the user will see the sigtrap panic.
	// Because LevelDB has it's own mutex to detect the usage, we didn't want to
	// put another one up here and drive performance down even more.
	// So if you use `-race` and it says closed is racey, it's your code that is
	// racey, not levigo.
	closed bool
}

// Range is a range of keys in the database. GetApproximateSizes calls with it
// begin at the key Start and end right before the key Limit.
type Range struct {
	Start []byte
	Limit []byte
}

// Snapshot provides a consistent view of read operations in a DB.
//
// Snapshot is used in read operations by setting it on a
// ReadOptions. Snapshots are created by calling DB.NewSnapshot.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
type Snapshot struct {
	snap *C.leveldb_snapshot_t
}

// Open opens a database.
//
// Creating a new database is done by calling SetCreateIfMissing(true) on the
// Options passed to Open.
//
// It is usually wise to set a Cache object on the Options with SetCache to
// keep recently used data from that database in memory.
func Open(dbname string, o *Options) (*DB, error) {
	var errStr *C.char
	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	leveldb := C.leveldb_open(o.Opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}
	return &DB{leveldb, false}, nil
}

// DestroyDatabase removes a database entirely, removing everything from the
// filesystem.
func DestroyDatabase(dbname string, o *Options) error {
	var errStr *C.char
	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	C.leveldb_destroy_db(o.Opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// RepairDatabase attempts to repair a database.
//
// If the database is unrepairable, an error is returned.
func RepairDatabase(dbname string, o *Options) error {
	var errStr *C.char
	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	C.leveldb_repair_db(o.Opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Put writes data associated with a key to the database.
//
// If a nil []byte is passed in as value, it will be returned by Get
// as an zero-length slice. The WriteOptions passed in can be reused
// by multiple calls to this and if the WriteOptions is left unchanged.
//
// The key and value byte slices may be reused safely. Put takes a copy of
// them before returning.
func (db *DB) Put(wo *WriteOptions, key, value []byte) error {
	if db.closed {
		panic(ErrDBClosed)
	}

	var errStr *C.char
	// leveldb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these []byte being
	// reclaimed by GC.
	var k, v *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) != 0 {
		v = (*C.char)(unsafe.Pointer(&value[0]))
	}

	lenk := len(key)
	lenv := len(value)
	C.leveldb_put(
		db.Ldb, wo.Opt, k, C.size_t(lenk), v, C.size_t(lenv), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Get returns the data associated with the key from the database.
//
// If the key does not exist in the database, a nil []byte is returned. If the
// key does exist, but the data is zero-length in the database, a zero-length
// []byte will be returned.
//
// The key byte slice may be reused safely. Get takes a copy of
// them before returning.
func (db *DB) Get(ro *ReadOptions, key []byte) ([]byte, error) {
	if db.closed {
		panic(ErrDBClosed)
	}

	var errStr *C.char
	var vallen C.size_t
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	value := C.leveldb_get(
		db.Ldb, ro.Opt, k, C.size_t(len(key)), &vallen, &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}

	if value == nil {
		return nil, nil
	}

	defer C.leveldb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}

// GetMany returns the values associated with keys from the database.
//
// GetMany fills in the entries of values consistent with Get():
// - If keys[i] exists in the database with non-zero value, values[i] holds its non-zero len []byte slice.
// - If keys[i] does not exist in the database, values[i] == nil.
// - If keys[i] does exist, but its value is zero-length, values[i] == []byte{}.
// - If there's an error looking up keys[i], values[i] == nil.
//
// err == nil if gets for all of the keys succeeded. Otherwise err be a MultiKeyError
// that caller may cleanly handle like:
//
// values, err := GetMany(ro, keys)
// if err != nil {
//      if mke, ok := err.(*levigo.MultiKeyError); ok {
//          errsByKeyIdx := mke.ErrorsByKeyIdx()
//          for idx, err := range errsByKeyIdx {
//              fmt.Printf("Failed for key %s, error: %s", keys[idx], err)
//          }
//      }
// }
//
// The keys byte slice may be reused safely. Go takes a copy of
// them before returning.
/*
func (db *DB) GetMany(ro *ReadOptions, keys [][]byte) ([][]byte, error) {
	if db.closed {
		panic(ErrDBClosed)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	var packedKeysBuffer bytes.Buffer
	cKeyLens := make([]C.size_t, len(keys))
	for i := range keys {
		packedKeysBuffer.Write(keys[i])
		cKeyLens[i] = C.size_t(len(keys[i]))
	}
	packedKeysBytes := packedKeysBuffer.Bytes()
	if len(packedKeysBytes) == 0 {
		// If all the keys are empty, abide by the behavior of Get() when given
		// an empty key and return the same (value, error) pair for all keys[i].
		// This is the most consistent, though somewhat hilarious, behavior.
		emptyKeyVal, err := db.Get(ro, []byte{})
		if err != nil {
			return nil, fmt.Errorf("singleton error from GetMany() on the same empty key: %s", err)
		}
		values := make([][]byte, len(keys))
		for i := range keys {
			values[i] = emptyKeyVal
		}
		return values, nil
	}

	var cPackedVals *C.char
	var cValLens *C.int
	var cPackedErrs *C.char
	var cErrLens *C.size_t
	C.leveldb_getmany(
		db.Ldb, ro.Opt, (*C.char)(unsafe.Pointer(&packedKeysBytes[0])), C.size_t(len(keys)), &cKeyLens[0],
		&cPackedVals, &cValLens,
		&cPackedErrs, &cErrLens)
	defer C.leveldb_free(unsafe.Pointer(cPackedErrs))
	defer C.leveldb_free(unsafe.Pointer(cErrLens))
	defer C.leveldb_free(unsafe.Pointer(cPackedVals))
	defer C.leveldb_free(unsafe.Pointer(cValLens))

	// Unpack the errors from C chars into Golang error objects which is then wrapped in MultiKeyError
	var err error
	if cPackedErrs != nil {
		errLens := (*[1 << 30]C.size_t)(unsafe.Pointer(cErrLens))[:len(keys):len(keys)]
		offset := 0
		var multiKeyErr *MultiKeyError
		for i := range errLens {
			if errLens[i] == 0 {
				continue
			}
			b := C.GoBytes(unsafe.Pointer(uintptr(unsafe.Pointer(cPackedErrs))+uintptr(offset)), C.int(errLens[i]))
			errStrLen := int(errLens[i])
			if multiKeyErr == nil {
				multiKeyErr = &MultiKeyError{}
			}
			multiKeyErr.addKeyErr(i, fmt.Errorf(string(b[:errStrLen])))
			offset += errStrLen
		}
		err = multiKeyErr
	} else {
		// err == nil indicates all gets succeeded
	}

	// Unpack the packed values from C chars into Golang byte slices
	// Invariant: cValLens is NOT nil with len(keys) entries, cPackedVals
	// may be nil if all gets failed with an error or the values of all keys are empty.
	valueLens := (*[1 << 30]C.int)(unsafe.Pointer(cValLens))[:len(keys):len(keys)]
	values := make([][]byte, len(valueLens))
	valOffset := 0
	for i := range valueLens {
		if valueLens[i] > 0 {
			values[i] = C.GoBytes(unsafe.Pointer(uintptr(unsafe.Pointer(cPackedVals))+uintptr(valOffset)), C.int(valueLens[i]))
			valOffset += int(valueLens[i])
		} else if (err == nil || err.(*MultiKeyError).errAt(i) == nil) && valueLens[i] == 0 {
			// No error and no value, it means keys[i] is found but with empty value
			values[i] = []byte{}
		}
		// else we have either:
		// 1. (err != nil && multiKeyErr.errAt(i) == nil) && valueLens[i] < 0 indicating keys[i] is not found
		// 2. (err != nil && multiKeyErr.errAt(i) != nil) indicating an error looking up keys[i].
		// In both cases values[i] defaults to nil
	}

	return values, err
}*/

// Delete removes the data associated with the key from the database.
//
// The key byte slice may be reused safely. Delete takes a copy of
// them before returning. The WriteOptions passed in can be reused by
// multiple calls to this and if the WriteOptions is left unchanged.
func (db *DB) Delete(wo *WriteOptions, key []byte) error {
	if db.closed {
		panic(ErrDBClosed)
	}

	var errStr *C.char
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	C.leveldb_delete(
		db.Ldb, wo.Opt, k, C.size_t(len(key)), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Write atomically writes a WriteBatch to disk. The WriteOptions
// passed in can be reused by multiple calls to this and other methods.
func (db *DB) Write(wo *WriteOptions, w *WriteBatch) error {
	if db.closed {
		panic(ErrDBClosed)
	}

	var errStr *C.char
	C.leveldb_write(db.Ldb, wo.Opt, w.wbatch, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// NewIterator returns an Iterator over the the database that uses the
// ReadOptions given.
//
// Often, this is used for large, offline bulk reads while serving live
// traffic. In that case, it may be wise to disable caching so that the data
// processed by the returned Iterator does not displace the already cached
// data. This can be done by calling SetFillCache(false) on the ReadOptions
// before passing it here.
//
// Similarly, ReadOptions.SetSnapshot is also useful.
//
// The ReadOptions passed in can be reused by multiple calls to this
// and other methods if the ReadOptions is left unchanged.
func (db *DB) NewIterator(ro *ReadOptions) *Iterator {
	if db.closed {
		panic(ErrDBClosed)
	}

	it := C.leveldb_create_iterator(db.Ldb, ro.Opt)
	return &Iterator{Iter: it}
}

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizes(ranges []Range) []uint64 {
	starts := make([]*C.char, len(ranges))
	limits := make([]*C.char, len(ranges))
	startLens := make([]C.size_t, len(ranges))
	limitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		starts[i] = C.CString(string(r.Start))
		startLens[i] = C.size_t(len(r.Start))
		limits[i] = C.CString(string(r.Limit))
		limitLens[i] = C.size_t(len(r.Limit))
	}
	sizes := make([]uint64, len(ranges))
	numranges := C.int(len(ranges))
	startsPtr := &starts[0]
	limitsPtr := &limits[0]
	startLensPtr := &startLens[0]
	limitLensPtr := &limitLens[0]
	sizesPtr := (*C.uint64_t)(&sizes[0])
	C.levigo_leveldb_approximate_sizes(
		db.Ldb, numranges, startsPtr, startLensPtr,
		limitsPtr, limitLensPtr, sizesPtr)
	for i := range ranges {
		C.free(unsafe.Pointer(starts[i]))
		C.free(unsafe.Pointer(limits[i]))
	}
	return sizes
}

// PropertyValue returns the value of a database property.
//
// Examples of properties include "leveldb.stats", "leveldb.sstables",
// and "leveldb.num-files-at-level0".
func (db *DB) PropertyValue(propName string) string {
	if db.closed {
		panic(ErrDBClosed)
	}

	cname := C.CString(propName)
	value := C.GoString(C.leveldb_property_value(db.Ldb, cname))
	C.free(unsafe.Pointer(cname))
	return value
}

// NewSnapshot creates a new snapshot of the database.
//
// The Snapshot, when used in a ReadOptions, provides a consistent
// view of state of the database at the the snapshot was created.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
//
// See the LevelDB documentation for details.
func (db *DB) NewSnapshot() *Snapshot {
	if db.closed {
		panic(ErrDBClosed)
	}

	return &Snapshot{C.leveldb_create_snapshot(db.Ldb)}
}

// ReleaseSnapshot removes the snapshot from the database's list of snapshots,
// and deallocates it.
func (db *DB) ReleaseSnapshot(snap *Snapshot) {
	if db.closed {
		panic(ErrDBClosed)
	}

	C.leveldb_release_snapshot(db.Ldb, snap.snap)
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (db *DB) CompactRange(r Range) {
	if db.closed {
		panic(ErrDBClosed)
	}

	var start, limit *C.char
	if len(r.Start) != 0 {
		start = (*C.char)(unsafe.Pointer(&r.Start[0]))
	}
	if len(r.Limit) != 0 {
		limit = (*C.char)(unsafe.Pointer(&r.Limit[0]))
	}
	C.leveldb_compact_range(
		db.Ldb, start, C.size_t(len(r.Start)), limit, C.size_t(len(r.Limit)))
}

// Close closes the database, rendering it unusable for I/O, by deallocating
// the underlying handle.
//
// Any attempts to use the DB after Close is called will panic.
func (db *DB) Close() {
	if db.closed {
		return
	}

	db.closed = true
	C.leveldb_close(db.Ldb)
}
