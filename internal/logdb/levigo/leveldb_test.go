package levigo

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

// This testcase is a port of leveldb's c_test.c.
func TestC(t *testing.T) {
	if GetLevelDBMajorVersion() <= 0 {
		t.Errorf("Major version cannot be less than zero")
	}

	dbname := tempDir(t)
	defer deleteDBDirectory(t, dbname)
	env := NewDefaultEnv()
	cache := NewLRUCache(1 << 20)

	options := NewOptions()
	// options.SetComparator(cmp)
	options.SetErrorIfExists(true)
	options.SetCache(cache)
	options.SetEnv(env)
	options.SetInfoLog(nil)
	options.SetWriteBufferSize(1 << 20)
	options.SetParanoidChecks(true)
	options.SetMaxOpenFiles(10)
	options.SetBlockSize(1024)
	options.SetBlockRestartInterval(8)
	//options.SetCompression(NoCompression)

	roptions := NewReadOptions()
	roptions.SetVerifyChecksums(true)
	roptions.SetFillCache(false)

	woptions := NewWriteOptions()
	woptions.SetSync(true)

	_ = DestroyDatabase(dbname, options)

	db, err := Open(dbname, options)
	if err == nil {
		t.Errorf("Open on missing db should have failed")
	}

	options.SetCreateIfMissing(true)
	db, err = Open(dbname, options)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	putKey := []byte("foo")
	putValue := []byte("hello")
	err = db.Put(woptions, putKey, putValue)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	CheckGet(t, "after Put", db, roptions, putKey, putValue)

	wb := NewWriteBatch()
	wb.Put([]byte("foo"), []byte("a"))
	wb.Clear()
	wb.Put([]byte("bar"), []byte("b"))
	wb.Put([]byte("box"), []byte("c"))
	wb.Delete([]byte("bar"))
	err = db.Write(woptions, wb)
	if err != nil {
		t.Errorf("Write batch failed: %v", err)
	}
	CheckGet(t, "after WriteBatch", db, roptions, []byte("foo"), []byte("hello"))
	CheckGet(t, "after WriteBatch", db, roptions, []byte("bar"), nil)
	CheckGet(t, "after WriteBatch", db, roptions, []byte("box"), []byte("c"))
	// TODO: WriteBatch iteration isn't easy. Suffers same problems as
	// Comparator.
	// wbiter := &TestWBIter{t: t}
	// wb.Iterate(wbiter)
	// if wbiter.pos != 3 {
	// 	t.Errorf("After Iterate, on the wrong pos: %d", wbiter.pos)
	// }
	wb.Close()

	iter := db.NewIterator(roptions)
	if iter.Valid() {
		t.Errorf("Read iterator should not be valid, yet")
	}
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Errorf("Read iterator should be valid after seeking to first record")
	}
	CheckIter(t, iter, []byte("box"), []byte("c"))
	iter.Next()
	CheckIter(t, iter, []byte("foo"), []byte("hello"))
	iter.Prev()
	CheckIter(t, iter, []byte("box"), []byte("c"))
	iter.Prev()
	if iter.Valid() {
		t.Errorf("Read iterator should not be valid after go back past the first record")
	}
	iter.SeekToLast()
	CheckIter(t, iter, []byte("foo"), []byte("hello"))
	iter.Seek([]byte("b"))
	CheckIter(t, iter, []byte("box"), []byte("c"))
	if iter.GetError() != nil {
		t.Errorf("Read iterator has an error we didn't expect: %v", iter.GetError())
	}
	iter.Close()

	// approximate sizes
	n := 20000
	for i := 0; i < n; i++ {
		keybuf := []byte(fmt.Sprintf("k%020d", i))
		valbuf := []byte(fmt.Sprintf("v%020d", i))
		err := db.Put(woptions, keybuf, valbuf)
		if err != nil {
			t.Errorf("Put error in approximate size test: %v", err)
		}
	}

	ranges := []Range{
		{[]byte("a"), []byte("k00000000000000010000")},
		{[]byte("k00000000000000010000"), []byte("z")},
	}
	sizes := db.GetApproximateSizes(ranges)
	if len(sizes) == 2 {
		if sizes[0] <= 0 {
			t.Errorf("First size range was %d", sizes[0])
		}
		if sizes[1] <= 0 {
			t.Errorf("Second size range was %d", sizes[1])
		}
	} else {
		t.Errorf("Expected 2 approx. sizes back, got %d", len(sizes))
	}

	// property
	prop := db.PropertyValue("nosuchprop")
	if prop != "" {
		t.Errorf("property nosuchprop should not have a value")
	}
	prop = db.PropertyValue("leveldb.stats")
	if prop == "" {
		t.Errorf("property leveldb.stats should have a value")
	}

	// snapshot
	snap := db.NewSnapshot()
	err = db.Delete(woptions, []byte("foo"))
	if err != nil {
		t.Errorf("Delete during snapshot test errored: %v", err)
	}
	roptions.SetSnapshot(snap)
	CheckGet(t, "from snapshot", db, roptions, []byte("foo"), []byte("hello"))
	roptions.SetSnapshot(nil)
	CheckGet(t, "from snapshot", db, roptions, []byte("foo"), nil)
	db.ReleaseSnapshot(snap)

	// repair
	db.Close()
	options.SetCreateIfMissing(false)
	options.SetErrorIfExists(false)
	err = RepairDatabase(dbname, options)
	if err != nil {
		t.Errorf("Repairing db failed: %v", err)
	}
	db, err = Open(dbname, options)
	if err != nil {
		t.Errorf("Unable to open repaired db: %v", err)
	}
	CheckGet(t, "repair", db, roptions, []byte("foo"), nil)
	CheckGet(t, "repair", db, roptions, []byte("bar"), nil)
	CheckGet(t, "repair", db, roptions, []byte("box"), []byte("c"))
	options.SetCreateIfMissing(true)
	options.SetErrorIfExists(true)

	// filter
	policy := NewBloomFilter(10)
	db.Close()
	DestroyDatabase(dbname, options)
	options.SetFilterPolicy(policy)
	db, err = Open(dbname, options)
	if err != nil {
		t.Fatalf("Unable to recreate db for filter tests: %v", err)
	}
	err = db.Put(woptions, []byte("foo"), []byte("foovalue"))
	if err != nil {
		t.Errorf("Unable to put 'foo' with filter: %v", err)
	}
	err = db.Put(woptions, []byte("bar"), []byte("barvalue"))
	if err != nil {
		t.Errorf("Unable to put 'bar' with filter: %v", err)
	}
	db.CompactRange(Range{nil, nil})
	CheckGet(t, "filter", db, roptions, []byte("foo"), []byte("foovalue"))
	CheckGet(t, "filter", db, roptions, []byte("bar"), []byte("barvalue"))
	options.SetFilterPolicy(nil)
	policy.Close()

	// cleanup
	db.Close()
	options.Close()
	roptions.Close()
	woptions.Close()
	cache.Close()
	// DestroyComparator(cmp)
	env.Close()
}

func TestNilSlicesInDb(t *testing.T) {
	dbname := tempDir(t)
	defer deleteDBDirectory(t, dbname)
	options := NewOptions()
	options.SetErrorIfExists(true)
	options.SetCreateIfMissing(true)
	ro := NewReadOptions()
	_ = DestroyDatabase(dbname, options)
	db, err := Open(dbname, options)
	if err != nil {
		t.Fatalf("Database could not be opened: %v", err)
	}
	defer db.Close()
	val, err := db.Get(ro, []byte("missing"))
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("A key not in the db should return nil, not %v", val)
	}
	wo := NewWriteOptions()
	db.Put(wo, nil, []byte("love"))
	val, err = db.Get(ro, nil)
	if !bytes.Equal([]byte("love"), val) {
		t.Errorf("Get should see the nil key: %v", val)
	}
	val, err = db.Get(ro, []byte{})
	if !bytes.Equal([]byte("love"), val) {
		t.Errorf("Get shouldn't distinguish between nil key and empty slice key: %v", val)
	}

	err = db.Put(wo, []byte("nilvalue"), nil)
	if err != nil {
		t.Errorf("nil value Put errored: %v", err)
	}
	// Compare with the []byte("missing") case. We expect Get to return a
	// []byte{} here, but expect a nil returned there.
	CheckGet(t, "nil value Put", db, ro, []byte("nilvalue"), []byte{})

	err = db.Put(wo, []byte("emptyvalue"), []byte{})
	if err != nil {
		t.Errorf("empty value Put errored: %v", err)
	}
	CheckGet(t, "empty value Put", db, ro, []byte("emptyvalue"), []byte{})

	err = db.Delete(wo, nil)
	if err != nil {
		t.Errorf("nil key Delete errored: %v", err)
	}
	err = db.Delete(wo, []byte{})
	if err != nil {
		t.Errorf("empty slice key Delete errored: %v", err)
	}

}

func TestIterationValidityLimits(t *testing.T) {
	dbname := tempDir(t)
	defer deleteDBDirectory(t, dbname)
	options := NewOptions()
	options.SetErrorIfExists(true)
	options.SetCreateIfMissing(true)
	ro := NewReadOptions()
	wo := NewWriteOptions()
	_ = DestroyDatabase(dbname, options)
	db, err := Open(dbname, options)
	if err != nil {
		t.Fatalf("Database could not be opened: %v", err)
	}
	defer db.Close()
	db.Put(wo, []byte("bat"), []byte("somedata"))
	db.Put(wo, []byte("done"), []byte("somedata"))
	it := db.NewIterator(ro)
	defer it.Close()
	if it.Valid() {
		t.Errorf("new Iterator was valid")
	}
	it.Seek([]byte("bat"))
	if !it.Valid() {
		t.Errorf("Seek to %#v failed.", []byte("bat"))
	}
	if !bytes.Equal([]byte("bat"), it.Key()) {
		t.Errorf("did not seek to []byte(\"bat\")")
	}
	key := it.Key()
	it.Next()
	if bytes.Equal(key, it.Key()) {
		t.Errorf("key should be a copy of last key")
	}
	it.Next()
	if it.Valid() {
		t.Errorf("iterating off the db should result in an invalid iterator")
	}
	err = it.GetError()
	if err != nil {
		t.Errorf("should not have seen an error on an invalid iterator")
	}
	it.Seek([]byte("bat"))
	if !it.Valid() {
		t.Errorf("Iterator should be valid again")
	}
}

// Tests DataDog-introduced special menu item GetMany()
func TestDBGetMany(t *testing.T) {
	dbname := tempDir(t)
	defer deleteDBDirectory(t, dbname)
	options := NewOptions()
	options.SetErrorIfExists(true)
	options.SetCreateIfMissing(true)
	ro := NewReadOptions()
	wo := NewWriteOptions()
	_ = DestroyDatabase(dbname, options)
	db, err := Open(dbname, options)
	if err != nil {
		t.Fatalf("Database could not be opened: %v", err)
	}
	defer db.Close()

	keyWithEmptyValue := []byte("I have empty value")
	keys := [][]byte{
		[]byte("hello world0"),
		[]byte("hello world1"),
		[]byte("hello world2"),
		[]byte("hello world3"),
		[]byte{}, // Yes an empty byte slice as key is valid
		[]byte("hello world4"),
		nil, // yes a nil key is also valid, it's interpreted as an empty byte slice
		keyWithEmptyValue,
	}

	emptyKeyValue := []byte("value for empty key")
	expectedValues := [][]byte{
		[]byte("value for hello world0"),
		[]byte("value for hello world1"),
		[]byte("value for hello world2"),
		[]byte("value for hello world3"),
		emptyKeyValue,
		[]byte("value for hello world4"),
		emptyKeyValue,
		nil,
	}
	if len(keys) != len(expectedValues) {
		t.Errorf("expecting len(keys) == len(expectedValues) as part of the test setup")
	}

	// Populate the db with some test key-value pairs
	for i := range keys {
		err := db.Put(wo, keys[i], expectedValues[i])
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}

	var values [][]byte
	values, err = db.GetMany(ro, keys)
	if err != nil {
		t.Errorf("expecting all gets succeeded")
	}
	if len(values) != len(keys) {
		t.Errorf("expecting len(values) == len(keys)")
	}
	for i := range keys {
		if bytes.Compare(expectedValues[i], values[i]) != 0 {
			t.Errorf("values[%d] is not the same as expected: %v", i, expectedValues[i])
		}
	}

	keys2 := [][]byte{
		[]byte("hello world0-NOT_FOUND"),
		[]byte("hello world1"),
		[]byte{}, // Yes an empty byte slice as key is valid
		[]byte("hello world5-NOT_FOUND"),
	}
	values, err = db.GetMany(ro, keys2)
	if err != nil {
		t.Errorf("expecting all gets succeeded")
	}
	if len(values) != len(keys2) {
		t.Errorf("expecting len(values) == len(keys2)")
	}

	if values[0] != nil {
		t.Errorf("Expecting non-existant key to return value of nil")
	}
	if bytes.Compare(expectedValues[1], values[1]) != 0 {
		t.Errorf("values[%d] is not the same as expected: %v", 1, expectedValues[1])
	}
	if bytes.Compare(emptyKeyValue, values[2]) != 0 {
		t.Errorf("values[%d] is not the same as expected value under empty key: %v", 2, emptyKeyValue)
	}
	if values[3] != nil {
		t.Errorf("Expecting non-existant key to return value of nil")
	}

	values, err = db.GetMany(ro, nil)
	if values != nil || err != nil {
		t.Errorf("GetMany(): on nil slice should return nil, nil")
	}

	// Calling GetMany on a slice of N nil slices will be interpreted
	// as N Gets of the same empty key
	emptyKeys := make([][]byte, 10)
	values, err = db.GetMany(ro, emptyKeys)
	if err != nil {
		t.Errorf("expecting all gets succeeded")
	}
	if len(values) != len(emptyKeys) {
		t.Errorf("expecting len(values) == len(emptyKeys)")
	}
	for i := range values {
		if bytes.Compare(emptyKeyValue, values[i]) != 0 {
			t.Errorf("GetMany() for an empty key should return the value under that key")
		}
	}

	// Also verify GetMany with keys all of whose values are the empty value
	keysWithEmptyValues := [][]byte{keyWithEmptyValue, keyWithEmptyValue, keyWithEmptyValue}
	emptyValues, err := db.GetMany(ro, keysWithEmptyValues)
	if err != nil {
		t.Errorf("expecting all gets succeeded")
	}
	if len(emptyValues) != len(keysWithEmptyValues) {
		t.Errorf("expecting len(emptyValues) == len(keysWithEmptyValues)")
	}
	for i := range emptyValues {
		if emptyValues[i] == nil || len(emptyValues[i]) != 0 {
			t.Errorf("expecting a zero-length, non-nil, empty-value for key %d", i)
		}
	}
}

func TestMultiKeyErrors(t *testing.T) {
	expectedKeyIdxToErr := map[int]error{
		11: errors.New("Error bar for key-11"),
		2:  errors.New("Error foo for key-2"),
		5:  errors.New("Error foo for key-5"),
		7:  errors.New("Error foo for key-7"),
	}

	mke := &MultiKeyError{}
	for keyIdx, err := range expectedKeyIdxToErr {
		mke.addKeyErr(keyIdx, err)
	}
	var err error = mke
	_, ok := err.(*MultiKeyError)
	if !ok {
		t.Errorf("type assertion failed")
	}

	errByKeyIdx := mke.ErrorsByKeyIdx()
	if len(errByKeyIdx) != len(expectedKeyIdxToErr) {
		t.Errorf("expecting %d errors", len(expectedKeyIdxToErr))
	}

	t.Logf("multikey-error str: %s", mke.Error())
	for keyIdx, err := range errByKeyIdx {
		t.Logf("For key index %d, error: %s", keyIdx, err)
		if err != expectedKeyIdxToErr[keyIdx] {
			t.Errorf("Expecting key index %d holding error %s, got %s instead", keyIdx, expectedKeyIdxToErr[keyIdx], err)
		}
	}
}
func BenchmarkDBGets(b *testing.B) {
	b.Run("multiple-Get()s", func(b *testing.B) { benchmarkDBGets(b, false) })
	b.Run("one-GetMany()", func(b *testing.B) { benchmarkDBGets(b, true) })
}

func benchmarkDBGets(b *testing.B, useGetMany bool) {
	dbname, err := ioutil.TempDir("", "levigo-benchmark-")
	if err != nil {
		b.Fatalf("Failed to create db for benchmark")
	}
	options := NewOptions()
	options.SetErrorIfExists(true)
	options.SetCreateIfMissing(true)
	ro := NewReadOptions()
	wo := NewWriteOptions()
	db, err := Open(dbname, options)
	if err != nil {
		b.Fatalf("Database could not be opened: %v", err)
	}
	defer os.RemoveAll(dbname)
	defer db.Close()

	// Populate the db with some test key-value pairs
	const fixedKeyLen = 20
	const fixedValueLen = 128
	keys := make([][]byte, 10000)
	expectedValues := make([][]byte, len(keys))
	nb := 0
	for i := range keys {
		keys[i] = make([]byte, fixedKeyLen)
		n, err := rand.Read(keys[i])
		if n != len(keys[i]) || err != nil {
			b.Fatalf("could not generate random keys")
		}
		expectedValues[i] = make([]byte, fixedValueLen)
		n, err = rand.Read(expectedValues[i])
		if n != len(expectedValues[i]) || err != nil {
			b.Fatalf("could not generate random values")
		}
		err = db.Put(wo, keys[i], expectedValues[i])
		nb += len(keys[i])
		if err != nil {
			b.Errorf("Put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.SetBytes(int64(nb))

	for i := 0; i < b.N; i++ {
		if useGetMany {
			values, _ := db.GetMany(ro, keys)
			runtime.KeepAlive(values)
		} else {
			for j := range keys {
				v, _ := db.Get(ro, keys[j])
				runtime.KeepAlive(v)
			}
		}
	}
}

func CheckGet(t *testing.T, where string, db *DB, roptions *ReadOptions, key, expected []byte) {
	getValue, err := db.Get(roptions, key)

	if err != nil {
		t.Errorf("%s, Get failed: %v", where, err)
	}
	if !bytes.Equal(getValue, expected) {
		t.Errorf("%s, expected Get value %v, got %v", where, expected, getValue)
	}
}

func WBIterCheckEqual(t *testing.T, where string, which string, pos int, expected, given []byte) {
	if !bytes.Equal(expected, given) {
		t.Errorf("%s at pos %d, %s expected: %v, got: %v", where, pos, which, expected, given)
	}
}

func CheckIter(t *testing.T, it *Iterator, key, value []byte) {
	if !bytes.Equal(key, it.Key()) {
		t.Errorf("Iterator: expected key %v, got %v", key, it.Key())
	}
	if !bytes.Equal(value, it.Value()) {
		t.Errorf("Iterator: expected value %v, got %v", value, it.Value())
	}
}

func deleteDBDirectory(t *testing.T, dirPath string) {
	err := os.RemoveAll(dirPath)
	if err != nil {
		t.Errorf("Unable to remove database directory: %s", dirPath)
	}
}

func tempDir(t *testing.T) string {
	bottom := fmt.Sprintf("levigo-test-%d", rand.Int())
	path := filepath.Join(os.TempDir(), bottom)
	deleteDBDirectory(t, path)
	return path
}
