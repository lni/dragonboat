package levigo

// #include "seek_to.h"
import "C"
import "unsafe"

// SeekResult is the result of a SeekTo.
type SeekResult struct {
	Valid bool
	Equal bool
	Value []byte
}

// SeekTo will seek the given iterator to the given key and return a result
// about it. It will check the validity of the key, if they're equal and if so,
// it will also fetch the value. The value slice is a direct reference to the
// underlying memory so it can only be referenced while the iterator is valid
// and has not moved. All of this exists because it eliminates a bunch fo cgo
// overhead and is faster.
func SeekTo(it *Iterator, key []byte) SeekResult {
	out := C.leveldb_iter_seek_to(it.Iter, (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	sr := SeekResult{
		Valid: ucharToBool(out.valid),
		Equal: ucharToBool(out.equal),
	}

	if sr.Valid && sr.Equal {
		sr.Value = C.GoBytes(unsafe.Pointer(out.val_data), C.int(out.val_len))
	}

	return sr
}

// Exists will seek the iterator to the given key and return true if it exists.
func Exists(it *Iterator, key []byte) bool {
	out := C.leveldb_iter_exists(it.Iter, (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	return ucharToBool(out)
}
