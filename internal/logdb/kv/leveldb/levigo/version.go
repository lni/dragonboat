package levigo

// #include "leveldb/c.h"
import "C"

// GetLevelDBMajorVersion ...
func GetLevelDBMajorVersion() int {
	return int(C.leveldb_major_version())
}

// GetLevelDBMinorVersion ...
func GetLevelDBMinorVersion() int {
	return int(C.leveldb_minor_version())
}
