// +build linux

package levigo

// #cgo CFLAGS: -DOS_LINUX
// #cgo CXXFLAGS: -DOS_LINUX
// #include "leveldb/c.h"
import "C"
