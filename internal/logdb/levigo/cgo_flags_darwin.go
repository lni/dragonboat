// +build darwin

package levigo

// TODO Elijah: SRCDIR?

// #cgo CFLAGS: -DOS_MACOSX
// #cgo CXXFLAGS: -DOS_MACOSX
// #include "leveldb/c.h"
import "C"
