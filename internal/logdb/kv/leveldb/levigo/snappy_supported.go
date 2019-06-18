package levigo

// #include "snappy_supported.h"
import "C"

// SnappySupported ...
func SnappySupported() bool {
	return bool(C.SnappyCompressionSupported())
}
