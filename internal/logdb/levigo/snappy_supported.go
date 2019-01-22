package levigo

// #include "snappy_supported.h"
import "C"

func SnappySupported() bool {
	return bool(C.SnappyCompressionSupported())
}
