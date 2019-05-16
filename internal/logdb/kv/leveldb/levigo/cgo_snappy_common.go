package levigo

// #cgo CFLAGS: -I${SRCDIR}/deps/snappy -DSNAPPY
// #cgo CXXFLAGS: -I${SRCDIR}/deps/snappy -std=c++11 -g -O2 -fPIC -DPIC -DSNAPPY
import "C"
