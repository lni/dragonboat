// a function from leveldb tests that determines if snappy compression is supported

#include "port/port.h"
#include "leveldb/slice.h"
#include <string>

namespace leveldb {

extern "C" {

bool SnappyCompressionSupported() {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::Snappy_Compress(in.data(), in.size(), &out);
}

}

}
