// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef BINDING_INCLUDE_DRAGONBOAT_SNAPSHOTIO_H_
#define BINDING_INCLUDE_DRAGONBOAT_SNAPSHOTIO_H_

#include <stdint.h>
#include <unistd.h>
#include <string>
#include "dragonboat/dragonboat.h"
#include "dragonboat/managed.h"

namespace dragonboat {

// IOResult is the struct returned by the snapshot read/write methods used
// to indicate the results of the read/write operations.
struct IOResult {
  size_t size;
  int error;
};

using IOResult = struct IOResult;

// SnapshotWriter is the base snapshot writer class.
class SnapshotWriter
{
 public:
  SnapshotWriter() noexcept;
  virtual ~SnapshotWriter();
  // Write writes the specified data buffer of length size to the writer. It
  // returns an IOResult object to indicate the result of the write operation.
  // The size field of the IOResult object contains the number of bytes written
  // or -1 on error. On error, the error field of IOResult will be set to the
  // error code provided by the OS.
  IOResult Write(const Byte *data, size_t size) noexcept;
 protected:
  virtual IOResult write(const Byte *data, size_t size) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(SnapshotWriter);
};

// SnapshotReader is the base snapshot reader class.
class SnapshotReader
{
 public:
  SnapshotReader() noexcept;
  virtual ~SnapshotReader();
  // Read reads up to size bytes to the specified data buffer which is of
  // length size. It returns an IOResult object to indicate the result of the
  // read operation. The size field of the IOResult is the number of bytes
  // read or -1 on error. On error, the error field of the IOResult is set to
  // the error code provided by the underlying OS.
  IOResult Read(Byte *data, size_t size) noexcept;
 protected:
  virtual IOResult read(Byte *data, size_t size) noexcept = 0;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(SnapshotReader);
};

// ProxySnapshotWriter is a proxy class used to delegate all snapshot writes to
// the Go side using the managed Go SnapshotWriter instance identified by oid.
class ProxySnapshotWriter : public SnapshotWriter, ManagedObject
{
 public:
  explicit ProxySnapshotWriter(oid_t oid) noexcept;
  ~ProxySnapshotWriter();
 protected:
  IOResult write(const Byte *data, size_t size) noexcept override;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(ProxySnapshotWriter);
};

// ProxySnapshotReader is a proxy class used to delegate all snapshot reads to
// the Go side using the managed Go SnapshotReader instance identified by oid.
class ProxySnapshotReader : public SnapshotReader, ManagedObject
{
 public:
  explicit ProxySnapshotReader(oid_t oid) noexcept;
  ~ProxySnapshotReader();
 protected:
  IOResult read(Byte *data, size_t size) noexcept override;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(ProxySnapshotReader);
};

// DoneChan is a proxy class for a Go channel used to notify the SaveSnapshot
// and RecoverFromSnapshot methods that the system has request it to stop.
class DoneChan : public ManagedObject
{
 public:
  explicit DoneChan(oid_t oid) noexcept;
  ~DoneChan();
  // Closed returns a boolean value indicating whether the system has request
  // the DoneChan to be closed. Such a closed channel is used to indicate that
  // the SaveSnapshot or RecoverFromSnapshot methods should stop.
  bool Closed() const noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(DoneChan);
};

// SnapshotFile is the struct used to describe an external file included as a
// part of the snapshot.
struct SnapshotFile {
  uint64_t FileID;
  std::string Filepath;
  Byte *Metadata;
  size_t Length;
};

using SnapshotFile = struct SnapshotFile;

// SnapshotFileCollection is the proxy class for SnapshotFileCollection in
// Go. It is used by the saveSnapshot() method to record external files that
// should be included as a part of the snapshot being generated.
class SnapshotFileCollection : public ManagedObject
{
 public:
  explicit SnapshotFileCollection(oid_t oid) noexcept;
  // AddFile adds an external file to the snapshot being currently generated.
  // The file must has been finalized meaning its content should not change
  // in the future. It is your application's responsibility to make sure that
  // the file being added can be accessible from the current process and it is
  // possible to create a hard link to it from the NodeHostDir directory
  // specified in NodeHost's NodeHostConfig. The file is identified by the
  // specified fileID. The metadata byte array is the metadata of the file being
  // added, it can be the checksum of the file, file type, file name or other
  // file hierarchy information, it can also be a serialized combination of such
  // metadata. The parameter length is the length of the metadata array in
  // number of bytes.
  void AddFile(uint64_t fileID,
    std::string path, const Byte *metadata, size_t length) noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(SnapshotFileCollection);
};

class CollectedFiles {
 public:
  CollectedFiles();
  ~CollectedFiles();
  void AddFile(uint64_t fileID,
    std::string path, const Byte *metadata, size_t length);
  std::vector<SnapshotFile> GetFiles() const;
 private:
  std::vector<SnapshotFile> files_;
};

}  // namespace dragonboat

#endif  // BINDING_INCLUDE_DRAGONBOAT_SNAPSHOTIO_H_
