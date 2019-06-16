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

#include <cassert>
#include <cstring>
#include <errno.h>
#include "_cgo_export.h"
#include "dragonboat/managed.h"
#include "dragonboat/dragonboat.h"
#include "dragonboat/statemachine.h"
#include "dragonboat/snapshotio.h"

namespace dragonboat {

WriteToManagedWriter_return writeToManagedWriter(oid_t oid,
  const Byte *data, size_t size)
{
  GoSlice bytes;
  bytes.data = reinterpret_cast<char *>(const_cast<Byte *>(data));
  bytes.len = size;
  bytes.cap = size;
  return WriteToManagedWriter(oid, bytes);
}

ReadFromManagedReader_return  readFromManagedReader(oid_t oid,
  Byte *data, size_t size)
{
  GoSlice bytes;
  bytes.data = reinterpret_cast<char *>(const_cast<Byte *>(data));
  bytes.len = size;
  bytes.cap = size;
  return ReadFromManagedReader(oid, bytes);
}

bool doneChanClosed(oid_t oid)
{
  return DoneChanClosed(oid);
}

void addFileToSnapshotFileCollection(oid_t oid, uint64_t fileID,
  std::string path, const Byte *metadata, size_t size)
{
  GoSlice goMetadata;
  goMetadata.data = reinterpret_cast<char *>(const_cast<Byte*>(metadata));
  goMetadata.len = size;
  goMetadata.cap = size;
  GoSlice pathData;
  pathData.data = const_cast<char*>(path.c_str());
  pathData.len = path.size();
  pathData.cap = path.size();
  AddToSnapshotFileCollection(oid, fileID, pathData, goMetadata);
}

SnapshotWriter::SnapshotWriter() noexcept
{
}

SnapshotWriter::~SnapshotWriter()
{
}

IOResult SnapshotWriter::Write(const Byte *data, size_t size) noexcept
{
  return this->write(data, size);
}

SnapshotReader::SnapshotReader() noexcept
{
}

SnapshotReader::~SnapshotReader()
{
}

IOResult SnapshotReader::Read(Byte *data, size_t size) noexcept
{
  return this->read(data, size);
}

ProxySnapshotWriter::ProxySnapshotWriter(oid_t oid) noexcept
  : ManagedObject(oid)
{
}

ProxySnapshotWriter::~ProxySnapshotWriter()
{
}

IOResult ProxySnapshotWriter::write(const Byte *data, size_t size) noexcept
{
  IOResult r;
  r.error = 0;
  WriteToManagedWriter_return ret = writeToManagedWriter(oid_, data, size);
  if (!ret.r0)
  {
    r.error = ret.r1;
    r.size = 0;
    return r;
  }
  r.size = size;

  return r;
}

ProxySnapshotReader::ProxySnapshotReader(oid_t oid) noexcept
  : ManagedObject(oid)
{
}

ProxySnapshotReader::~ProxySnapshotReader()
{
}

IOResult ProxySnapshotReader::read(Byte *data, size_t size) noexcept
{
  IOResult r;
  r.error = 0;
  ReadFromManagedReader_return ret = readFromManagedReader(oid_, data, size);
  if (ret.r0 < 0)
  {
    r.error = ret.r1;
    r.size = 0;
    return r;
  } else {
    r.size = ret.r0;
  }
  return r;
}

DoneChan::DoneChan(oid_t oid) noexcept
  : ManagedObject(oid)
{
}

DoneChan::~DoneChan()
{
}

bool DoneChan::Closed() const noexcept
{
  return doneChanClosed(oid_);
}

SnapshotFileCollection::SnapshotFileCollection(oid_t oid) noexcept
  : ManagedObject(oid) {
}

void SnapshotFileCollection::AddFile(uint64_t fileID,
  std::string path, const Byte *metadata, size_t length) noexcept {
  addFileToSnapshotFileCollection(oid_, fileID, path, metadata, length);
}

CollectedFiles::CollectedFiles()
{
}

CollectedFiles::~CollectedFiles()
{
  for (auto &t : files_)
  {
    delete[] t.Metadata;
  }
}

void CollectedFiles::AddFile(uint64_t fileID,
  std::string path, const Byte *metadata, size_t length)
{
  SnapshotFile f;
  f.FileID = fileID;
  f.Filepath = path;
  f.Metadata = new Byte[length];
  std::memcpy(f.Metadata, metadata, length);
  f.Length = length;
  files_.push_back(f);
}

std::vector<SnapshotFile> CollectedFiles::GetFiles() const
{
  return files_;
}

}  // namespace dragonboat
