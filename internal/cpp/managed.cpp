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

#include "_cgo_export.h"
#include "dragonboat/managed.h"

namespace dragonboat {

ManagedObject::ManagedObject() noexcept
  : oid_(0)
{
}

ManagedObject::ManagedObject(oid_t oid) noexcept
  : oid_(oid)
{
}

ManagedObject::~ManagedObject()
{
  if (oid_)
  {
    RemoveManagedGoObject(oid_);
  }
}

oid_t ManagedObject::OID() const noexcept
{
  return oid_;
}

}  // namespace dragonboat
