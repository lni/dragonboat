// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

#ifndef BINDING_INCLUDE_DRAGONBOAT_MANAGED_H_
#define BINDING_INCLUDE_DRAGONBOAT_MANAGED_H_

#include <cstddef>
#include "dragonboat/types.h"

namespace dragonboat {

// oid_t is the id type used to identify Go managed object.
using oid_t = uint64_t;

class NodeHost;

// ManagedObject is the base class for Go managed objects.
class ManagedObject
{
 public:
  virtual ~ManagedObject();
 protected:
  ManagedObject() noexcept;
  explicit ManagedObject(oid_t oid) noexcept;
  oid_t OID() const noexcept;
  oid_t oid_;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(ManagedObject);
  friend class NodeHost;
};

}  // namespace dragonboat

#endif  // BINDING_INCLUDE_DRAGONBOAT_MANAGED_H_

