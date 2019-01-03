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

#ifndef BINDING_INCLUDE_DRAGONBOAT_TYPES_H_
#define BINDING_INCLUDE_DRAGONBOAT_TYPES_H_

#define DISALLOW_COPY_MOVE_AND_ASSIGN(TypeName)     \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete;      \
TypeName(TypeName&&) = delete;                      \
TypeName& operator=(const TypeName&&) = delete

#endif  // BINDING_INCLUDE_DRAGONBOAT_TYPES_H_
