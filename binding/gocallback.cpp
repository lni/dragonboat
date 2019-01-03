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

#include "dragonboat/binding.h"
#include "dragonboat/dragonboat.h"

Membership *CreateMembership(size_t sz)
{
  Membership *m = new Membership;
  m->nodeIDList = new uint64_t[sz];
  m->nodeAddressList = new DBString[sz];
  m->nodeListLen = sz;
  m->index = 0;
  return m;
}

void AddClusterMember(Membership *m, uint64_t nodeID, char *addr, size_t len)
{
  DBString s;
  s.str = addr;
  s.len = len;
  m->nodeIDList[m->index] = nodeID;
  m->nodeAddressList[m->index] = s;
  m->index++;
}

void CPPCompleteHandler(void *event, int code, uint64_t result)
{
  dragonboat::Event *e = reinterpret_cast<dragonboat::Event *>(event);
  e->Set(code, result);
}

void RunIOService(void *iosp)
{
  dragonboat::IOService *i = reinterpret_cast<dragonboat::IOService *>(iosp);
  i->Run();
}
