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

package drummer

import (
	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
)

// some validation code used for self testing generated drummer requests
func validateNodeHostRequest(req pb.NodeHostRequest) {
	if req.Change.Type == pb.Request_ADD {
		if len(req.AddressList) != 1 {
			plog.Panicf("len(req.AddressList) != 1")
		}
	} else {
		if len(req.NodeIdList) != len(req.AddressList) {
			plog.Panicf("len(req.NodeIdList) != len(req.AddressList)")
		}
	}
	for _, nid := range req.NodeIdList {
		if nid == 0 {
			plog.Panicf("nid == 0")
		}
	}
	for _, addr := range req.AddressList {
		if len(addr) == 0 {
			plog.Panicf("len(addr) == 0 ")
		}
	}
	if len(req.RaftAddress) == 0 {
		plog.Panicf("len(req.RaftAddress) == 0")
	}
	if req.Change.Type == pb.Request_ADD ||
		req.Change.Type == pb.Request_DELETE ||
		req.Change.Type == pb.Request_KILL {
		if len(req.Change.Members) == 0 {
			plog.Panicf("len(req.Change.Members) == 0")
		}
		if req.Change.Members[0] == 0 {
			plog.Panicf("req.Change.Members[0] == 0")
		}
		if req.Change.ClusterId == 0 {
			plog.Panicf("req.Change.ClusterId == 0")
		}
	} else if req.Change.Type == pb.Request_CREATE {
		if req.InstantiateNodeId == 0 {
			plog.Panicf("req.InstantiateNodeId == 0")
		}
		if len(req.AppName) == 0 {
			plog.Panicf("len(req.AppName) == 0")
		}
		if len(req.NodeIdList) == 0 {
			plog.Panicf("len(req.NodeIdList) == 0")
		}
	} else if req.Change.Type == pb.Request_ADD {
		if len(req.AddressList) == 0 {
			plog.Panicf("len(req.AddressList) == 0")
		}
		if len(req.AddressList[0]) == 0 {
			plog.Panicf("len(req.AddressList[0]) == 0")
		}
	}
}
