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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"context"
	"log"
	"time"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/tests"
	sm "github.com/lni/dragonboat/statemachine"
)

var nh *NodeHost
var ctx context.Context

func ExampleNewNodeHost() {
	// Let's say we want to put all LogDB's WAL data in a directory named wal,
	// everything else is stored in a directory named dragonboat. Assume the
	// RTT between nodes is 200 milliseconds, and the nodehost address is
	// myhostname:5012
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 200,
		RaftAddress:    "myhostname:5012",
	}
	// Creates a nodehost instance using the above NodeHostConfig instnace.
	nh, err := NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	log.Printf("NodeHost %s created\n", nh.RaftAddress())
}

func ExampleNodeHost_StartCluster() {
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 200,
		RaftAddress:    "myhostname:5012",
	}
	// Creates a nodehost instance using the above NodeHostConfig instnace.
	nh, err := NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	// config for raft
	rc := config.Config{
		NodeID:             1,
		ClusterID:          100,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10000,
		CompactionOverhead: 5000,
	}
	peers := make(map[uint64]string)
	peers[100] = "myhostname1:5012"
	peers[200] = "myhostname2:5012"
	peers[300] = "myhostname3:5012"
	// Use this NO-OP data store in this example
	NewStateMachine := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &tests.NoOP{}
	}
	if err := nh.StartCluster(peers, false, NewStateMachine, rc); err != nil {
		log.Fatalf("failed to add cluster, %v\n", err)
	}
}

func ExampleNodeHost_Propose() {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// Use NO-OP client session, cluster ID is 100
	// Check the example on the GetNewSession method to see how to use a
	// real client session object to make proposals.
	cs := nh.GetNoOPSession(100)
	// make a proposal with the proposal content "test-data", timeout is set to
	// 2000 milliseconds.
	rs, err := nh.Propose(cs, []byte("test-data"), 2000*time.Millisecond)
	if err != nil {
		// failed to start the proposal
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the proposal failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the proposal has been committed and applied
		// put the request state instance back to the recycle pool
	} else if s.Terminated() {
		// proposal terminated as the system is being shut down, time to exit
	}
	// note that s.Code == RequestRejected is not suppose to happen as we are
	// using a NO-OP client session in this example.
}

func ExampleNodeHost_ReadIndex() {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	data := make([]byte, 1024)
	rs, err := nh.ReadIndex(100, 2000*time.Millisecond)
	if err != nil {
		// ReadIndex failed to start
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the ReadIndex operation failed to complete before the deadline, maybe
		// retry the request
	} else if s.Completed() {
		// the ReadIndex operation completed. the local IStateMachine is ready to be
		// queried
		result, err := nh.ReadLocalNode(rs, data)
		if err != nil {
			return
		}
		// use query result here
		_ = result
	} else if s.Terminated() {
		// the ReadIndex operation terminated as the system is being shut down,
		// time to exit
	}
}

func ExampleNodeHost_RequestDeleteNode() {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// request node with ID 1 to be removed as a member node of raft cluster 100.
	// the third parameter is OrderID.
	rs, err := nh.RequestDeleteNode(100, 1, 0, 2000*time.Millisecond)
	if err != nil {
		// failed to start the membership change request
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the request failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the requested node has been removed from the raft cluster, ready to
		// remove the node from the NodeHost running at myhostname1:5012, e.g.
		// nh.RemoveCluster(100)
	} else if s.Terminated() {
		// request terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// request rejected as it is out of order. try again with the latest order
		// id value returned by NodeHost's GetClusterMembership() method.
	}
}

func ExampleNodeHost_RequestAddNode() {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// request node with ID 4 running at myhostname4:5012 to be added as a member
	// node of raft cluster 100. the fourth parameter is OrderID.
	rs, err := nh.RequestAddNode(100,
		4, "myhostname4:5012", 0, 2000*time.Millisecond)
	if err != nil {
		// failed to start the membership change request
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the request failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the requested new node has been added to the raft cluster, ready to
		// add the node to the NodeHost running at myhostname4:5012. run the
		// following code on the NodeHost running at myhostname4:5012 -
		//
		// NewStateMachine := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		//   return &tests.NoOP{}
		// }
		// rc := config.Config{
		//   NodeID:             4,
		//   ClusterID:          100,
		//   ElectionRTT:        5,
		//   HeartbeatRTT:       1,
		//   CheckQuorum:        true,
		//   SnapshotEntries:    10000,
		//   CompactionOverhead: 5000,
		// }
		// nh.StartCluster(nil, true, NewStateMachine, rc)
	} else if s.Terminated() {
		// request terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// request rejected as it is out of order. try again with the latest order
		// id value returned by NodeHost's GetClusterMembership() method.
	}
}

func ExampleNodeHost_GetNewSession() {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// Create a client session first, cluster ID is 100
	// Check the example on the GetNewSession method to see how to use a
	// real client session object to make proposals.
	cs, err := nh.GetNewSession(ctx, 100)
	if err != nil {
		// failed to get the client session, if it is a timeout error then try
		// again later.
		return
	}
	defer func() {
		if err := nh.CloseSession(ctx, cs); err != nil {
			log.Printf("close session failed %v\n", err)
		}
	}()
	// make a proposal with the proposal content "test-data", timeout is set to
	// 2000 milliseconds.
	rs, err := nh.Propose(cs, []byte("test-data"), 2000*time.Millisecond)
	if err != nil {
		// failed to start the proposal
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the proposal failed to complete before the deadline. maybe retry
		// the request with the same client session instance s.
		// on timeout, there is actually no guarantee on whether the proposed
		// entry has been applied or not, the idea is that when retrying with
		// the same proposal using the same client session instance, dragonboat
		// makes sure that the proposal is retried and it will be applied if
		// and only if it has not been previously applied.
	} else if s.Completed() {
		// the proposal has been committed and applied, call
		// s.ProposalCompleted() to notify the client session that the previous
		// request has been successfully completed. this makes the client
		// session ready to be used when you make the next proposal.
		cs.ProposalCompleted()
	} else if s.Terminated() {
		// proposal terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// client session s is not evicted from the server side, probably because
		// there are too many concurrent client sessions. in case you want to
		// strictly ensure that each proposal will never be applied twice, we
		// recommend to fail the client program. Note that this is highly unlikely
		// to happen.
		panic("client session already evicted")
	}
	//
	// now you can use the same client session instance s to make more proposals
	//
}
