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

/*
Package binding contains utility functions and structs for making language
bindings.

You can safely ignore this package when working on a dragonboat based Go
application.
*/
package main

//#cgo CFLAGS: -I./include -O3
//#cgo CXXFLAGS: -std=c++11 -O3 -I./include
//#include "dragonboat/binding.h"
import "C"
import (
	"context"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/cpp"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/logger"
)

func init() {
	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
}

// CompleteHandlerType defines the completion handler type.
type CompleteHandlerType int

const (
	// CompleteHandlerCPP is the completion handler type for C++.
	CompleteHandlerCPP = iota
	// CompleteHandlerPython is the completion handler type for Python.
	CompleteHandlerPython
)

var completeHandlerPool = &sync.Pool{}

// required for the .so module built in c-shared mode.
func main() {}

func addManagedObject(object interface{}) uint64 {
	return cpp.AddManagedObject(object)
}

func getManagedObject(oid uint64) (interface{}, bool) {
	return cpp.GetManagedObject(oid)
}

func removeManagedObject(objectID uint64) {
	cpp.RemoveManagedObject(objectID)
}

func getNodeHost(oid uint64) *dragonboat.NodeHost {
	v, ok := getManagedObject(oid)
	if !ok {
		panic("nodehost object not found")
	}
	return v.(*dragonboat.NodeHost)
}

func getSession(oid uint64) *client.Session {
	v, ok := getManagedObject(oid)
	if !ok {
		panic("client session object not found")
	}
	return v.(*client.Session)
}

func getRequestState(oid uint64) *dragonboat.RequestState {
	v, ok := getManagedObject(oid)
	if !ok {
		panic("request state not found")
	}
	return v.(*dragonboat.RequestState)
}

// GetManagedObjectCount returns the count of the managed object.
//export GetManagedObjectCount
func GetManagedObjectCount() uint64 {
	return cpp.GetManagedObjectCount()
}

// GetInterestedGoroutines returns a managed collection of existing goroutines.
//export GetInterestedGoroutines
func GetInterestedGoroutines() uint64 {
	ig := leaktest.GetInterestedGoroutines()
	return addManagedObject(ig)
}

// AssertNoGoroutineLeak checks whether there is any leaked goroutine. The
// program will panic if there is any identified leaked goroutine.
//export AssertNoGoroutineLeak
func AssertNoGoroutineLeak(oid uint64) {
	defer RemoveManagedObject(oid)
	init, ok := getManagedObject(oid)
	if !ok {
		panic("failed to get the init goroutine collection")
	}
	leaktest.AssertNoGoroutineLeak(init.(map[int64]string))
}

// TestLatency is a simple function used for measuring C to Go function call
// latency.
//export TestLatency
func TestLatency(v uint64) uint64 {
	return v + 1
}

// JoinIOServiceThreads joins IO Service threads managed by Go.
//export JoinIOServiceThreads
func JoinIOServiceThreads(oid uint64) {
	stopper, ok := getManagedObject(oid)
	if !ok {
		panic("failed to get io service handler")
	}
	s := stopper.(*syncutil.Stopper)
	s.Stop()
}

// RunIOServiceInGo runs IO service using Go managed thread.
//export RunIOServiceInGo
func RunIOServiceInGo(iosp unsafe.Pointer, count int) uint64 {
	stopper := syncutil.NewStopper()
	for i := 0; i < count; i++ {
		stopper.RunWorker(func() {
			C.RunIOService(iosp)
		})
	}
	return addManagedObject(stopper)
}

// RemoveManagedObject removes the specified Go object from the system.
//export RemoveManagedObject
func RemoveManagedObject(rsoid uint64) {
	removeManagedObject(rsoid)
}

// SetLogLevel sets the log level of the specified package.
//export SetLogLevel
func SetLogLevel(packageName C.DBString, level int) int {
	pkgName := charArrayToString(packageName.str, packageName.len)
	logger.GetLogger(pkgName).SetLevel(logger.LogLevel(level))
	return 0
}

// SelectOnRequestStateForMembershipChange selects on the RequestState and
// wait until the CompleteC channel to be signaled.
//export SelectOnRequestStateForMembershipChange
func SelectOnRequestStateForMembershipChange(rsoid uint64) int {
	rs := getRequestState(rsoid)
	var err error
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			err = nil
		} else if r.Timeout() {
			err = dragonboat.ErrTimeout
		} else if r.Terminated() {
			err = dragonboat.ErrClusterClosed
		} else if r.Rejected() {
			err = dragonboat.ErrRejected
		} else {
			panic("unknown code")
		}
	}
	return getErrorCode(err)
}

// SessionProposalCompleted marks the client session instance specified
// by the object id value csoid as proposal completed. This makes the client
// session ready to be used for further proposals.
//export SessionProposalCompleted
func SessionProposalCompleted(csoid uint64) {
	cs := getSession(csoid)
	cs.ProposalCompleted()
}

// CreateSession creates a new client session object for the specified
// cluster.
//export CreateSession
func CreateSession(clusterID uint64) uint64 {
	cs := client.NewSession(clusterID, random.LockGuardedRand)
	return addManagedObject(cs)
}

// CreateNoOPSession creates a new NoOP client session object ready
// to be used for making proposals.
//export CreateNoOPSession
func CreateNoOPSession(clusterID uint64) uint64 {
	cs := client.NewNoOPSession(clusterID, random.LockGuardedRand)
	return addManagedObject(cs)
}

// NewNodeHost creates a new NodeHost instance and return the object id of the
// new NodeHost instance.
//export NewNodeHost
func NewNodeHost(cfg C.NodeHostConfig) uint64 {
	completeHandlerPool.New = func() interface{} {
		v := &cppCompleteHandler{
			pool: completeHandlerPool,
		}
		return v
	}
	c := &config.NodeHostConfig{
		DeploymentID:   uint64(cfg.DeploymentID),
		WALDir:         charArrayToString(cfg.WALDir.str, cfg.WALDir.len),
		NodeHostDir:    charArrayToString(cfg.NodeHostDir.str, cfg.NodeHostDir.len),
		RTTMillisecond: uint64(cfg.RTTMillisecond),
		RaftAddress:    charArrayToString(cfg.RaftAddress.str, cfg.RaftAddress.len),
		APIAddress:     charArrayToString(cfg.APIAddress.str, cfg.APIAddress.len),
		MutualTLS:      cboolToBool(cfg.MutualTLS),
		CAFile:         charArrayToString(cfg.CAFile.str, cfg.CAFile.len),
		CertFile:       charArrayToString(cfg.CertFile.str, cfg.CertFile.len),
		KeyFile:        charArrayToString(cfg.KeyFile.str, cfg.KeyFile.len),
	}
	nh := dragonboat.NewNodeHost(*c)
	return addManagedObject(nh)
}

// StopNodeHost stops the specified NodeHost instance.
//export StopNodeHost
func StopNodeHost(oid uint64) {
	nh := getNodeHost(oid)
	nh.Stop()
}

// NodeHostStartCluster adds a new raft cluster node to be managed by the
// specified NodeHost and start the node to make it ready to accept incoming
// requests.
//export NodeHostStartCluster
func NodeHostStartCluster(oid uint64,
	nodeIDList *C.uint64_t, nodeAddressList *C.DBString, nodeListLen C.size_t,
	joinPeer C.char, pluginFilename C.DBString, cfg C.RaftConfig) int {
	c := config.Config{
		NodeID:              uint64(cfg.NodeID),
		ClusterID:           uint64(cfg.ClusterID),
		IsObserver:          cboolToBool(cfg.IsObserver),
		CheckQuorum:         cboolToBool(cfg.CheckQuorum),
		Quiesce:             cboolToBool(cfg.Quiesce),
		ElectionRTT:         uint64(cfg.ElectionRTT),
		HeartbeatRTT:        uint64(cfg.HeartbeatRTT),
		SnapshotEntries:     uint64(cfg.SnapshotEntries),
		CompactionOverhead:  uint64(cfg.CompactionOverhead),
		OrderedConfigChange: cboolToBool(cfg.OrderedConfigChange),
	}
	join := charToBool(joinPeer)
	pfn := charArrayToString(pluginFilename.str, pluginFilename.len)
	peers := make(map[uint64]string)
	var nap unsafe.Pointer
	var nidp unsafe.Pointer
	nap = (unsafe.Pointer)(nodeAddressList)
	nidp = (unsafe.Pointer)(nodeIDList)
	addrListSz := unsafe.Sizeof(*nodeAddressList)
	idListSz := unsafe.Sizeof(*nodeIDList)
	for i := 0; i < int(nodeListLen); i++ {
		curNodeAddressPointer := (*C.DBString)(unsafe.Pointer(uintptr(nap) + addrListSz*uintptr(i)))
		curNodeIDListPointer := (*C.uint64_t)(unsafe.Pointer(uintptr(nidp) + idListSz*uintptr(i)))
		nodeAddress := charArrayToString(curNodeAddressPointer.str, curNodeAddressPointer.len)
		nodeID := uint64(*curNodeIDListPointer)
		peers[nodeID] = nodeAddress
	}
	nh := getNodeHost(oid)
	err := nh.StartClusterUsingPlugin(peers, join, pfn, c)
	return getErrorCode(err)
}

// NodeHostStartClusterFromFactory adds a new raft cluster node to be managed by the
// specified NodeHost and start the node to make it ready to accept incoming
// requests.
//export NodeHostStartClusterFromFactory
func NodeHostStartClusterFromFactory(oid uint64,
	nodeIDList *C.uint64_t, nodeAddressList *C.DBString, nodeListLen C.size_t,
	joinPeer C.char, cfactory C.uint64_t, cfg C.RaftConfig) int {
	c := config.Config{
		NodeID:              uint64(cfg.NodeID),
		ClusterID:           uint64(cfg.ClusterID),
		IsObserver:          cboolToBool(cfg.IsObserver),
		CheckQuorum:         cboolToBool(cfg.CheckQuorum),
		Quiesce:             cboolToBool(cfg.Quiesce),
		ElectionRTT:         uint64(cfg.ElectionRTT),
		HeartbeatRTT:        uint64(cfg.HeartbeatRTT),
		SnapshotEntries:     uint64(cfg.SnapshotEntries),
		CompactionOverhead:  uint64(cfg.CompactionOverhead),
		OrderedConfigChange: cboolToBool(cfg.OrderedConfigChange),
	}
	join := charToBool(joinPeer)
	peers := make(map[uint64]string)
	factory := uint64(cfactory)
	var nap unsafe.Pointer
	var nidp unsafe.Pointer
	nap = (unsafe.Pointer)(nodeAddressList)
	nidp = (unsafe.Pointer)(nodeIDList)
	addrListSz := unsafe.Sizeof(*nodeAddressList)
	idListSz := unsafe.Sizeof(*nodeIDList)
	for i := 0; i < int(nodeListLen); i++ {
		curNodeAddressPointer := (*C.DBString)(unsafe.Pointer(uintptr(nap) + addrListSz*uintptr(i)))
		curNodeIDListPointer := (*C.uint64_t)(unsafe.Pointer(uintptr(nidp) + idListSz*uintptr(i)))
		nodeAddress := charArrayToString(curNodeAddressPointer.str, curNodeAddressPointer.len)
		nodeID := uint64(*curNodeIDListPointer)
		peers[nodeID] = nodeAddress
	}
	nh := getNodeHost(oid)
	err := nh.StartClusterUsingFactory(peers, join, factory, c)
	return getErrorCode(err)
}

// NodeHostStopCluster removes the specified raft cluster node from the
// NodeHost instance and stops the running node.
//export NodeHostStopCluster
func NodeHostStopCluster(oid uint64, clusterID uint64) int {
	nh := getNodeHost(oid)
	err := nh.StopCluster(clusterID)
	return getErrorCode(err)
}

// NodeHostGetNewSession creates a new client session instance ready to
// be used for making proposals.
//export NodeHostGetNewSession
func NodeHostGetNewSession(oid uint64, timeout uint64,
	clusterID uint64) (uint64, int) {
	nh := getNodeHost(oid)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeout)*time.Millisecond)
	defer cancel()
	cs, err := nh.GetNewSession(ctx, clusterID)
	if err != nil {
		return 0, getErrorCode(err)
	}
	csoid := addManagedObject(cs)
	return csoid, getErrorCode(err)
}

// NodeHostCloseSession closes the specified client session instance.
//export NodeHostCloseSession
func NodeHostCloseSession(oid uint64, timeout uint64, csoid uint64) int {
	nh := getNodeHost(oid)
	cs := getSession(csoid)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeout)*time.Millisecond)
	defer cancel()
	err := nh.CloseSession(ctx, cs)
	return getErrorCode(err)
}

// NodeHostSyncPropose makes a new proposal on the specified NodeHost instance.
//export NodeHostSyncPropose
func NodeHostSyncPropose(oid uint64, timeout uint64,
	csoid uint64, csupdate bool,
	buf *C.uchar, len C.size_t) (uint64, int) {
	nh := getNodeHost(oid)
	cs := getSession(csoid)
	if csupdate {
		cs.ProposalCompleted()
	}
	cmd := ucharToByte(buf, len)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeout)*time.Millisecond)
	defer cancel()
	v, err := nh.SyncPropose(ctx, cs, cmd)
	return v, getErrorCode(err)
}

type cppCompleteHandler struct {
	waitable unsafe.Pointer
	pool     *sync.Pool
}

func (h *cppCompleteHandler) Notify(result dragonboat.RequestResult) {
	if h.waitable == nil {
		panic("h.waitable == nul")
	}
	C.CPPCompleteHandler(h.waitable, C.int(result.GetCode()),
		C.uint64_t(result.GetResult()))
}

func (h *cppCompleteHandler) Release() {
	if h.pool != nil {
		h.waitable = nil
		h.pool.Put(h)
	}
}

// NodeHostPropose makes a new async proposal.
//export NodeHostPropose
func NodeHostPropose(oid uint64, timeout uint64, csoid uint64,
	csupdate bool, prepareForProposal bool,
	buf *C.uchar, sz C.size_t, waitable unsafe.Pointer,
	handlerType CompleteHandlerType) (uint64, int) {
	nh := getNodeHost(oid)
	cs := getSession(csoid)
	if csupdate && prepareForProposal {
		panic("both csupdate && prepareForProposal are set")
	}
	if prepareForProposal {
		cs.PrepareForPropose()
	}
	if csupdate {
		cs.ProposalCompleted()
	}
	if !cs.ValidForProposal(cs.ClusterID) {
		panic("client session not valid for making proposal")
	}
	cmd := ucharToByte(buf, sz)
	var handler *cppCompleteHandler
	if handlerType == CompleteHandlerCPP {
		handler = completeHandlerPool.Get().(*cppCompleteHandler)
		handler.waitable = waitable
	} else {
		panic("not supported type")
	}
	_, err := nh.ProposeCH(cs,
		cmd, handler, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	//req.Release()
	return 0, getErrorCode(err)
}

// NodeHostProposeSession makes a asynchronous proposal on the specified
// cluster for client session related operation.
//export NodeHostProposeSession
func NodeHostProposeSession(oid uint64, timeout uint64, csoid uint64,
	forRegisteration bool, forUnregisteration bool,
	waitable unsafe.Pointer, handlerType CompleteHandlerType) (uint64, int) {
	nh := getNodeHost(oid)
	cs := getSession(csoid)
	if forRegisteration && forUnregisteration {
		panic("both forRegisteration && forUnregisteration both set")
	}
	if !forRegisteration && !forUnregisteration {
		panic("forRegisteration && forUnregisteration nothing set")
	}
	if forRegisteration {
		cs.PrepareForRegister()
	}
	if forUnregisteration {
		cs.PrepareForUnregister()
	}
	var handler *cppCompleteHandler
	if handlerType == CompleteHandlerCPP {
		handler = completeHandlerPool.Get().(*cppCompleteHandler)
		handler.waitable = waitable
	} else {
		panic("not supported type")
	}
	_, err := nh.ProposeSessionCH(cs,
		handler, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	return 0, getErrorCode(err)
}

// NodeHostSyncRead makes a linearizable read on the specified
// NodeHost instance.
//export NodeHostSyncRead
func NodeHostSyncRead(oid uint64, timeout uint64, clusterID uint64,
	queryBuf *C.uchar, queryLen C.size_t,
	resultBuf *C.uchar, resultLen C.size_t) (int, int) {
	nh := getNodeHost(oid)
	query := ucharToByte(queryBuf, queryLen)
	result := ucharToByte(resultBuf, resultLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeout)*time.Millisecond)
	defer cancel()
	r, err := nh.SyncRead(ctx, clusterID, query)
	if err != nil {
		return getErrorCode(err), 0
	}
	if len(r) > int(resultLen) {
		return int(C.ErrResultBufferTooSmall), 0
	}
	if copy(result, r) != len(r) {
		panic("failed to copy buffer")
	}
	return getErrorCode(err), len(r)
}

// NodeHostReadIndex starts the ReadIndex protocol to get ready for a
// linearizable read.
//export NodeHostReadIndex
func NodeHostReadIndex(oid uint64,
	timeout uint64, clusterID uint64, waitable unsafe.Pointer,
	handlerType CompleteHandlerType) (uint64, int) {
	return readIndex(oid, timeout, clusterID, waitable, handlerType)
}

func readIndex(oid uint64,
	timeout uint64, clusterID uint64, waitable unsafe.Pointer,
	handlerType CompleteHandlerType) (uint64, int) {
	nh := getNodeHost(oid)
	var handler *cppCompleteHandler
	if handlerType == CompleteHandlerCPP {
		handler = completeHandlerPool.Get().(*cppCompleteHandler)
		handler.waitable = waitable
	} else {
		panic("not supported type")
	}
	_, err := nh.ReadIndexCH(clusterID,
		handler, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	return 0, getErrorCode(err)
}

// NodeHostReadLocal makes a local read on the specified StateMachine.
//export NodeHostReadLocal
func NodeHostReadLocal(oid uint64, clusterID uint64,
	queryBuf *C.uchar, queryLen C.size_t,
	resultBuf *C.uchar, resultLen C.size_t) (int, int) {
	nh := getNodeHost(oid)
	query := ucharToByte(queryBuf, queryLen)
	result := ucharToByte(resultBuf, resultLen)
	r, err := nh.ReadLocal(clusterID, query)
	if err != nil {
		return getErrorCode(err), 0
	}
	if len(r) > int(resultLen) {
		return int(C.ErrResultBufferTooSmall), 0
	}
	if copy(result, r) != len(r) {
		panic("failed to copy buffer")
	}
	return getErrorCode(err), len(r)
}

// NodeHostRequestAddNode requests the specified new node to be added to the
// specified raft cluster.
//export NodeHostRequestAddNode
func NodeHostRequestAddNode(oid uint64, timeout uint64, clusterID uint64,
	nodeID uint64, url C.DBString, orderID uint64) (uint64, int) {
	nh := getNodeHost(oid)
	nodeURL := charArrayToString(url.str, url.len)
	rs, err := nh.RequestAddNode(clusterID,
		nodeID, nodeURL, orderID, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	return addManagedObject(rs), getErrorCode(err)
}

// NodeHostRequestDeleteNode requests the specified node to be removed from the
// specified raft cluster.
//export NodeHostRequestDeleteNode
func NodeHostRequestDeleteNode(oid uint64,
	timeout uint64, clusterID uint64,
	nodeID uint64, orderID uint64) (uint64, int) {
	nh := getNodeHost(oid)
	rs, err := nh.RequestDeleteNode(clusterID,
		nodeID, orderID, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	return addManagedObject(rs), getErrorCode(err)
}

// NodeHostRequestAddObserver requests the specified new node to be added to
// the specified cluster as observer.
//export NodeHostRequestAddObserver
func NodeHostRequestAddObserver(oid uint64, timeout uint64, clusterID uint64,
	nodeID uint64, url C.DBString, orderID uint64) (uint64, int) {
	nh := getNodeHost(oid)
	nodeURL := charArrayToString(url.str, url.len)
	rs, err := nh.RequestAddObserver(clusterID,
		nodeID, nodeURL, orderID, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, getErrorCode(err)
	}
	return addManagedObject(rs), getErrorCode(err)
}

// NodeHostRequestLeaderTransfer request to transfer leadership to the
// specified target node on the specified cluster.
//export NodeHostRequestLeaderTransfer
func NodeHostRequestLeaderTransfer(oid uint64,
	clusterID uint64, targetNodeID uint64) int {
	nh := getNodeHost(oid)
	err := nh.RequestLeaderTransfer(clusterID, targetNodeID)
	return getErrorCode(err)
}

// NodeHostGetClusterMembership returns the membership of the specified cluster.
//export NodeHostGetClusterMembership
func NodeHostGetClusterMembership(oid uint64, clusterID uint64,
	timeout uint64) (*C.Membership, uint64, int) {
	nh := getNodeHost(oid)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(timeout)*time.Millisecond)
	defer cancel()
	membership, err := nh.GetClusterMembership(ctx, clusterID)
	if err != nil {
		return nil, 0, getErrorCode(err)
	}
	if len(membership.Nodes) == 0 {
		return nil, 0, getErrorCode(nil)
	}
	members := C.CreateMembership(C.size_t(len(membership.Nodes)))
	for nid, addr := range membership.Nodes {
		addrData := []byte(addr)
		C.AddClusterMember(members, C.uint64_t(nid),
			(*C.char)(unsafe.Pointer(&addrData[0])), C.size_t(len(addrData)))
	}
	return members, membership.ConfigChangeID, getErrorCode(nil)
}

// NodeHostGetLeaderID returns the leader ID of the specified cluster.
//export NodeHostGetLeaderID
func NodeHostGetLeaderID(oid uint64, clusterID uint64) (uint64, bool, int) {
	nh := getNodeHost(oid)
	leaderID, valid, err := nh.GetLeaderID(clusterID)
	return leaderID, valid, getErrorCode(err)
}
