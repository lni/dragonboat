// Copyright 2018-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/memberlist"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/utils"
	"github.com/lni/dragonboat/v4/logger"
)

var firstError = utils.FirstError
var plog = logger.GetLogger("registry")

type getShardInfo func() []ShardInfo

type meta struct {
	RaftAddress string
	Data        []byte
}

func (m *meta) marshal() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	result := make([]byte, len(data)+2)
	binary.BigEndian.PutUint16(result, uint16(len(data)))
	copy(result[2:], data)
	return result
}

func (m *meta) unmarshal(data []byte) bool {
	if len(data) <= 2 {
		return false
	}
	sz := binary.BigEndian.Uint16(data)
	if sz > 512 {
		return false
	}
	data = data[:sz+2]
	buf := bytes.NewBuffer(data[2:])
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(m); err != nil {
		return false
	}
	return true
}

type metaStore struct {
	nodes sync.Map
}

func (m *metaStore) put(name string, md meta) {
	m.nodes.Store(name, md)
}

func (m *metaStore) get(name string) (meta, bool) {
	v, ok := m.nodes.Load(name)
	if ok {
		return v.(meta), true
	}
	return meta{}, false
}

func (m *metaStore) delete(name string) {
	m.nodes.Delete(name)
}

// GossipRegistry is a node registry backed by gossip. It is capable of
// supporting NodeHosts with dynamic RaftAddress values.
type GossipRegistry struct {
	nodes  *Registry
	gossip *gossipManager
}

// NewGossipRegistry creates a new GossipRegistry instance.
func NewGossipRegistry(nhid string, f getShardInfo,
	nhConfig config.NodeHostConfig, streamConnections uint64,
	v config.TargetValidator) (*GossipRegistry, error) {
	gossip, err := newGossipManager(nhid, f, nhConfig)
	if err != nil {
		return nil, err
	}
	r := &GossipRegistry{
		nodes:  NewNodeRegistry(streamConnections, v),
		gossip: gossip,
	}
	return r, nil
}

// GetNodeHostRegistry returns the NodeHostRegistry backed by gossip.
func (n *GossipRegistry) GetNodeHostRegistry() *NodeHostRegistry {
	return n.gossip.GetNodeHostRegistry()
}

// Close closes the GossipRegistry instance.
func (n *GossipRegistry) Close() error {
	return n.gossip.Close()
}

// AdvertiseAddress returns the advertise address of the gossip service.
func (n *GossipRegistry) AdvertiseAddress() string {
	return n.gossip.advertiseAddress()
}

// NumMembers returns the number of live nodes known by the gossip service.
func (n *GossipRegistry) NumMembers() int {
	return n.gossip.numMembers()
}

// Add adds a new node with its known NodeHostID to the registry.
func (n *GossipRegistry) Add(shardID uint64,
	replicaID uint64, target string) {
	n.nodes.Add(shardID, replicaID, target)
}

// Remove removes the specified node from the registry.
func (n *GossipRegistry) Remove(shardID uint64, replicaID uint64) {
	n.nodes.Remove(shardID, replicaID)
}

// RemoveShard removes the specified node from the registry.
func (n *GossipRegistry) RemoveShard(shardID uint64) {
	n.nodes.RemoveShard(shardID)
}

// Resolve returns the current RaftAddress and connection key of the specified
// node. It returns ErrUnknownTarget when the RaftAddress is unknown.
func (n *GossipRegistry) Resolve(shardID uint64,
	replicaID uint64) (string, string, error) {
	target, key, err := n.nodes.Resolve(shardID, replicaID)
	if err != nil {
		return "", "", err
	}
	addr, ok := n.gossip.GetRaftAddress(target)
	if ok {
		return addr, key, nil
	}
	return "", "", ErrUnknownTarget
}

type eventDelegate struct {
	ed      *sliceEventDelegate
	stopper *syncutil.Stopper
	store   *metaStore
}

func newEventDelegate(s *syncutil.Stopper, store *metaStore) *eventDelegate {
	ed := &eventDelegate{
		stopper: s,
		store:   store,
		ed:      newSliceEventDelegate(),
	}
	return ed
}

func (d *eventDelegate) handle() {
	//for {
	events := d.ed.get()
	if len(events) == 0 {
		return
	}
	for _, e := range events {
		if e.Event == memberlist.NodeJoin || e.Event == memberlist.NodeUpdate {
			var m meta
			if m.unmarshal(e.Node.Meta) {
				d.store.put(e.Node.Name, m)
			}
		} else if e.Event == memberlist.NodeLeave {
			d.store.delete(e.Node.Name)
		} else {
			panic("unknown event type")
		}
	}
	//}
}

func (d *eventDelegate) start() {
	d.stopper.RunWorker(func() {
		for {
			select {
			case <-d.stopper.ShouldStop():
				return
			case <-d.ed.ch:
				d.handle()
			}
		}
	})
}

type delegate struct {
	getShardInfo getShardInfo
	meta         meta
	view         *view
}

func (d *delegate) NodeMeta(limit int) []byte {
	m := d.meta.marshal()
	if len(m) > limit {
		panic("meta message is too big")
	}
	return m
}

func (d *delegate) NotifyMsg(buf []byte) {
	d.view.updateFrom(buf)
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	if d.getShardInfo != nil {
		d.view.update(toShardViewList(d.getShardInfo()))
	}
	data := d.view.getGossipData(limit - overhead)
	if data == nil {
		return nil
	}
	if len(data) > limit-overhead {
		panic("broadcast message is too big")
	}

	result := make([][]byte, 1)
	result[0] = data
	return result
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	d.view.updateFrom(buf)
}

func (d *delegate) LocalState(join bool) []byte {
	if d.getShardInfo != nil {
		d.view.update(toShardViewList(d.getShardInfo()))
	}
	return d.view.getFullSyncData()
}

func parseAddress(addr string) (string, int, error) {
	host, sp, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(sp, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(port), nil
}

type gossipManager struct {
	nhConfig     config.NodeHostConfig
	cfg          *memberlist.Config
	list         *memberlist.Memberlist
	ed           *eventDelegate
	view         *view
	store        *metaStore
	stopper      *syncutil.Stopper
	eventStopper *syncutil.Stopper
}

func newGossipManager(nhid string, f getShardInfo,
	nhConfig config.NodeHostConfig) (*gossipManager, error) {
	eventStopper := syncutil.NewStopper()
	store := &metaStore{}
	ed := newEventDelegate(eventStopper, store)
	cfg := memberlist.DefaultWANConfig()
	cfg.Logger = newGossipLogWrapper()
	cfg.Name = nhid
	cfg.PushPullInterval = 500 * time.Millisecond
	cfg.GossipInterval = 250 * time.Millisecond
	cfg.GossipNodes = 6
	cfg.UDPBufferSize = 32 * 1024
	if nhConfig.Expert.TestGossipProbeInterval > 0 {
		plog.Infof("gossip probe interval set to %s",
			nhConfig.Expert.TestGossipProbeInterval)
		cfg.ProbeInterval = nhConfig.Expert.TestGossipProbeInterval
	}
	bindAddr, bindPort, err := parseAddress(nhConfig.Gossip.BindAddress)
	if err != nil {
		return nil, err
	}
	cfg.BindAddr = bindAddr
	cfg.BindPort = bindPort
	plog.Infof("gossip bind address %s port %d", cfg.BindAddr, cfg.BindPort)
	if len(nhConfig.Gossip.AdvertiseAddress) > 0 {
		aAddr, aPort, err := parseAddress(nhConfig.Gossip.AdvertiseAddress)
		if err != nil {
			return nil, err
		}
		cfg.AdvertiseAddr = aAddr
		cfg.AdvertisePort = aPort
		plog.Infof("gossip advertise address %s port %d", aAddr, aPort)
	}
	view := newView(nhConfig.GetDeploymentID())
	meta := meta{
		RaftAddress: nhConfig.RaftAddress,
		Data:        nhConfig.Gossip.Meta,
	}
	cfg.Delegate = &delegate{
		meta:         meta,
		getShardInfo: f,
		view:         view,
	}
	cfg.Events = ed.ed

	list, err := memberlist.Create(cfg)
	if err != nil {
		plog.Errorf("failed to create memberlist, %v", err)
		return nil, err
	}
	seed := make([]string, 0, len(nhConfig.Gossip.Seed))
	seed = append(seed, nhConfig.Gossip.Seed...)
	g := &gossipManager{
		nhConfig:     nhConfig,
		cfg:          cfg,
		list:         list,
		ed:           ed,
		view:         view,
		store:        store,
		stopper:      syncutil.NewStopper(),
		eventStopper: eventStopper,
	}
	// eventDelegate must be started first, otherwise join() could be blocked
	// on a large cluster
	g.ed.start()
	g.join(seed)
	g.stopper.RunWorker(func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(g.list.Members()) > 1 {
					continue
				}
				g.join(seed)
			case <-g.stopper.ShouldStop():
				return
			}
		}
	})
	return g, nil
}

func (g *gossipManager) join(seed []string) {
	if count, err := g.list.Join(seed); err != nil {
		plog.Errorf("failed to join the gossip group, %v", err)
	} else {
		plog.Infof("connected to %d gossip nodes", count)
	}
}

func (g *gossipManager) Close() error {
	g.stopper.Stop()
	if err := g.list.Leave(time.Second); err != nil {
		plog.Errorf("memberlist leave failed: %v", err)
	}
	if err := g.list.Shutdown(); err != nil {
		return errors.Wrapf(err, "shutdown memberlist failed")
	}
	g.eventStopper.Stop()
	return nil
}

func (g *gossipManager) GetNodeHostRegistry() *NodeHostRegistry {
	return &NodeHostRegistry{
		view:  g.view,
		store: g.store,
	}
}

func (g *gossipManager) GetRaftAddress(nhid string) (string, bool) {
	if g.cfg.Name == nhid {
		return g.nhConfig.RaftAddress, true
	}
	if v, ok := g.ed.store.get(nhid); ok {
		return v.RaftAddress, true
	}
	return "", false
}

func (g *gossipManager) advertiseAddress() string {
	return g.list.LocalNode().Address()
}

func (g *gossipManager) numMembers() int {
	return g.list.NumMembers()
}
