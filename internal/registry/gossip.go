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
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/memberlist"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/utils"
	"github.com/lni/dragonboat/v3/logger"
)

var firstError = utils.FirstError
var plog = logger.GetLogger("registry")

type getClusterInfo func() []ClusterInfo

// GossipRegistry is a node registry backed by gossip. It is capable of
// supporting NodeHosts with dynamic RaftAddress values.
type GossipRegistry struct {
	nodes  *Registry
	gossip *gossipManager
}

// NewGossipRegistry creates a new GossipRegistry instance.
func NewGossipRegistry(nhid string, f getClusterInfo,
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
func (n *GossipRegistry) Add(clusterID uint64,
	nodeID uint64, target string) {
	n.nodes.Add(clusterID, nodeID, target)
}

// Remove removes the specified node from the registry.
func (n *GossipRegistry) Remove(clusterID uint64, nodeID uint64) {
	n.nodes.Remove(clusterID, nodeID)
}

// RemoveCluster removes the specified node from the registry.
func (n *GossipRegistry) RemoveCluster(clusterID uint64) {
	n.nodes.RemoveCluster(clusterID)
}

// Resolve returns the current RaftAddress and connection key of the specified
// node. It returns ErrUnknownTarget when the RaftAddress is unknown.
func (n *GossipRegistry) Resolve(clusterID uint64,
	nodeID uint64) (string, string, error) {
	target, key, err := n.nodes.Resolve(clusterID, nodeID)
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
	memberlist.ChannelEventDelegate
	ch      chan memberlist.NodeEvent
	stopper *syncutil.Stopper
	nodes   sync.Map
}

func newEventDelegate(s *syncutil.Stopper) *eventDelegate {
	ch := make(chan memberlist.NodeEvent, 10)
	ed := &eventDelegate{
		stopper:              s,
		ch:                   ch,
		ChannelEventDelegate: memberlist.ChannelEventDelegate{Ch: ch},
	}
	return ed
}

func (d *eventDelegate) start() {
	d.stopper.RunWorker(func() {
		for {
			select {
			case <-d.stopper.ShouldStop():
				return
			case e := <-d.ch:
				if e.Event == memberlist.NodeJoin || e.Event == memberlist.NodeUpdate {
					d.nodes.Store(e.Node.Name, string(e.Node.Meta))
				} else if e.Event == memberlist.NodeLeave {
					d.nodes.Delete(e.Node.Name)
				} else {
					panic("unknown event type")
				}
			}
		}
	})
}

type delegate struct {
	raftAddress    string
	getClusterInfo getClusterInfo
	view           *view
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte(d.raftAddress)
}
func (d *delegate) NotifyMsg(buf []byte) {
	d.view.updateFrom(buf)
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	if d.getClusterInfo != nil {
		d.view.update(d.getClusterInfo())
	}
	data := d.view.getGossipData(limit - overhead)
	if data == nil {
		return nil
	}

	result := make([][]byte, 1)
	result[0] = data
	return result
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	d.view.updateFrom(buf)
}

func (d *delegate) LocalState(join bool) []byte {
	if d.getClusterInfo != nil {
		d.view.update(d.getClusterInfo())
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
	nhConfig config.NodeHostConfig
	cfg      *memberlist.Config
	list     *memberlist.Memberlist
	ed       *eventDelegate
	view     *view
	stopper  *syncutil.Stopper
}

func newGossipManager(nhid string, f getClusterInfo,
	nhConfig config.NodeHostConfig) (*gossipManager, error) {
	stopper := syncutil.NewStopper()
	ed := newEventDelegate(stopper)
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
	if len(nhConfig.Gossip.AdvertiseAddress) > 0 {
		aAddr, aPort, err := parseAddress(nhConfig.Gossip.AdvertiseAddress)
		if err != nil {
			return nil, err
		}
		cfg.AdvertiseAddr = aAddr
		cfg.AdvertisePort = aPort
	}
	view := newView(nhConfig.GetDeploymentID())
	cfg.Delegate = &delegate{
		raftAddress:    nhConfig.RaftAddress,
		getClusterInfo: f,
		view:           view,
	}
	cfg.Events = ed

	list, err := memberlist.Create(cfg)
	if err != nil {
		plog.Errorf("failed to create memberlist, %v", err)
		return nil, err
	}
	seed := make([]string, 0, len(nhConfig.Gossip.Seed))
	seed = append(seed, nhConfig.Gossip.Seed...)
	g := &gossipManager{
		nhConfig: nhConfig,
		cfg:      cfg,
		list:     list,
		ed:       ed,
		view:     view,
		stopper:  stopper,
	}
	g.join(seed)
	g.ed.start()
	g.stopper.RunWorker(func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(g.list.Members()) > 1 {
					return
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
	var err error
	var cerr error
	if err = g.list.Leave(2 * time.Second); err != nil {
		err = errors.Wrapf(err, "leave memberlist failed")
	}
	if cerr = g.list.Shutdown(); cerr != nil {
		cerr = errors.Wrapf(cerr, "shutdown memberlist failed")
	}
	return firstError(err, cerr)
}

func (g *gossipManager) GetNodeHostRegistry() *NodeHostRegistry {
	return &NodeHostRegistry{
		view: g.view,
	}
}

func (g *gossipManager) GetRaftAddress(nhid string) (string, bool) {
	if g.cfg.Name == nhid {
		return g.nhConfig.RaftAddress, true
	}
	if v, ok := g.ed.nodes.Load(nhid); ok {
		return v.(string), true
	}
	return "", false
}

func (g *gossipManager) advertiseAddress() string {
	return g.list.LocalNode().Address()
}

func (g *gossipManager) numMembers() int {
	return g.list.NumMembers()
}
