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
Package drummer implements a reference Master server and client. They can manage
and monitor large number of NodeHosts distributed across the network. Raft nodes
running on failed NodeHost instances can be detected and replaced automatically
by Drummer servers, thus bring in high availability to the system.
*/
package drummer

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/client"
	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/compression"
	"github.com/lni/dragonboat/internal/utils/envutil"
	"github.com/lni/dragonboat/internal/utils/lang"
	"github.com/lni/dragonboat/internal/utils/netutil"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/internal/utils/stringutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/logger"
)

var (
	plog               = logger.GetLogger("drummer")
	errSessionNotReady = errors.New("client session not ready")
)

var (
	loopIntervalFactor   = settings.Soft.DrummerLoopIntervalFactor
	maxServerMsgSize     = int(settings.Soft.MaxDrummerServerMsgSize)
	tickIntervalSecond   = uint64(5)
	raftTimeoutSecond    = uint64(10)
	sessionTimeoutSecond = uint64(3)
	configFilename       = "dragonboat-drummer.json"
	// NodeHostInfoReportSecond defines how often should the NodeHost reports its
	// full details to drummer servers.
	NodeHostInfoReportSecond = settings.Soft.NodeHostInfoReportSecond
	loopIntervalSecond       = NodeHostInfoReportSecond * loopIntervalFactor
)

type sessionUser struct {
	nh      *dragonboat.NodeHost
	session *client.Session
}

func (c *sessionUser) getSession(ctx context.Context) *client.Session {
	if c.session == nil {
		rctx, cancel := context.WithTimeout(ctx,
			time.Duration(sessionTimeoutSecond)*time.Second)
		cs, err := c.nh.GetNewSession(rctx, defaultClusterID)
		cancel()
		if err != nil {
			return nil
		}
		c.session = cs
	}
	return c.session
}

func (c *sessionUser) resetSession(ctx context.Context) {
	if c.session == nil {
		return
	}
	cs := c.session
	c.session = nil
	rctx, cancel := context.WithTimeout(ctx,
		time.Duration(sessionTimeoutSecond)*time.Second)
	if err := c.nh.CloseSession(rctx, cs); err != nil {
		plog.Errorf("failed to close session %v", err)
	}
	cancel()
}

func (c *sessionUser) clientProposalCompleted() {
	if c.session == nil {
		plog.Panicf("no client session available")
	}
	c.session.ProposalCompleted()
}

// Drummer is the main struct used by the Drummer server
type Drummer struct {
	nh              *dragonboat.NodeHost
	stopper         *syncutil.Stopper
	server          *server
	scheduler       *scheduler
	deploymentID    uint64
	grpcHost        string
	grpcServer      *grpc.Server
	electionManager *electionManager
	ctx             context.Context
	cancel          context.CancelFunc
	*sessionUser
}

// NewDrummer creates a new Drummer instance.
func NewDrummer(nh *dragonboat.NodeHost, grpcHost string) *Drummer {
	if !stringutil.IsValidAddress(grpcHost) {
		plog.Panicf("invalid drummer grpc address %s", grpcHost)
	}
	randSrc := random.NewLockedRand()
	server := newDrummerServer(nh, randSrc)
	config := GetClusterConfig()
	stopper := syncutil.NewStopper()
	d := Drummer{
		nh:              nh,
		stopper:         stopper,
		server:          server,
		scheduler:       newScheduler(server, config),
		grpcHost:        grpcHost,
		electionManager: newElectionManager(stopper, server, randSrc),
	}
	d.sessionUser = &sessionUser{
		nh: nh,
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	return &d
}

// Start starts the Drummer server.
func (d *Drummer) Start() {
	d.stopper.RunWorker(func() {
		d.drummerWorker()
	})
}

// Stop gracefully stops the Drummer server.
func (d *Drummer) Stop() {
	plog.Infof("drummer's server ctx is going to be stopped")
	d.cancel()
	d.electionManager.stop()
	d.stopper.Stop()
	addr := d.nh.RaftAddress()
	plog.Debugf("going to stop the election manager, %s", addr)
	plog.Debugf("election manager stopped on %s", addr)
	if d.grpcServer != nil {
		plog.Debugf("grpc server will be stopped on %s", addr)
		d.grpcServer.Stop()
		plog.Debugf("grpc server stopped on %s", addr)
	}
}

func (d *Drummer) drummerWorker() {
	// this will not return until the deployment id is set.
	d.setDeploymentID()
	select {
	case <-d.stopper.ShouldStop():
		return
	default:
	}
	plog.Infof("going to start the Drummer API server")
	// now deployment id is ready, start the DrummerAPI so it can handle
	// incoming requests.
	d.startDrummerRPCServer()
	// start the main loop which will never return until stop is called
	d.drummerMain()
}

// GetClusterConfig returns the configuration used by Raft clusters managed
// by the drummer server.
func GetClusterConfig() pb.Config {
	fpList := make([]string, 0)
	for _, dir := range envutil.GetConfigDirs() {
		fp := filepath.Join(dir, configFilename)
		fpList = append(fpList, fp)
	}

	return getClusterConfig(fpList)
}

// GetClusterConfigFromFile returns the DrummerConfig read from the specified
// configuration file.
func GetClusterConfigFromFile(fp string) pb.Config {
	return getClusterConfig([]string{fp})
}

func getClusterConfig(fpList []string) pb.Config {
	directories := envutil.GetConfigDirs()
	for _, dir := range directories {
		fp := filepath.Join(dir, configFilename)
		f, err := os.Open(filepath.Clean(fp))
		if err != nil {
			continue
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
		data, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		var cc pb.Config
		err = json.Unmarshal(data, &cc)
		if err != nil {
			plog.Panicf("invalid json config file %s, %v", fp, err)
		}
		plog.Infof("using drummer config file found at %s", fp)
		return cc
	}
	plog.Warningf("no drummer config file, using default values")
	return getDefaultClusterConfig()
}

func (d *Drummer) setDeploymentID() {
	doneC := make(chan struct{})
	var did uint64
	d.stopper.RunWorker(func() {
		defer close(doneC)
		defer func() {
			plog.Infof("On drummer server using nh %s, deployment id has been set",
				d.nh.RaftAddress())
		}()
		for {
			session := d.getSession(d.ctx)
			if session != nil {
				did, err := d.server.setDeploymentID(d.ctx, session)
				if err != nil {
					if d.ctx.Err() == context.Canceled {
						return
					}
					d.resetSession(d.ctx)
					plog.Errorf(err.Error())
				} else {
					d.clientProposalCompleted()
					if did == 0 {
						plog.Warningf("DeploymentID has not been set yet.")
					} else {
						return
					}
				}
			}
			for i := 0; i < 30; i++ {
				if d.ctx.Err() == context.Canceled {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
	select {
	case <-d.stopper.ShouldStop():
		return
	case <-doneC:
		if did != 0 {
			d.deploymentID = did
		}
		return
	}
}

func (d *Drummer) startDrummerRPCServer() {
	stoppableListener, err := netutil.NewStoppableListener(d.grpcHost, nil,
		d.stopper.ShouldStop())
	if err != nil {
		plog.Panicf("failed to create a listener %v", err)
	}
	var opts []grpc.ServerOption
	tt := "insecure"
	nhCfg := d.nh.NodeHostConfig()
	tlsConfig, err := nhCfg.GetServerTLSConfig()
	if err != nil {
		panic(err)
	}
	if tlsConfig != nil {
		tt = "TLS"
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	opts = append(opts, grpc.MaxRecvMsgSize(maxServerMsgSize))
	opts = append(opts, grpc.MaxSendMsgSize(maxServerMsgSize))
	opts = append(opts, grpc.RPCCompressor(compression.NewCompressor()))
	opts = append(opts, grpc.RPCDecompressor(compression.NewDecompressor()))
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDrummerServer(grpcServer, d.server)
	d.stopper.RunWorker(func() {
		if err := grpcServer.Serve(stoppableListener); err != nil {
			plog.Errorf("serve returned %v", err)
		}
		plog.Infof("Drummer server's gRPC serve function returned, nh %s",
			d.nh.RaftAddress())
	})
	d.grpcServer = grpcServer
	plog.Infof("Drummer server using %s transport is listening on %s",
		tt, d.grpcHost)
}

func (d *Drummer) isLeaderDrummerNode() bool {
	return d.electionManager.isLeader()
}

func (d *Drummer) bootstrapped(ctx context.Context) bool {
	bootstrapped, err := d.server.getBootstrapped(ctx)
	if err != nil {
		plog.Errorf("failed to get bootstrap flag: %v", err)
		return false
	}
	return bootstrapped
}

func (d *Drummer) hasEnoughNodeHostConnected(ctx context.Context) bool {
	collection, err := d.server.GetNodeHostCollection(ctx, nil)
	if err != nil {
		return false
	}
	return len(collection.Collection) >= 3
}

func (d *Drummer) tick() (uint64, error) {
	update := pb.Update{
		Type: pb.Update_TICK,
	}
	cs := d.getSession(d.ctx)
	if cs == nil {
		return 0, errSessionNotReady
	}
	tick, err := d.server.proposeDrummerUpdate(d.ctx, cs, update)
	if err != nil {
		plog.Errorf("tick failed, %v", err)
		d.resetSession(d.ctx)
		return 0, err
	}
	d.clientProposalCompleted()
	if tick.Value == 0 {
		plog.Panicf("invalid zero tick")
	}
	return tick.Value, nil
}

func (d *Drummer) updateRequests(reqs []pb.NodeHostRequest) (uint64, error) {
	if len(reqs) == 0 {
		return 0, nil
	}
	update := pb.Update{
		Type: pb.Update_REQUESTS,
		Requests: pb.NodeHostRequestCollection{
			Requests: reqs,
		},
	}
	cs := d.getSession(d.ctx)
	if cs == nil {
		return 0, errSessionNotReady
	}
	count, err := d.server.proposeDrummerUpdate(d.ctx, cs, update)
	if err != nil {
		d.resetSession(d.ctx)
		return 0, err
	}
	d.clientProposalCompleted()
	return count.Value, nil
}

func (d *Drummer) getSchedulerContext() (*schedulerContext, error) {
	timeout := time.Duration(raftOpTimeoutMillisecond) * time.Millisecond
	ctx, cancel := context.WithTimeout(d.ctx, timeout)
	defer cancel()
	return d.server.getSchedulerContext(ctx)
}

func (d *Drummer) drummerMain() {
	timeout := time.Duration(raftTimeoutSecond) * time.Second
	count := uint64(0)
	td := time.Duration(tickIntervalSecond) * time.Second
	tf := func() bool {
		count += tickIntervalSecond
		leader := d.isLeaderDrummerNode()
		// process the incoming nodehost record
		if leader {
			if _, err := d.tick(); err != nil {
				plog.Errorf("tick failed %v", err)
			}
			// leader drummer
			if count < loopIntervalSecond {
				return false
			}
			count = 0
		} else {
			// follower drummer node, do nothing
			return false
		}
		ctx, cancel := context.WithTimeout(d.ctx, timeout)
		if !d.hasEnoughNodeHostConnected(ctx) {
			plog.Warningf("not enough connected nodehost instances")
			cancel()
			return false
		}
		if !d.bootstrapped(ctx) {
			plog.Infof("drummer is waiting for bootstrap to complete")
			cancel()
			return false
		}
		reqs := d.schedule(ctx)
		if _, err := d.updateRequests(reqs); err != nil {
			plog.Errorf("failed to update requests, %v", err)
		}
		cancel()
		return false
	}
	lang.RunTicker(td, tf, d.stopper.ShouldStop(), nil)
	plog.Debugf("drummer's main loop stopped, nh addr: %s", d.nh.RaftAddress())
}

func (d *Drummer) schedule(ctx context.Context) []pb.NodeHostRequest {
	requests := make([]pb.NodeHostRequest, 0)
	launched, err := d.server.getLaunched(ctx)
	if err == context.Canceled {
		return nil
	}
	sc, err := d.getSchedulerContext()
	if err != nil {
		plog.Errorf("failed to get scheduler context, skip the schedule step")
		return nil
	}
	d.scheduler.updateSchedulerContext(sc)
	// DrummberDB indicates there is no launched cluster
	// we don't remember ever launched anything
	// there is no cluster running to the best knowledge of the Drummer.
	if !launched && !d.scheduler.hasRunningCluster() {
		plog.Infof("going to call launch clusters")
		reqs, err := d.scheduler.launch()
		if err != nil {
			plog.Errorf("drummer failed to launch clusters, %v", err)
			return nil
		}
		return reqs
	}
	// see whether we can repair clusters
	reqs, err := d.maintainClusters()
	if err != nil {
		plog.Warningf("maintainClusters returned %v", err)
		return nil
	}
	return append(requests, reqs...)
}

func (d *Drummer) maintainClusters() ([]pb.NodeHostRequest, error) {
	// TODO:
	// need to look into problem of whether recently repaired clusters should be
	// restored. seem to be okay to restore them as false positive ops are going
	// to cause some zombie nodes in worse case, but they are going to be killed
	// and we already know those zombies are not going to corrupt the data or
	// significantly affect performance.
	plog.Debugf("drummer %s is the leader, running maintain clusters now",
		d.nh.RaftAddress())
	requests := make([]pb.NodeHostRequest, 0)
	restoredClusters := make(map[uint64]struct{})
	reqs, err := d.scheduler.restore()
	if err != nil {
		return nil, err
	}
	for _, req := range reqs {
		restoredClusters[req.Change.ClusterId] = struct{}{}
	}
	requests = append(requests, reqs...)
	// repair clusters?
	reqs, err = d.scheduler.repairClusters(restoredClusters)
	if err != nil {
		return nil, err
	}
	requests = append(requests, reqs...)
	// kill zombies
	reqs = d.scheduler.killZombieNodes()
	requests = append(requests, reqs...)
	for _, req := range requests {
		validateNodeHostRequest(req)
	}
	return requests, nil
}

// FIXME:
// fix these hard coded values
func getDefaultClusterConfig() pb.Config {
	return pb.Config{
		ElectionRTT:        50,
		HeartbeatRTT:       5,
		CheckQuorum:        true,
		CompactionOverhead: 10000,
		SnapshotEntries:    10000,
	}
}
