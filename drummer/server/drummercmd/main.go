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
Drummercmd is a drummer client program.
*/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	dc "github.com/lni/dragonboat/drummer"
	"github.com/lni/dragonboat/drummer/client"
	pb "github.com/lni/dragonboat/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
)

const (
	defaultTimeout = 10 * time.Second
)

var (
	plog = logger.GetLogger("drummercmd")
)

func getDrummerClient(pool *client.Pool,
	drummerServers []string, mutualTLS bool,
	caFile string, certFile string, keyFile string) pb.DrummerClient {
	if !mutualTLS {
		for _, server := range drummerServers {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			conn, err := pool.GetInsecureConnection(ctx, server)
			cancel()
			if err == nil {
				return pb.NewDrummerClient(conn.ClientConn())
			}
		}
	} else {
		for _, server := range drummerServers {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			conn, err := pool.GetTLSConnection(ctx, server, caFile, certFile, keyFile)
			cancel()
			if err == nil {
				return pb.NewDrummerClient(conn.ClientConn())
			}
		}
	}

	return nil
}

func main() {
	exitCode := 1
	l := logger.GetLogger("client")
	l.SetLevel(logger.WARNING)
	servers := flag.String("servers",
		"localhost:9121,localhost:9122,localhost:9123",
		"comma separated Drummer address list")
	op := flag.String("op",
		"list-nodehost",
		"list-nodehost, list-cluster, set-bootstrapped, set-regions, add-server, remove-server and create are supported")
	nodeID := flag.Uint64("nodeid", 4, "node id to be added or removed")
	address := flag.String("address", "", "address of the server to be added")
	clusterID := flag.Uint64("clusterid", 1, "cluster id for the create and list-cluster operation")
	count := flag.Int("size", 3, "number of nodes in the cluster")
	appname := flag.String("appname", "", "application name")
	regions := flag.String("regions", "", "region configuration")
	verbose := flag.Bool("verbose", false, "verbose mode, more details will be printed out")
	mutualtls := flag.Bool("mutual-tls", false, "whether to use Mutual TLS authentication")
	cafile := flag.String("ca-file", "", "CA certificate file path")
	certfile := flag.String("cert-file", "", "client certificate file")
	keyfile := flag.String("key-file", "", "client key file")
	flag.Parse()
	if !checkOpValue(*op) {
		os.Exit(exitCode)
	}
	if !checkCreateParameters(*op, *clusterID, *count) ||
		!checkListClusterParameters(*op, *clusterID) ||
		!checkAddRemoveServerParameters(*op, *nodeID, *address) {
		os.Exit(exitCode)
	}
	if *mutualtls {
		if len(*cafile) == 0 || len(*certfile) == 0 || len(*keyfile) == 0 {
			panic("CA file, cert file or key file not specified")
		}
	}
	pool := client.NewDrummerConnectionPool()
	defer pool.Close()
	drummerServers := strings.Split(*servers, ",")
	if len(drummerServers) == 0 {
		plog.Errorf("drummer server not specified")
		os.Exit(exitCode)
	}
	client := getDrummerClient(pool, drummerServers,
		*mutualtls, *cafile, *certfile, *keyfile)
	if client == nil {
		plog.Errorf("failed to get a drummer client")
		os.Exit(exitCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if *op == "list-nodehost" {
		collection, err := dc.GetNodeHostCollection(ctx, client)
		if err == nil {
			printNodeHostCollection(collection, *verbose)
			exitCode = 0
		} else {
			plog.Errorf("failed to get node host collection, %v", err)
		}
	} else if *op == "list-cluster" {
		collection, err := dc.GetClusterStates(ctx, client, []uint64{*clusterID})
		if err == nil {
			printClusterStates(collection)
			exitCode = 0
		} else {
			plog.Errorf("failed to get cluster state, %v", err)
		}
	} else if *op == "list-launched-clusters" {
		collection, err := dc.GetClusterCollection(ctx, client)
		if err == nil {
			printClusterCollection(collection)
			exitCode = 0
		} else {
			plog.Errorf("failed to get cluster collection, %v", err)
		}
	} else if *op == "set-bootstrapped" {
		if err := dc.SubmitBootstrappped(ctx, client); err != nil {
			plog.Errorf("failed to set bootstrapped flag to true, %v", err)
		} else {
			exitCode = 0
		}
	} else if *op == "set-regions" {
		r, err := parseRegions(*regions)
		if err != nil {
			plog.Errorf("failed to parse the regions settings, %v", err)
		} else {
			if err := dc.SubmitRegions(ctx, client, r); err != nil {
				plog.Errorf("failed to set regions, %v", err)
			} else {
				exitCode = 0
			}
		}
	} else if *op == "create" {
		members := getRandomNodeIDs(*count)
		if err := dc.SubmitCreateDrummerChange(ctx,
			client, *clusterID, members, *appname); err != nil {
			plog.Errorf("failed to submit drummer CREATE change, %v", err)
		} else {
			exitCode = 0
		}
	} else if *op == "add-server" {
		_, err := dc.AddDrummerServer(ctx, client, *nodeID, *address)
		if err != nil {
			plog.Errorf("failed to add drummer server, %v", err)
		} else {
			printDrummerConfigChange()
			exitCode = 0
		}
	} else if *op == "remove-server" {
		_, err := dc.RemoveDrummerServer(ctx, client, *nodeID)
		if err != nil {
			plog.Errorf("failed to remove drummer server, %v", err)
		} else {
			printDrummerConfigChange()
			exitCode = 0
		}
	} else {
		plog.Panicf("not suppose to reach here")
	}

	os.Exit(exitCode)
}

func getRandomNodeIDs(count int) []uint64 {
	result := make([]uint64, 0)
	r := random.NewLockedRand()
	for i := 0; i < count; i++ {
		result = append(result, r.Uint64())
	}
	return result
}

func parseRegions(payload string) (pb.Regions, error) {
	regions := make([]string, 0)
	counts := make([]uint64, 0)
	parts := strings.Split(payload, ",")
	for _, curPart := range parts {
		elements := strings.Split(curPart, ":")
		if len(elements) != 2 {
			return pb.Regions{}, errors.New("invalid region setting format")
		}
		regions = append(regions, elements[0])
		c, err := strconv.ParseUint(elements[1], 10, 64)
		if err != nil {
			return pb.Regions{}, errors.New("invalid count in region setting")
		}
		counts = append(counts, c)
	}
	return pb.Regions{
		Region: regions,
		Count:  counts,
	}, nil
}

func printClusterStates(clusterStates *pb.ClusterStates) {
	fmt.Printf("total number of returned clusters: %d\n",
		len(clusterStates.Collection))
	for _, s := range clusterStates.Collection {
		fmt.Printf("ClusterID: %d, unavailable: %t\n",
			s.ClusterId, s.State == pb.ClusterState_UNAVAILABLE)
		for nodeID, addr := range s.Nodes {
			RPCAddr := s.RPCAddresses[nodeID]
			isLeader := nodeID == s.LeaderNodeId
			fmt.Printf("NodeID: %d, Address:%s, API Address: %s, IsLeader: %t\n",
				nodeID, addr, RPCAddr, isLeader)
		}
	}
}

func printClusterCollection(clusterInfo *pb.ClusterCollection) {
	fmt.Printf("total number of clusters launched by Drummer: %d\n",
		len(clusterInfo.Clusters))
	for _, v := range clusterInfo.Clusters {
		fmt.Printf("Cluster ID: %d, NodeIDs: ", v.ClusterId)
		for i, m := range v.Members {
			if i == len(v.Members)-1 {
				fmt.Printf("%d", m)
			} else {
				fmt.Printf("%d,", m)
			}
		}
		fmt.Printf("\n")
	}
}

func printNodeHostCollection(c *pb.NodeHostCollection, verbose bool) {
	fmt.Printf("total nodehost count: %d\n", len(c.Collection))
	for _, nh := range c.Collection {
		failed := entityFailed(nh.LastTick, c.Tick)
		fmt.Printf("Address: %s, API Address: %s, Region: %s, Raft clusters count: %d, Failed: %t\n",
			nh.RaftAddress, nh.RPCAddress, nh.Region, len(nh.ClusterInfo), failed)
		if verbose {
			for _, ci := range nh.ClusterInfo {
				fmt.Printf("\tCluster ClusterID: %d, NodeID: %d, Is Leader: %v\n",
					ci.ClusterId, ci.NodeId, ci.IsLeader)
			}
		}
	}
}

func entityFailed(lastTick uint64, currentTick uint64) bool {
	return dc.EntityFailed(lastTick, currentTick)
}

func checkOpValue(op string) bool {
	if op != "list-nodehost" && op != "list-cluster" && op != "list-launched-clusters" &&
		op != "create" && op != "set-bootstrapped" && op != "set-regions" &&
		op != "add-server" && op != "remove-server" {
		plog.Errorf("invalid op value %s", op)
		return false
	}
	return true
}

func checkListClusterParameters(op string, clusterID uint64) bool {
	if op != "list-cluster" {
		return true
	}
	if clusterID == 0 {
		plog.Errorf("invalid cluster id value %d", clusterID)
		return false
	}
	return true
}

func checkCreateParameters(op string, clusterID uint64, count int) bool {
	if op != "create" {
		return true
	}
	if clusterID == 0 {
		plog.Errorf("invalid cluster id value %d", clusterID)
		return false
	}
	if count <= 0 {
		plog.Errorf("invalid count value %d", count)
		return false
	}
	if count > 5 {
		plog.Warningf("large count value (%d) specified", count)
	}
	return true
}

func checkAddRemoveServerParameters(op string, nodeID uint64, addr string) bool {
	if op == "add-server" {
		if nodeID <= 4 {
			plog.Errorf("invalid NodeID value %d", nodeID)
			return false
		}
		if len(addr) == 0 {
			plog.Errorf("invalid address %s", addr)
			return false
		}
	} else if op == "remove-server" {
		if nodeID == 0 {
			plog.Errorf("invalid NodeID value %d", nodeID)
			return false
		}
	}
	return true
}

func printDrummerConfigChange() {
	fmt.Printf("NOTICE - Drummer server has been added or removed.\n" +
		"dragonboat-drummer.json configuration files on all Drummer servers must be updated " +
		"accordingly to reflect the added/removed Drummer server.\n")
}
