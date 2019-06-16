// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package logutil

import (
	"fmt"
)

const (
	mod = 100000
)

// ClusterID returns the string representation of a cluster id value
func ClusterID(clusterID uint64) string {
	return fmt.Sprintf("c%05d", clusterID%mod)
}

// NodeID returns the string representation of a node id value.
func NodeID(nodeID uint64) string {
	return fmt.Sprintf("n%05d", nodeID%mod)
}

// DescribeNode returns the string representation of a node with known
// cluster id and node id values.
func DescribeNode(clusterID uint64, nodeID uint64) string {
	return fmt.Sprintf("[%05d:%05d]", clusterID%mod, nodeID%mod)
}

// DescribeSM returns the string representation of a State Machine object
// with known cluster id and node id values.
func DescribeSM(clusterID uint64, nodeID uint64) string {
	return fmt.Sprintf("[%05d:%05d]", clusterID%mod, nodeID%mod)
}
