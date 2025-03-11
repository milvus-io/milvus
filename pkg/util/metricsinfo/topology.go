// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"encoding/json"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// in topology graph, the name of all nodes are consisted of role name and its' id
// for example, Proxy1, DataCoord3

// ConstructComponentName returns a name according to the role name and its' id
func ConstructComponentName(role string, id typeutil.UniqueID) string {
	return role + strconv.Itoa(int(id))
}

// Topology defines the interface of topology graph between different components
type Topology interface{}

// MarshalTopology returns the json string of Topology
func MarshalTopology(topology Topology) (string, error) {
	binary, err := json.Marshal(topology)
	return string(binary), err
}

// UnmarshalTopology constructs a Topology object using the json string
func UnmarshalTopology(s string, topology Topology) error {
	return json.Unmarshal([]byte(s), topology)
}

// QueryClusterTopology shows the topology between QueryCoord and QueryNodes
type QueryClusterTopology struct {
	Self           QueryCoordInfos  `json:"self"`
	ConnectedNodes []QueryNodeInfos `json:"connected_nodes"`
}

// ConnectionType is the type of connection between nodes
type ConnectionType = string

// ConnectionType definitions
const (
	CoordConnectToNode ConnectionType = "manage"
	Forward            ConnectionType = "forward"
)

// ConnectionTargetType is the type of connection target
type ConnectionTargetType = string

// ConnectionInfo contains info of connection target
type ConnectionInfo struct {
	TargetName string               `json:"target_name"`
	TargetType ConnectionTargetType `json:"target_type"`
}

// ConnTopology contains connection topology
// TODO(dragondriver)
// necessary to show all connection edge in topology graph?
// for example, in system, Proxy connects to RootCoord and RootCoord also connects to Proxy,
// if we do so, the connection relationship may be confusing.
// ConnTopology shows how different components connect to each other.
type ConnTopology struct {
	Name                string           `json:"name"`
	ConnectedComponents []ConnectionInfo `json:"connected_components"`
}

// QueryCoordTopology shows the whole metrics of query cluster
type QueryCoordTopology struct {
	Cluster     QueryClusterTopology `json:"cluster"`
	Connections ConnTopology         `json:"connections"`
}

// DataClusterTopology shows the topology between DataCoord and DataNodes
type DataClusterTopology struct {
	Self               DataCoordInfos  `json:"self"`
	ConnectedDataNodes []DataNodeInfos `json:"connected_data_nodes"`
}

// DataCoordTopology shows the whole metrics of index cluster
type DataCoordTopology struct {
	Cluster     DataClusterTopology `json:"cluster"`
	Connections ConnTopology        `json:"connections"`
}

// RootCoordTopology shows the whole metrics of root coordinator
type RootCoordTopology struct {
	Self        RootCoordInfos `json:"self"`
	Connections ConnTopology   `json:"connections"`
}

// ConnectionEdge contains connection's id, type and target type
type ConnectionEdge struct {
	ConnectedIdentifier int                  `json:"connected_identifier"`
	Type                ConnectionType       `json:"type"`
	TargetType          ConnectionTargetType `json:"target_type"` // RootCoord, DataCoord ...
}

// SystemTopologyNode is a node in system topology graph.
type SystemTopologyNode struct {
	Identifier int              `json:"identifier"` // unique in the SystemTopology graph
	Connected  []ConnectionEdge `json:"connected"`
	Infos      ComponentInfos   `json:"infos"`
}

// SystemTopology shows the system topology
type SystemTopology struct {
	NodesInfo []SystemTopologyNode `json:"nodes_info"`
}
