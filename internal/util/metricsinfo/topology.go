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

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// in topology graph, the name of all nodes are consisted of role name and its' id
// for example, Proxy1, DataCoord3

// ConstructComponentName returns a name according to the role name and its' id
func ConstructComponentName(role string, id typeutil.UniqueID) string {
	return role + strconv.Itoa(int(id))
}

// TODO(dragondriver): how different components extend their metrics information?
//					   so maybe providing a interface instead of a struct is better?
//					   then every components can implement it to extend metrics.

// ComponentInfos shows the all necessary metrics of node.
type ComponentInfos struct {
	HasError    bool   `json:"has_error"`
	ErrorReason string `json:"error_reason"`
	Name        string `json:"name"`
	// TODO(dragondriver): more required information
}

// Marshal returns the json string of ComponentInfos
func (infos *ComponentInfos) Marshal() (string, error) {
	binary, err := json.Marshal(infos)
	return string(binary), err
}

// Unmarshal constructs a ComponentInfos object using a json string
func (infos *ComponentInfos) Unmarshal(s string) error {
	return json.Unmarshal([]byte(s), infos)
}

// CoordTopology used for coordinator to show their managed nodes.
type CoordTopology struct {
	Self           ComponentInfos   `json:"self"`
	ConnectedNodes []ComponentInfos `json:"connected_nodes"`
}

// Marshal returns the json string of CoordTopology
func (topology *CoordTopology) Marshal() (string, error) {
	binary, err := json.Marshal(topology)
	return string(binary), err
}

// Unmarshal constructs a CoordTopology object using a json string
func (topology *CoordTopology) Unmarshal(s string) error {
	return json.Unmarshal([]byte(s), topology)
}

// TODO(dragondriver)
// necessary to show all connection edge in topology graph?
// for example, in system, Proxy connects to RootCoord and RootCoord also connects to Proxy,
// if we do so, the connection relationship may be confusing.
// ConnTopology shows how different components connect to each other.
type ConnTopology struct {
	Name                string   `json:"name"`
	ConnectedComponents []string `json:"connected_components"`
}

// Marshal returns the json string of ConnTopology
func (topology *ConnTopology) Marshal() (string, error) {
	binary, err := json.Marshal(topology)
	return string(binary), err
}

// Unmarshal constructs a ConnTopology object using a json string
func (topology *ConnTopology) Unmarshal(s string) error {
	return json.Unmarshal([]byte(s), topology)
}

// SystemTopology shows the system topology
type SystemTopology struct {
}

// Marshal returns the json string of SystemTopology
func (topology *SystemTopology) Marshal() (string, error) {
	binary, err := json.Marshal(topology)
	return string(binary), err
}

// Unmarshal constructs a SystemTopology object using a json string
func (topology *SystemTopology) Unmarshal(s string) error {
	return json.Unmarshal([]byte(s), topology)
}
