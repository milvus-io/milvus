// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

// Timestamp is an alias of uint64
type Timestamp = uint64

// IntPrimaryKey is an alias of int64
type IntPrimaryKey = int64

// UniqueID is an alias of int64
type UniqueID = int64

const (
	// EmbeddedRole is for embedded Milvus.
	EmbeddedRole = "embedded"
	// StandaloneRole is a constant represent Standalone
	StandaloneRole = "standalone"
	// RootCoordRole is a constant represent RootCoord
	RootCoordRole = "rootcoord"
	// ProxyRole is a constant represent Proxy
	ProxyRole = "proxy"
	// QueryCoordRole is a constant represent QueryCoord
	QueryCoordRole = "querycoord"
	// QueryNodeRole is a constant represent QueryNode
	QueryNodeRole = "querynode"
	// IndexCoordRole is a constant represent IndexCoord
	IndexCoordRole = "indexcoord"
	// IndexNodeRole is a constant represent IndexNode
	DataCoordRole = "datacoord"
	// DataNodeRole is a constant represent DataNode
	DataNodeRole = "datanode"
	// IndexNodeRole is a constant represent IndexNode
	IndexNodeRole = "indexnode"
	// MixtureRole is a constant represents Mixture running modtoe
	MixtureRole = "mixture"
	// StreamingCoord is a constant represent StreamingCoord
	StreamingCoordRole = "streamingcoord"
	// StreamingNode is a constant represent StreamingNode
	StreamingNodeRole = "streamingnode"
	// MixCoordRole is a constant represent MixCoord
	MixCoordRole = "mixcoord"
)

var (
	serverTypeSet = NewSet(
		StandaloneRole,
		RootCoordRole,
		ProxyRole,
		QueryCoordRole,
		QueryNodeRole,
		IndexNodeRole,
		DataCoordRole,
		DataNodeRole,
		StreamingNodeRole,
		MixCoordRole,
	)
	serverTypeList = serverTypeSet.Collect()
)

const Unlimited int64 = -1

func ServerTypeSet() Set[string] {
	return serverTypeSet
}

func ServerTypeList() []string {
	return serverTypeList
}
