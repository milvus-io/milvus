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

package entity

type ResourceGroup struct {
	Name             string
	Capacity         int32
	NumAvailableNode int32
	NumLoadedReplica map[string]int32
	NumOutgoingNode  map[string]int32
	NumIncomingNode  map[string]int32
	Config           *ResourceGroupConfig
	Nodes            []NodeInfo
}

type NodeInfo struct {
	NodeID   int64
	Address  string
	HostName string
}

type ResourceGroupLimit struct {
	NodeNum int32
}

type ResourceGroupTransfer struct {
	ResourceGroup string
}

type ResourceGroupNodeFilter struct {
	NodeLabels map[string]string
}

type ResourceGroupConfig struct {
	Requests     ResourceGroupLimit
	Limits       ResourceGroupLimit
	TransferFrom []*ResourceGroupTransfer
	TransferTo   []*ResourceGroupTransfer
	NodeFilter   ResourceGroupNodeFilter
}

type ReplicaInfo struct {
	ReplicaID         int64
	Shards            []*Shard
	Nodes             []int64
	ResourceGroupName string
	NumOutboundNode   map[string]int32
}

type Shard struct {
	ChannelName string
	ShardNodes  []int64
	ShardLeader int64
}
