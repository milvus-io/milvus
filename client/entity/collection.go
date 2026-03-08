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

// DefaultShardNumber const value for using Milvus default shard number.
const DefaultShardNumber int32 = 0

// DefaultConsistencyLevel const value for using Milvus default consistency level setting.
const DefaultConsistencyLevel ConsistencyLevel = ClBounded

// Collection represent collection meta in Milvus
type Collection struct {
	ID               int64   // collection id
	Name             string  // collection name
	Schema           *Schema // collection schema, with fields schema and primary key definition
	PhysicalChannels []string
	VirtualChannels  []string
	Loaded           bool
	ConsistencyLevel ConsistencyLevel
	ShardNum         int32
	Properties       map[string]string

	// collection update timestamp, usually used for internal change detection
	UpdateTimestamp uint64
}

// Partition represent partition meta in Milvus
type Partition struct {
	ID     int64  // partition id
	Name   string // partition name
	Loaded bool   // partition loaded
}

// ReplicaGroup represents a replica group
type ReplicaGroup struct {
	ReplicaID     int64
	NodeIDs       []int64
	ShardReplicas []*ShardReplica
}

// ShardReplica represents a shard in the ReplicaGroup
type ShardReplica struct {
	LeaderID      int64
	NodesIDs      []int64
	DmChannelName string
}
