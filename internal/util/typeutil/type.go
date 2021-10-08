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

package typeutil

// Timestamp is an alias of uint64
type Timestamp = uint64

// IntPrimaryKey is an alias of int64
type IntPrimaryKey = int64

// UniqueID is an alias of int64
type UniqueID = int64

const (
	// RootCoordRole is a constant represent RootCoord
	RootCoordRole = "RootCoord"
	// ProxyRole is a constant represent Proxy
	ProxyRole = "Proxy"
	// QueryCoordRole is a constant represent QueryCoord
	QueryCoordRole = "QueryCoord"
	// QueryNodeRole is a constant represent QueryNode
	QueryNodeRole = "QueryNode"
	// IndexCoordRole is a constant represent IndexCoord
	IndexCoordRole = "IndexCoord"
	// IndexNodeRole is a constant represent IndexNode
	IndexNodeRole = "IndexNode"
	// DataCoordRole is a constant represent DataCoord
	DataCoordRole = "DataCoord"
	// DataNodeRole is a constant represent DataNode
	DataNodeRole = "DataNode"
)
