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

import (
	"fmt"
	"reflect"
)

// ImmutablemapString2string is a map only support read
type ImmutablemapString2string struct {
	storemap map[string]string
}

func NewImmutablemapString2string(in map[string]string) ImmutablemapString2string {
	out := ImmutablemapString2string{
		storemap: in,
	}
	return out
}

func (imstr2str ImmutablemapString2string) IsEmpty() bool {
	return reflect.DeepEqual(imstr2str, ImmutablemapString2string{})
}

func (imstr2str *ImmutablemapString2string) Get(key string) (string, error) {
	if imstr2str.storemap == nil {
		return "", fmt.Errorf("key not exist")
	}
	rstr, ok := imstr2str.storemap[key]
	if !ok {
		return "", fmt.Errorf("key not exist")
	}
	return rstr, nil
}

func (imstr2str *ImmutablemapString2string) Put(key string, val string) (string, error) {
	return "", fmt.Errorf("not allowed put in immutablemap")
}

func (imstr2str *ImmutablemapString2string) GetCopy() map[string]string {
	mapcopy := make(map[string]string)
	for key, value := range imstr2str.storemap {
		mapcopy[key] = value
	}
	return mapcopy
}

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
	IndexNodeRole = "indexnode"
	// DataCoordRole is a constant represent DataCoord
	DataCoordRole = "datacoord"
	// DataNodeRole is a constant represent DataNode
	DataNodeRole = "datanode"
)

func ServerTypeMap() map[string]interface{} {
	return map[string]interface{}{
		EmbeddedRole:   nil,
		StandaloneRole: nil,
		RootCoordRole:  nil,
		ProxyRole:      nil,
		QueryCoordRole: nil,
		QueryNodeRole:  nil,
		IndexCoordRole: nil,
		IndexNodeRole:  nil,
		DataCoordRole:  nil,
		DataNodeRole:   nil,
	}
}

func ServerTypeList() []string {
	serverTypeMap := ServerTypeMap()
	types := make([]string, 0, len(serverTypeMap))
	for key := range serverTypeMap {
		types = append(types, key)
	}
	return types
}
