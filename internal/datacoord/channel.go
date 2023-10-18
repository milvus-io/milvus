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

package datacoord

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ROChannel interface {
	String() string
	GetName() string
	GetCollectionID() UniqueID
	GetStartPositions() []*commonpb.KeyDataPair
	GetSchema() *schemapb.CollectionSchema
	GetCreateTimestamp() Timestamp
	GetWatchInfo() *datapb.ChannelWatchInfo
}

type RWChannel interface {
	ROChannel
	UpdateWatchInfo(info *datapb.ChannelWatchInfo)
}

var _ RWChannel = (*channelMeta)(nil)

type channelMeta struct {
	Name            string
	CollectionID    UniqueID
	StartPositions  []*commonpb.KeyDataPair
	Schema          *schemapb.CollectionSchema
	CreateTimestamp uint64
	WatchInfo       *datapb.ChannelWatchInfo
}

func (ch *channelMeta) UpdateWatchInfo(info *datapb.ChannelWatchInfo) {
	ch.WatchInfo = info
}

func (ch *channelMeta) GetWatchInfo() *datapb.ChannelWatchInfo {
	return ch.WatchInfo
}

func (ch *channelMeta) GetName() string {
	return ch.Name
}

func (ch *channelMeta) GetCollectionID() UniqueID {
	return ch.CollectionID
}

func (ch *channelMeta) GetStartPositions() []*commonpb.KeyDataPair {
	return ch.StartPositions
}

func (ch *channelMeta) GetSchema() *schemapb.CollectionSchema {
	return ch.Schema
}

func (ch *channelMeta) GetCreateTimestamp() Timestamp {
	return ch.CreateTimestamp
}

// String implement Stringer.
func (ch *channelMeta) String() string {
	// schema maybe too large to print
	return fmt.Sprintf("Name: %s, CollectionID: %d, StartPositions: %v", ch.Name, ch.CollectionID, ch.StartPositions)
}
