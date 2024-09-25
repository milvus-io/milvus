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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
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

func NewRWChannel(name string,
	collectionID int64,
	startPos []*commonpb.KeyDataPair,
	schema *schemapb.CollectionSchema,
	createTs uint64,
) RWChannel {
	return &StateChannel{
		Name:            name,
		CollectionID:    collectionID,
		StartPositions:  startPos,
		Schema:          schema,
		CreateTimestamp: createTs,
	}
}

type channelMeta struct {
	Name            string
	CollectionID    UniqueID
	StartPositions  []*commonpb.KeyDataPair
	Schema          *schemapb.CollectionSchema
	CreateTimestamp uint64
	WatchInfo       *datapb.ChannelWatchInfo
}

var _ RWChannel = (*channelMeta)(nil)

func (ch *channelMeta) UpdateWatchInfo(info *datapb.ChannelWatchInfo) {
	log.Info("Channel updating watch info",
		zap.Any("old watch info", ch.WatchInfo),
		zap.Any("new watch info", info))
	ch.WatchInfo = proto.Clone(info).(*datapb.ChannelWatchInfo)
	if ch.Schema == nil {
		ch.Schema = info.GetSchema()
	}
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

type ChannelState string

const (
	Standby   ChannelState = "Standby"
	ToWatch   ChannelState = "ToWatch"
	Watching  ChannelState = "Watching"
	Watched   ChannelState = "Watched"
	ToRelease ChannelState = "ToRelease"
	Releasing ChannelState = "Releasing"
	Legacy    ChannelState = "Legacy"
)

type StateChannel struct {
	Name            string
	CollectionID    UniqueID
	StartPositions  []*commonpb.KeyDataPair
	Schema          *schemapb.CollectionSchema
	CreateTimestamp uint64
	Info            *datapb.ChannelWatchInfo

	currentState ChannelState
	assignedNode int64
}

var _ RWChannel = (*StateChannel)(nil)

func NewStateChannel(ch RWChannel) *StateChannel {
	c := &StateChannel{
		Name:            ch.GetName(),
		CollectionID:    ch.GetCollectionID(),
		StartPositions:  ch.GetStartPositions(),
		Schema:          ch.GetSchema(),
		CreateTimestamp: ch.GetCreateTimestamp(),
		Info:            ch.GetWatchInfo(),

		assignedNode: bufferID,
	}

	c.setState(Standby)
	return c
}

func NewStateChannelByWatchInfo(nodeID int64, info *datapb.ChannelWatchInfo) *StateChannel {
	c := &StateChannel{
		Name:         info.GetVchan().GetChannelName(),
		CollectionID: info.GetVchan().GetCollectionID(),
		Schema:       info.GetSchema(),
		Info:         info,
		assignedNode: nodeID,
	}

	switch info.GetState() {
	case datapb.ChannelWatchState_ToWatch:
		c.setState(ToWatch)
	case datapb.ChannelWatchState_ToRelease:
		c.setState(ToRelease)
		// legacy state
	case datapb.ChannelWatchState_WatchSuccess:
		c.setState(Watched)
	case datapb.ChannelWatchState_WatchFailure, datapb.ChannelWatchState_ReleaseSuccess, datapb.ChannelWatchState_ReleaseFailure:
		c.setState(Standby)
	default:
		c.setState(Standby)
	}

	if nodeID == bufferID {
		c.setState(Standby)
	}
	return c
}

func (c *StateChannel) TransitionOnSuccess() {
	switch c.currentState {
	case Standby:
		c.setState(ToWatch)
	case ToWatch:
		c.setState(Watching)
	case Watching:
		c.setState(Watched)
	case Watched:
		c.setState(ToRelease)
	case ToRelease:
		c.setState(Releasing)
	case Releasing:
		c.setState(Standby)
	}
}

func (c *StateChannel) TransitionOnFailure() {
	switch c.currentState {
	case Watching:
		c.setState(Standby)
	case Releasing:
		c.setState(Standby)
	case Standby, ToWatch, Watched, ToRelease:
		// Stay original state
	}
}

func (c *StateChannel) Clone() *StateChannel {
	return &StateChannel{
		Name:            c.Name,
		CollectionID:    c.CollectionID,
		StartPositions:  c.StartPositions,
		Schema:          c.Schema,
		CreateTimestamp: c.CreateTimestamp,
		Info:            proto.Clone(c.Info).(*datapb.ChannelWatchInfo),

		currentState: c.currentState,
		assignedNode: c.assignedNode,
	}
}

func (c *StateChannel) String() string {
	// schema maybe too large to print
	return fmt.Sprintf("Name: %s, CollectionID: %d, StartPositions: %v, Schema: %v", c.Name, c.CollectionID, c.StartPositions, c.Schema)
}

func (c *StateChannel) GetName() string {
	return c.Name
}

func (c *StateChannel) GetCollectionID() UniqueID {
	return c.CollectionID
}

func (c *StateChannel) GetStartPositions() []*commonpb.KeyDataPair {
	return c.StartPositions
}

func (c *StateChannel) GetSchema() *schemapb.CollectionSchema {
	return c.Schema
}

func (c *StateChannel) GetCreateTimestamp() Timestamp {
	return c.CreateTimestamp
}

func (c *StateChannel) GetWatchInfo() *datapb.ChannelWatchInfo {
	return c.Info
}

func (c *StateChannel) UpdateWatchInfo(info *datapb.ChannelWatchInfo) {
	if c.Info != nil && c.Info.Vchan != nil && info.GetVchan().GetChannelName() != c.Info.GetVchan().GetChannelName() {
		log.Warn("Updating incorrect channel watch info",
			zap.Any("old watch info", c.Info),
			zap.Any("new watch info", info),
			zap.Stack("call stack"),
		)
		return
	}

	c.Info = proto.Clone(info).(*datapb.ChannelWatchInfo)
	if c.Schema == nil {
		log.Info("Channel updating watch info for nil schema in old info",
			zap.Any("old watch info", c.Info),
			zap.Any("new watch info", info))
		c.Schema = info.GetSchema()
	}
}

func (c *StateChannel) Assign(nodeID int64) {
	c.assignedNode = nodeID
}

func (c *StateChannel) setState(state ChannelState) {
	c.currentState = state
}
