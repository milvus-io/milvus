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

package channel

import (
	"context"
	"fmt"
	"math"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type channelManager struct {
	catalog metastore.DataCoordCatalog

	mut         lock.RWMutex
	checkpoints map[string]*msgpb.MsgPosition
}

func NewChannelManager(ctx context.Context, catalog metastore.DataCoordCatalog) (*channelManager, error) {
	m := &channelManager{
		catalog: catalog,

		checkpoints: make(map[string]*msgpb.MsgPosition),
	}

	err := m.reloadFromKV(ctx)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *channelManager) reloadFromKV(ctx context.Context) error {
	channelCPs, err := m.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		return err
	}
	for vChannel, pos := range channelCPs {
		// for 2.2.2 issue https://github.com/milvus-io/milvus/issues/22181
		pos.ChannelName = vChannel
		m.checkpoints[vChannel] = pos
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel).
			Set(float64(ts.Unix()))
	}
	return nil
}

func (m *channelManager) GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition {
	m.mut.RLock()
	defer m.mut.RUnlock()
	cp, ok := m.checkpoints[vChannel]
	if !ok {
		return nil
	}
	return proto.Clone(cp).(*msgpb.MsgPosition)
}

func (m *channelManager) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	err := m.catalog.DropChannelCheckpoint(ctx, vChannel)
	if err != nil {
		return err
	}
	delete(m.checkpoints, vChannel)
	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel)
	log.Info("DropChannelCheckpoint done", zap.String("vChannel", vChannel))
	return nil
}

func (m *channelManager) GetChannelCheckpoints() map[string]*msgpb.MsgPosition {
	m.mut.RLock()
	defer m.mut.RUnlock()

	checkpoints := make(map[string]*msgpb.MsgPosition, len(m.checkpoints))
	for ch, cp := range m.checkpoints {
		checkpoints[ch] = typeutil.Clone(cp)
	}
	return checkpoints
}

// UpdateChannelCheckpoint updates and saves channel checkpoint.
func (m *channelManager) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	if pos == nil || pos.GetMsgID() == nil {
		return fmt.Errorf("channelCP is nil, vChannel=%s", vChannel)
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	oldPosition, ok := m.checkpoints[vChannel]
	if !ok || oldPosition.Timestamp < pos.Timestamp {
		err := m.catalog.SaveChannelCheckpoint(ctx, vChannel, pos)
		if err != nil {
			return err
		}
		m.checkpoints[vChannel] = pos
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		log.Info("UpdateChannelCheckpoint done",
			zap.String("vChannel", vChannel),
			zap.Uint64("ts", pos.GetTimestamp()),
			zap.ByteString("msgID", pos.GetMsgID()),
			zap.Time("time", ts))
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel).
			Set(float64(ts.Unix()))
	}
	return nil
}

// MarkChannelCheckpointDropped set channel checkpoint to MaxUint64 preventing future update
// and remove the metrics for channel checkpoint lag.
func (m *channelManager) MarkChannelCheckpointDropped(ctx context.Context, channel string) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	cp := &msgpb.MsgPosition{
		ChannelName: channel,
		Timestamp:   math.MaxUint64,
	}

	err := m.catalog.SaveChannelCheckpoints(ctx, []*msgpb.MsgPosition{cp})
	if err != nil {
		return err
	}

	m.checkpoints[channel] = cp

	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), channel)
	return nil
}

// UpdateChannelCheckpoints updates and saves channel checkpoints.
func (m *channelManager) UpdateChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	toUpdates := lo.Filter(positions, func(pos *msgpb.MsgPosition, _ int) bool {
		if pos == nil || pos.GetMsgID() == nil || pos.GetChannelName() == "" {
			log.Warn("illegal channel cp", zap.Any("pos", pos))
			return false
		}
		vChannel := pos.GetChannelName()
		oldPosition, ok := m.checkpoints[vChannel]
		return !ok || oldPosition.Timestamp < pos.Timestamp
	})
	err := m.catalog.SaveChannelCheckpoints(ctx, toUpdates)
	if err != nil {
		return err
	}
	for _, pos := range toUpdates {
		channel := pos.GetChannelName()
		m.checkpoints[channel] = pos
		log.Info("UpdateChannelCheckpoint done", zap.String("channel", channel),
			zap.Uint64("ts", pos.GetTimestamp()),
			zap.Time("time", tsoutil.PhysicalTime(pos.GetTimestamp())))
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), channel).Set(float64(ts.Unix()))
	}
	return nil
}

func (m *channelManager) MarkChannelAdded(ctx context.Context, channelName string) error {
	return m.catalog.MarkChannelAdded(ctx, channelName)
}

func (m *channelManager) MarkChannelDeleted(ctx context.Context, channelName string) error {
	return m.catalog.MarkChannelDeleted(ctx, channelName)
}

func (m *channelManager) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	return m.catalog.GcConfirm(ctx, collectionID, partitionID)
}

func (m *channelManager) ChannelExists(ctx context.Context, channelName string) bool {
	return m.catalog.ChannelExists(ctx, channelName)
}

func (m *channelManager) ShouldDropChannel(ctx context.Context, channelName string) bool {
	return m.catalog.ShouldDropChannel(ctx, channelName)
}

func (m *channelManager) DropChannel(ctx context.Context, channelName string) error {
	return m.catalog.DropChannel(ctx, channelName)
}
