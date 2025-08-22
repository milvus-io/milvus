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

package replicatemanager

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ChannelReplicator is the client that replicates the message to the channel in the target cluster.
type ChannelReplicator interface {
	// StartReplicateChannel starts the replicate for the channel.
	StartReplicateChannel()

	// StopReplicateChannel stops the replicate loop
	// and wait for the loop to exit.
	StopReplicateChannel()

	// GetState returns the current state of the replicator.
	GetState() typeutil.LifetimeState
}

var _ ChannelReplicator = (*channelReplicator)(nil)

// channelReplicator is the implementation of ChannelReplicator.
type channelReplicator struct {
	channel string
	cluster *milvuspb.MilvusCluster

	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime
}

// NewChannelReplicator creates a new ChannelReplicator.
func NewChannelReplicator(channel string, cluster *milvuspb.MilvusCluster) ChannelReplicator {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelReplicator{
		channel:  channel,
		cluster:  cluster,
		ctx:      ctx,
		cancel:   cancel,
		lifetime: typeutil.NewLifetime(),
	}
}

func (r *channelReplicator) StartReplicateChannel() {
	logger := log.With(zap.String("cluster", r.cluster.GetClusterId()), zap.String("channel", r.channel))
	if !r.lifetime.Add(typeutil.LifetimeStateWorking) {
		logger.Warn("replicate channel already started")
		return
	}
	logger.Info("start replicate channel")
	go func() {
		defer r.lifetime.Done()
		err := r.replicateLoop()
		if err != nil {
			logger.Warn("replicate channel failed", zap.Error(err))
			r.lifetime.SetState(typeutil.LifetimeStateStopped)
		}
	}()
}

// replicateLoop starts the replicate loop.
func (r *channelReplicator) replicateLoop() error {
	logger := log.With(zap.String("cluster", r.cluster.GetClusterId()), zap.String("channel", r.channel))
	startFrom, err := r.getReplicateStartMessageID()
	if err != nil {
		return err
	}
	ch := make(adaptor.ChanMessageHandler, 64)
	scanner := streaming.WAL().Read(r.ctx, streaming.ReadOption{
		PChannel:       r.channel,
		DeliverPolicy:  options.DeliverPolicyStartFrom(startFrom),
		DeliverFilters: []options.DeliverFilter{},
		MessageHandler: ch,
	})
	defer scanner.Close()

	rsc := replicatestream.NewReplicateStreamClient(r.ctx, r.cluster, r.channel)
	defer rsc.Close()

	for {
		select {
		case <-r.ctx.Done():
			logger.Info("replicate channel stopped")
			return nil
		case msg := <-ch:
			err := rsc.Replicate(msg)
			if err != nil {
				panic(fmt.Sprintf("replicate message failed due to unrecoverable error: %v", err))
			}
		}
	}
}

func (r *channelReplicator) getReplicateStartMessageID() (message.MessageID, error) {
	milvusClient, err := resource.Resource().ClusterClient().CreateMilvusClient(r.ctx, r.cluster)
	if err != nil {
		return nil, err
	}
	defer milvusClient.Close(r.ctx)

	replicateInfo, err := milvusClient.GetReplicateInfo(r.ctx, &milvuspb.GetReplicateInfoRequest{
		SourceClusterId: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
	})
	if err != nil {
		return nil, err
	}

	var checkpoint *milvuspb.ReplicateCheckpoint
	for _, cp := range replicateInfo.GetCheckpoints() {
		if cp.GetSourceChannelName() == r.channel {
			checkpoint = cp
			break
		}
	}
	if checkpoint == nil {
		return nil, fmt.Errorf("channel %s not found in replicate info in cluster %s", r.channel, r.cluster.GetClusterId())
	}

	startFrom := adaptor.MustGetMessageIDFromMQWrapperIDBytes(
		streaming.WAL().WALName(),
		[]byte(checkpoint.GetReplicateMessageId().GetId()),
	)
	log.Info("replicate messages from position",
		zap.String("cluster", r.cluster.GetClusterId()),
		zap.String("channel", r.channel),
		zap.Any("checkpoint", checkpoint),
		zap.Any("startFromMessageID", startFrom),
	)
	return startFrom, nil
}

func (r *channelReplicator) StopReplicateChannel() {
	r.cancel()
	r.lifetime.SetState(typeutil.LifetimeStateStopped)
	r.lifetime.Wait()
}

func (r *channelReplicator) GetState() typeutil.LifetimeState {
	return r.lifetime.GetState()
}
