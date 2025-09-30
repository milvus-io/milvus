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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const scannerHandlerChanSize = 64

// Replicator is the client that replicates the message to the channel in the target cluster.
type Replicator interface {
	// StartReplicate starts the replicate for the channel.
	StartReplicate()

	// StopReplicate stops the replicate loop
	// and wait for the loop to exit.
	StopReplicate()

	// GetState returns the current state of the replicator.
	GetState() typeutil.LifetimeState
}

var _ Replicator = (*channelReplicator)(nil)

// channelReplicator is the implementation of ChannelReplicator.
type channelReplicator struct {
	replicateInfo *streamingpb.ReplicatePChannelMeta
	createRscFunc replicatestream.CreateReplicateStreamClientFunc

	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime
}

// NewChannelReplicator creates a new ChannelReplicator.
func NewChannelReplicator(replicateMeta *streamingpb.ReplicatePChannelMeta) Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	createRscFunc := replicatestream.NewReplicateStreamClient
	return &channelReplicator{
		replicateInfo: replicateMeta,
		createRscFunc: createRscFunc,
		ctx:           ctx,
		cancel:        cancel,
		lifetime:      typeutil.NewLifetime(),
	}
}

func (r *channelReplicator) StartReplicate() {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)
	if !r.lifetime.Add(typeutil.LifetimeStateWorking) {
		logger.Warn("replicate channel already started")
		return
	}
	logger.Info("start replicate channel")
	go func() {
		defer r.lifetime.Done()
		for {
			err := r.replicateLoop()
			if err != nil {
				logger.Warn("replicate channel failed", zap.Error(err))
				time.Sleep(10 * time.Second)
				continue
			}
			break
		}
		logger.Info("stop replicate channel")
	}()
}

// replicateLoop starts the replicate loop.
func (r *channelReplicator) replicateLoop() error {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)
	cp, err := r.getReplicateCheckpoint()
	if err != nil {
		return err
	}
	ch := make(adaptor.ChanMessageHandler, scannerHandlerChanSize)
	scanner := streaming.WAL().Read(r.ctx, streaming.ReadOption{
		PChannel:       r.replicateInfo.GetSourceChannelName(),
		DeliverPolicy:  options.DeliverPolicyStartFrom(cp.MessageID),
		DeliverFilters: []options.DeliverFilter{options.DeliverFilterTimeTickGT(cp.TimeTick)},
		MessageHandler: ch,
	})
	defer scanner.Close()

	rsc := r.createRscFunc(r.ctx, r.replicateInfo)
	defer rsc.Close()

	logger.Info("start replicate channel loop", zap.Any("startFrom", cp))

	for {
		select {
		case <-r.ctx.Done():
			logger.Info("replicate channel stopped")
			return nil
		case msg := <-ch:
			// TODO: Should be done at streamingnode.
			if msg.MessageType().IsSelfControlled() {
				if msg.MessageType() != message.MessageTypeTimeTick {
					logger.Debug("skip self-controlled message", log.FieldMessage(msg))
				}
				continue
			}
			err := rsc.Replicate(msg)
			if err != nil {
				panic(fmt.Sprintf("replicate message failed due to unrecoverable error: %v", err))
			}
			logger.Debug("replicate message success", log.FieldMessage(msg))
		}
	}
}

func (r *channelReplicator) getReplicateCheckpoint() (*utility.ReplicateCheckpoint, error) {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)

	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	milvusClient, err := resource.Resource().ClusterClient().CreateMilvusClient(ctx, r.replicateInfo.GetTargetCluster())
	if err != nil {
		return nil, err
	}
	defer milvusClient.Close(ctx)

	sourceClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	replicateInfo, err := milvusClient.GetReplicateInfo(ctx, sourceClusterID)
	if err != nil {
		return nil, err
	}

	var checkpoint *commonpb.ReplicateCheckpoint
	for _, cp := range replicateInfo.GetCheckpoints() {
		if cp.GetPchannel() == r.replicateInfo.GetSourceChannelName() {
			checkpoint = cp
			break
		}
	}
	if checkpoint == nil || checkpoint.MessageId == nil {
		initializedCheckpoint := utility.NewReplicateCheckpointFromProto(r.replicateInfo.InitializedCheckpoint)
		logger.Info("channel not found in replicate info, will start from the beginning",
			zap.Stringer("messageID", initializedCheckpoint.MessageID),
			zap.Uint64("timeTick", initializedCheckpoint.TimeTick),
		)
		return initializedCheckpoint, nil
	}

	cp := utility.NewReplicateCheckpointFromProto(checkpoint)
	logger.Info("replicate messages from position",
		zap.Stringer("messageID", cp.MessageID),
		zap.Uint64("timeTick", cp.TimeTick),
	)
	return cp, nil
}

func (r *channelReplicator) StopReplicate() {
	r.lifetime.SetState(typeutil.LifetimeStateStopped)
	r.cancel()
	r.lifetime.Wait()
}

func (r *channelReplicator) GetState() typeutil.LifetimeState {
	return r.lifetime.GetState()
}
