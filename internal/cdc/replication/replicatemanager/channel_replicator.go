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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/internal/cdc/util"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// Replicator is the client that replicates the message to the channel in the target cluster.
type Replicator interface {
	// StartReplication starts the replicate for the channel.
	StartReplication()

	// StopReplication stops the replicate loop
	// and wait for the loop to exit.
	StopReplication()
}

var _ Replicator = (*channelReplicator)(nil)

// channelReplicator is the implementation of ChannelReplicator.
type channelReplicator struct {
	channel       *meta.ReplicateChannel
	createRscFunc replicatestream.CreateReplicateStreamClientFunc
	createMcFunc  cluster.CreateMilvusClientFunc
	targetClient  cluster.MilvusClient
	streamClient  replicatestream.ReplicateStreamClient
	msgScanner    streaming.Scanner
	msgChan       adaptor.ChanMessageHandler

	asyncNotifier *syncutil.AsyncTaskNotifier[struct{}]
}

// NewChannelReplicator creates a new ChannelReplicator.
func NewChannelReplicator(channel *meta.ReplicateChannel) Replicator {
	createRscFunc := replicatestream.NewReplicateStreamClient
	return &channelReplicator{
		channel:       channel,
		createRscFunc: createRscFunc,
		createMcFunc:  cluster.NewMilvusClient,
		asyncNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
}

func (r *channelReplicator) StartReplication() {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("modRevision", r.channel.ModRevision))
	logger.Info("start replicate channel")
	go func() {
		defer func() {
			if r.streamClient != nil {
				r.streamClient.Close()
			}
			if r.msgScanner != nil {
				r.msgScanner.Close()
			}
			if r.targetClient != nil {
				r.targetClient.Close(r.asyncNotifier.Context())
			}
			r.asyncNotifier.Finish(struct{}{})
		}()
	INIT_LOOP:
		for {
			select {
			case <-r.asyncNotifier.Context().Done():
				return
			default:
				err := r.init()
				if err != nil {
					logger.Warn("initialize replicator failed", zap.Error(err))
					continue
				}
				break INIT_LOOP
			}
		}
		r.startConsumeLoop()
	}()
}

func (r *channelReplicator) init() error {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("modRevision", r.channel.ModRevision))
	// init target client
	if r.targetClient == nil {
		dialCtx, dialCancel := context.WithTimeout(r.asyncNotifier.Context(), 30*time.Second)
		defer dialCancel()
		milvusClient, err := r.createMcFunc(dialCtx, r.channel.Value.GetTargetCluster())
		if err != nil {
			return err
		}
		r.targetClient = milvusClient
		logger.Info("target client initialized")
	}
	// init msg scanner
	if r.msgScanner == nil {
		cp, err := r.getReplicateCheckpoint()
		if err != nil {
			return err
		}
		ch := make(adaptor.ChanMessageHandler)
		scanner := streaming.WAL().Read(r.asyncNotifier.Context(), streaming.ReadOption{
			PChannel:               r.channel.Value.GetSourceChannelName(),
			DeliverPolicy:          options.DeliverPolicyStartFrom(cp.MessageID),
			DeliverFilters:         []options.DeliverFilter{options.DeliverFilterTimeTickGT(cp.TimeTick)},
			MessageHandler:         ch,
			IgnorePauseConsumption: true,
		})
		r.msgScanner = scanner
		r.msgChan = ch
		logger.Info("scanner initialized", zap.Any("checkpoint", cp))
	}
	// init replicate stream client
	if r.streamClient == nil {
		r.streamClient = r.createRscFunc(r.asyncNotifier.Context(), r.targetClient, r.channel)
		logger.Info("stream client initialized")
	}
	return nil
}

// startConsumeLoop starts the replicate loop.
func (r *channelReplicator) startConsumeLoop() {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("modRevision", r.channel.ModRevision))
	logger.Info("start consume loop")

	for {
		select {
		case <-r.asyncNotifier.Context().Done():
			logger.Info("consume loop stopped")
			return
		case msg := <-r.msgChan:
			err := r.streamClient.Replicate(msg)
			if err != nil {
				if !errors.Is(err, replicatestream.ErrReplicateIgnored) {
					panic(fmt.Sprintf("replicate message failed due to unrecoverable error: %v", err))
				}
				continue
			}
			logger.Debug("replicate message success", log.FieldMessage(msg))
			if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
				if util.IsReplicationRemovedByAlterReplicateConfigMessage(msg, r.channel.Value) {
					logger.Info("replication removed, stop consume loop")
					r.streamClient.BlockUntilFinish()
					return
				}
			}
		}
	}
}

func (r *channelReplicator) getReplicateCheckpoint() (*utility.ReplicateCheckpoint, error) {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("modRevision", r.channel.ModRevision))

	// For pchannel-increasing tasks, the secondary WAL for new pchannels hasn't received the
	// AlterReplicateConfig yet, so GetReplicateInfo would fail. Use InitializedCheckpoint directly.
	if r.channel.Value.GetSkipGetReplicateCheckpoint() {
		initializedCheckpoint := utility.NewReplicateCheckpointFromProto(r.channel.Value.InitializedCheckpoint)
		logger.Info("skip get replicate checkpoint for pchannel-increasing task, use initialized checkpoint",
			zap.Stringer("messageID", initializedCheckpoint.MessageID),
			zap.Uint64("timeTick", initializedCheckpoint.TimeTick),
		)
		return initializedCheckpoint, nil
	}

	ctx, cancel := context.WithTimeout(r.asyncNotifier.Context(), 30*time.Second)
	defer cancel()

	sourceClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	req := &milvuspb.GetReplicateInfoRequest{
		SourceClusterId: sourceClusterID,
		TargetPchannel:  r.channel.Value.GetTargetChannelName(),
	}
	replicateInfo, err := r.targetClient.GetReplicateInfo(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get replicate info")
	}

	checkpoint := replicateInfo.GetCheckpoint()
	if checkpoint == nil || checkpoint.MessageId == nil {
		initializedCheckpoint := utility.NewReplicateCheckpointFromProto(r.channel.Value.InitializedCheckpoint)
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

func (r *channelReplicator) StopReplication() {
	r.asyncNotifier.Cancel()
	r.asyncNotifier.BlockUntilFinish()
}
