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

	"github.com/cenkalti/backoff/v4"
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

// ErrCheckpointExpired is returned when the replicate checkpoint has been deleted
// by the message queue retention policy. The checkpoint position no longer exists
// in the source MQ, so replication cannot resume from that position.
var ErrCheckpointExpired = errors.New("replicate checkpoint has been expired by message queue retention policy")

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
		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = 100 * time.Millisecond
		bo.MaxInterval = 10 * time.Second
		bo.MaxElapsedTime = 0 // retry indefinitely
		bo.Reset()
	INIT_LOOP:
		for {
			select {
			case <-r.asyncNotifier.Context().Done():
				return
			default:
				err := r.init()
				if err != nil {
					nextInterval := bo.NextBackOff()
					logger.Warn("initialize replicator failed",
						zap.Error(err), zap.Duration("nextRetryInterval", nextInterval))
					select {
					case <-r.asyncNotifier.Context().Done():
						return
					case <-time.After(nextInterval):
					}
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
		if err := r.validateCheckpoint(cp); err != nil {
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

// validateCheckpoint checks whether the checkpoint position still exists in the source MQ.
// It creates a temporary scanner from the earliest available position and compares
// the earliest message ID with the checkpoint's message ID. If the checkpoint is older
// than the earliest available message, it means the checkpoint has been deleted by
// the MQ retention policy, and ErrCheckpointExpired is returned.
func (r *channelReplicator) validateCheckpoint(cp *utility.ReplicateCheckpoint) error {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("modRevision", r.channel.ModRevision))

	validateCtx, validateCancel := context.WithTimeout(r.asyncNotifier.Context(), 30*time.Second)
	defer validateCancel()

	// Create a temporary scanner from the earliest position to get the earliest available message ID.
	tempCh := make(adaptor.ChanMessageHandler)
	tempScanner := streaming.WAL().Read(validateCtx, streaming.ReadOption{
		PChannel:       r.channel.Value.GetSourceChannelName(),
		DeliverPolicy:  options.DeliverPolicyAll(),
		MessageHandler: tempCh,
	})
	defer func() {
		tempScanner.Close()
		// Drain remaining messages to avoid goroutine leak in the handler.
		for range tempCh {
		}
	}()

	// Wait for the first message or timeout.
	select {
	case <-validateCtx.Done():
		return errors.Wrap(validateCtx.Err(), "timeout waiting for earliest message to validate checkpoint")
	case <-tempScanner.Done():
		// Scanner terminated before delivering any message.
		if err := tempScanner.Error(); err != nil {
			return errors.Wrap(err, "failed to read earliest message for checkpoint validation")
		}
		// Scanner closed without error and no message — empty topic, checkpoint is valid.
		return nil
	case earliestMsg := <-tempCh:
		earliestID := earliestMsg.MessageID()
		if cp.MessageID.LT(earliestID) {
			logger.Warn("replicate checkpoint has been expired by MQ retention policy",
				zap.Stringer("expiredCheckpointID", cp.MessageID),
				zap.Uint64("checkpointTimeTick", cp.TimeTick),
				zap.Stringer("earliestAvailableID", earliestID),
				zap.String("pchannel", r.channel.Value.GetSourceChannelName()),
			)
			return errors.Wrapf(ErrCheckpointExpired,
				"checkpoint %s is older than earliest available message %s on channel %s",
				cp.MessageID, earliestID, r.channel.Value.GetSourceChannelName())
		}
		logger.Info("checkpoint validated, position still available in MQ",
			zap.Stringer("checkpointID", cp.MessageID),
			zap.Stringer("earliestAvailableID", earliestID),
		)
		return nil
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
