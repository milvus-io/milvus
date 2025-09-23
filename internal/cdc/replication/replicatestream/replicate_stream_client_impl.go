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

package replicatestream

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

const pendingMessageQueueLength = 128

// replicateStreamClient is the implementation of ReplicateStreamClient.
type replicateStreamClient struct {
	replicateInfo *streamingpb.ReplicatePChannelMeta

	clusterID       string
	client          milvuspb.MilvusService_CreateReplicateStreamClient
	pendingMessages MsgQueue
	metrics         ReplicateMetrics

	ctx        context.Context
	cancel     context.CancelFunc
	finishedCh chan struct{}
}

// NewReplicateStreamClient creates a new ReplicateStreamClient.
func NewReplicateStreamClient(ctx context.Context, replicateInfo *streamingpb.ReplicatePChannelMeta) ReplicateStreamClient {
	ctx1, cancel := context.WithCancel(ctx)
	ctx1 = contextutil.WithClusterID(ctx1, replicateInfo.GetTargetCluster().GetClusterId())

	rs := &replicateStreamClient{
		clusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		replicateInfo:   replicateInfo,
		pendingMessages: NewMsgQueue(pendingMessageQueueLength),
		metrics:         NewReplicateMetrics(replicateInfo),
		ctx:             ctx1,
		cancel:          cancel,
		finishedCh:      make(chan struct{}),
	}

	rs.metrics.OnConnect()
	go rs.startInternal()
	return rs
}

func (r *replicateStreamClient) startInternal() {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)

	defer func() {
		r.metrics.OnDisconnect()
		logger.Info("replicate stream client closed")
		close(r.finishedCh)
	}()

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	for {
		// Create a local context for this connection that can be canceled
		// when we need to stop the send/recv loops
		connCtx, connCancel := context.WithCancel(r.ctx)

		milvusClient, err := resource.Resource().ClusterClient().CreateMilvusClient(connCtx, r.replicateInfo.GetTargetCluster())
		if err != nil {
			logger.Warn("create milvus client failed, retry...", zap.Error(err))
			time.Sleep(backoff.NextBackOff())
			continue
		}
		client, err := milvusClient.CreateReplicateStream(connCtx)
		if err != nil {
			logger.Warn("create milvus replicate stream failed, retry...", zap.Error(err))
			time.Sleep(backoff.NextBackOff())
			continue
		}
		logger.Info("replicate stream client service started")

		// reset client and pending messages
		r.client = client
		r.pendingMessages.SeekToHead()

		sendCh := r.startSendLoop(connCtx)
		recvCh := r.startRecvLoop(connCtx)

		select {
		case <-r.ctx.Done():
		case <-sendCh:
		case <-recvCh:
		}

		connCancel() // Cancel the connection context
		<-sendCh
		<-recvCh // wait for send/recv loops to exit

		if r.ctx.Err() != nil {
			logger.Info("close replicate stream client by ctx done")
			return
		} else {
			logger.Warn("restart replicate stream client")
			r.metrics.OnDisconnect()
			time.Sleep(backoff.NextBackOff())
		}
	}
}

// Replicate replicates the message to the target cluster.
func (r *replicateStreamClient) Replicate(msg message.ImmutableMessage) error {
	select {
	case <-r.ctx.Done():
		return nil
	default:
		r.metrics.StartReplicate(msg)
		r.pendingMessages.Enqueue(r.ctx, msg)
		return nil
	}
}

func (r *replicateStreamClient) startSendLoop(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = r.sendLoop(ctx)
		close(ch)
	}()
	return ch
}

func (r *replicateStreamClient) startRecvLoop(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = r.recvLoop(ctx)
		close(ch)
	}()
	return ch
}

func (r *replicateStreamClient) sendLoop(ctx context.Context) (err error) {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)
	defer func() {
		if err != nil {
			logger.Warn("send loop closed by unexpected error", zap.Error(err))
		} else {
			logger.Info("send loop closed")
		}
		r.client.CloseSend()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := r.pendingMessages.ReadNext(ctx)
			if err != nil {
				// context canceled, return nil
				return nil
			}
			if msg.MessageType() == message.MessageTypeTxn {
				txnMsg := message.AsImmutableTxnMessage(msg)

				// send txn begin message
				beginMsg := txnMsg.Begin()
				err := r.sendMessage(beginMsg)
				if err != nil {
					return err
				}

				// send txn messages
				err = txnMsg.RangeOver(func(msg message.ImmutableMessage) error {
					return r.sendMessage(msg)
				})
				if err != nil {
					return err
				}

				// send txn commit message
				commitMsg := txnMsg.Commit()
				err = r.sendMessage(commitMsg)
				if err != nil {
					return err
				}
			} else {
				err = r.sendMessage(msg)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (r *replicateStreamClient) sendMessage(msg message.ImmutableMessage) (err error) {
	defer func() {
		logger := log.With(
			zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
			zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
		)
		if err != nil {
			logger.Warn("send message failed", zap.Error(err), log.FieldMessage(msg))
		} else {
			r.metrics.OnSent(msg)
			logger.Debug("send message success", log.FieldMessage(msg))
		}
	}()
	immutableMessage := msg.IntoImmutableMessageProto()
	req := &milvuspb.ReplicateRequest{
		Request: &milvuspb.ReplicateRequest_ReplicateMessage{
			ReplicateMessage: &milvuspb.ReplicateMessage{
				SourceClusterId: r.clusterID,
				Message: &commonpb.ImmutableMessage{
					Id:         msg.MessageID().IntoProto(),
					Payload:    immutableMessage.GetPayload(),
					Properties: immutableMessage.GetProperties(),
				},
			},
		},
	}
	return r.client.Send(req)
}

func (r *replicateStreamClient) recvLoop(ctx context.Context) (err error) {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)
	defer func() {
		if err != nil {
			logger.Warn("recv loop closed by unexpected error", zap.Error(err))
		} else {
			logger.Info("recv loop closed")
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := r.client.Recv()
			if err != nil {
				logger.Warn("replicate stream recv failed", zap.Error(err))
				return err
			}
			lastConfirmedMessageInfo := resp.GetReplicateConfirmedMessageInfo()
			if lastConfirmedMessageInfo != nil {
				messages := r.pendingMessages.CleanupConfirmedMessages(lastConfirmedMessageInfo.GetConfirmedTimeTick())
				for _, msg := range messages {
					if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
						roleChanged := r.handleAlterReplicateConfigMessage(msg)
						if roleChanged {
							// Role changed, return and stop replicate.
							return nil
						}
					}
					r.metrics.OnConfirmed(msg)
				}
			}
		}
	}
}

func (r *replicateStreamClient) handleAlterReplicateConfigMessage(msg message.ImmutableMessage) (roleChanged bool) {
	logger := log.With(
		zap.String("sourceChannel", r.replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", r.replicateInfo.GetTargetChannelName()),
	)
	logger.Info("handle AlterReplicateConfigMessage", log.FieldMessage(msg))
	prcMsg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
	replicateConfig := prcMsg.Header().ReplicateConfiguration
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentCluster := replicateutil.MustNewConfigHelper(currentClusterID, replicateConfig).GetCurrentCluster()
	_, err := currentCluster.GetTargetChannel(r.replicateInfo.GetSourceChannelName(),
		r.replicateInfo.GetTargetCluster().GetClusterId())
	if err != nil {
		// Cannot find the target channel, it means that the `current->target` topology edge is removed,
		// so we need to remove the replicate pchannel and stop replicate.
		err := resource.Resource().ReplicationCatalog().RemoveReplicatePChannel(r.ctx, r.replicateInfo)
		if err != nil {
			panic(fmt.Sprintf("failed to remove replicate pchannel: %v", err))
		}
		logger.Info("handle AlterReplicateConfigMessage done, replicate pchannel removed")
		return true
	}
	logger.Info("target channel found, skip handle AlterReplicateConfigMessage")
	return false
}

func (r *replicateStreamClient) Close() {
	r.cancel()
	<-r.finishedCh
}
