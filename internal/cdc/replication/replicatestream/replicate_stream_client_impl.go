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
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/cdc/util"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	// TODO: sheep, make these parameters configurable
	pendingMessageQueueLength  = 128
	pendingMessageQueueMaxSize = 128 * 1024 * 1024
)

var ErrReplicationRemoved = errors.New("replication removed")

// replicateStreamClient is the implementation of ReplicateStreamClient.
type replicateStreamClient struct {
	clusterID       string
	targetClient    cluster.MilvusClient
	client          milvuspb.MilvusService_CreateReplicateStreamClient
	channel         *meta.ReplicateChannel
	pendingMessages MsgQueue
	metrics         ReplicateMetrics

	ctx        context.Context
	cancel     context.CancelFunc
	finishedCh chan struct{}
}

// NewReplicateStreamClient creates a new ReplicateStreamClient.
func NewReplicateStreamClient(ctx context.Context, c cluster.MilvusClient, channel *meta.ReplicateChannel) ReplicateStreamClient {
	ctx1, cancel := context.WithCancel(ctx)
	ctx1 = contextutil.WithClusterID(ctx1, channel.Value.GetTargetCluster().GetClusterId())

	options := MsgQueueOptions{
		Capacity: pendingMessageQueueLength,
		MaxSize:  pendingMessageQueueMaxSize,
	}
	pendingMessages := NewMsgQueue(options)
	rs := &replicateStreamClient{
		clusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		targetClient:    c,
		channel:         channel,
		pendingMessages: pendingMessages,
		metrics:         NewReplicateMetrics(channel.Value),
		ctx:             ctx1,
		cancel:          cancel,
		finishedCh:      make(chan struct{}),
	}

	rs.metrics.OnInitiate()
	go rs.startInternal()
	return rs
}

func (r *replicateStreamClient) startInternal() {
	defer func() {
		log.Info("replicate stream client closed",
			zap.String("key", r.channel.Key),
			zap.Int64("revision", r.channel.ModRevision))
		r.metrics.OnClose()
		close(r.finishedCh)
	}()

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	backoff.MaxElapsedTime = 0

	for {
		restart := r.startReplicating(backoff)
		if !restart {
			return
		}
		time.Sleep(backoff.NextBackOff())
	}
}

func (r *replicateStreamClient) startReplicating(backoff backoff.BackOff) (needRestart bool) {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("revision", r.channel.ModRevision))
	if r.ctx.Err() != nil {
		logger.Info("close replicate stream client due to ctx done")
		return false
	}

	// Create a local context for this connection that can be canceled
	// when we need to stop the send/recv loops
	connCtx, connCancel := context.WithCancel(r.ctx)
	defer connCancel()

	client, err := r.targetClient.CreateReplicateStream(connCtx)
	if err != nil {
		logger.Warn("create milvus replicate stream failed, retry...", zap.Error(err))
		return true
	}
	defer client.CloseSend()

	logger.Info("replicate stream client service started")
	r.metrics.OnConnect()
	backoff.Reset()

	// reset client and pending messages
	r.client = client
	r.pendingMessages.SeekToHead()

	sendCh := r.startSendLoop(connCtx)
	recvCh := r.startRecvLoop(connCtx)

	var chErr error
	select {
	case <-r.ctx.Done():
	case chErr = <-sendCh:
	case chErr = <-recvCh:
	}

	connCancel() // Cancel the connection context
	<-sendCh
	<-recvCh // wait for send/recv loops to exit

	if r.ctx.Err() != nil {
		logger.Info("close replicate stream client due to ctx done")
		return false
	} else if errors.Is(chErr, ErrReplicationRemoved) {
		logger.Info("close replicate stream client due to replication removed")
		return false
	} else {
		logger.Warn("restart replicate stream client due to unexpected error", zap.Error(chErr))
		r.metrics.OnDisconnect()
		return true
	}
}

// Replicate replicates the message to the target cluster.
func (r *replicateStreamClient) Replicate(msg message.ImmutableMessage) error {
	select {
	case <-r.ctx.Done():
		return nil
	default:
		// TODO: Should be done at streamingnode, but after move it into streamingnode, the metric need to be adjusted.
		if msg.MessageType().IsSelfControlled() {
			if r.pendingMessages.Len() == 0 {
				// if there is no pending messages, there's no lag between source and target.
				r.metrics.OnNoIncomingMessages()
			}
			return ErrReplicateIgnored
		}
		r.metrics.StartReplicate(msg)
		r.pendingMessages.Enqueue(r.ctx, msg)
		return nil
	}
}

func (r *replicateStreamClient) startSendLoop(ctx context.Context) <-chan error {
	ch := make(chan error)
	go func() {
		err := r.sendLoop(ctx)
		ch <- err
		close(ch)
	}()
	return ch
}

func (r *replicateStreamClient) startRecvLoop(ctx context.Context) <-chan error {
	ch := make(chan error)
	go func() {
		err := r.recvLoop(ctx)
		ch <- err
		close(ch)
	}()
	return ch
}

func (r *replicateStreamClient) sendLoop(ctx context.Context) (err error) {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("revision", r.channel.ModRevision))
	defer func() {
		if err != nil {
			logger.Warn("send loop closed by unexpected error", zap.Error(err))
		} else {
			logger.Info("send loop closed")
		}
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
		logger := log.With(zap.String("key", r.channel.Key), zap.Int64("revision", r.channel.ModRevision))
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
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("revision", r.channel.ModRevision))
	defer func() {
		if err != nil && !errors.Is(err, ErrReplicationRemoved) {
			logger.Warn("recv loop closed by unexpected error", zap.Error(err))
		} else {
			logger.Info("recv loop closed", zap.Error(err))
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
					r.metrics.OnConfirmed(msg)
					if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
						replicationRemoved := r.handleAlterReplicateConfigMessage(msg)
						if replicationRemoved {
							// Replication removed, return and stop replicate.
							return ErrReplicationRemoved
						}
					}
				}
			}
		}
	}
}

func (r *replicateStreamClient) handleAlterReplicateConfigMessage(msg message.ImmutableMessage) (replicationRemoved bool) {
	logger := log.With(zap.String("key", r.channel.Key), zap.Int64("revision", r.channel.ModRevision))
	logger.Info("handle AlterReplicateConfigMessage", log.FieldMessage(msg))

	replicationRemoved = util.IsReplicationRemovedByAlterReplicateConfigMessage(msg, r.channel.Value)
	if replicationRemoved {
		// Cannot find the target channel, it means that the `current->target` topology edge is removed,
		// so we need to remove the replicate pchannel and stop replicate.
		etcdCli := resource.Resource().ETCD()
		ok, err := meta.RemoveReplicatePChannelWithRevision(r.ctx, etcdCli, r.channel.Key, r.channel.ModRevision)
		if err != nil {
			logger.Warn("failed to remove replicate pchannel", zap.Error(err))
			// When performing delete operation on etcd, the context may be canceled by the delete event
			// in cdc controller and then return `context.Canceled` error.
			// Since the delete event is generated after the delete operation is committed in etcd,
			// the delete is guaranteed to have succeeded on the server side.
			// So we can ignore the context canceled error here.
			if !errors.Is(err, context.Canceled) {
				panic(fmt.Sprintf("failed to remove replicate pchannel: %v", err))
			}
		}
		if ok {
			logger.Info("handle AlterReplicateConfigMessage done, replicate pchannel removed")
		} else {
			logger.Info("handle AlterReplicateConfigMessage done, revision not match, replicate pchannel not removed")
		}
		return true
	}
	logger.Info("target channel found, skip handle AlterReplicateConfigMessage")
	return false
}

func (r *replicateStreamClient) BlockUntilFinish() {
	<-r.finishedCh
}

func (r *replicateStreamClient) Close() {
	r.cancel()
	<-r.finishedCh
}
