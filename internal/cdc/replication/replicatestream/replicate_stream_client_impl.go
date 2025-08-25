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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/cenkalti/backoff/v4"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const pendingMessageQueueLength = 128

// replicateStreamClient is the implementation of ReplicateStreamClient.
type replicateStreamClient struct {
	targetCluster *milvuspb.MilvusCluster
	targetChannel string
	walName       commonpb.WALName

	client          milvuspb.MilvusService_CreateReplicateStreamClient
	pendingMessages MsgQueue

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReplicateStreamClient creates a new ReplicateStreamClient.
func NewReplicateStreamClient(ctx context.Context, targetCluster *milvuspb.MilvusCluster, targetChannel string) ReplicateStreamClient {
	ctx1, cancel := context.WithCancel(ctx)
	ctx1 = contextutil.WithClusterID(ctx1, targetCluster.GetClusterId())

	walNameStr := streaming.WAL().WALName()
	walName := message.GetWALName(walNameStr)

	rs := &replicateStreamClient{
		targetCluster:   targetCluster,
		targetChannel:   targetChannel,
		walName:         walName,
		pendingMessages: NewMsgQueue(pendingMessageQueueLength),
		ctx:             ctx1,
		cancel:          cancel,
	}

	go rs.startInternal()
	return rs
}

func (r *replicateStreamClient) startInternal() {
	logger := log.With(zap.String("targetChannel", r.targetChannel))

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	for {
		select {
		case <-r.ctx.Done():
			logger.Info("replicate stream client closed by ctx done")
			return
		default:
			milvusClient, err := resource.Resource().ClusterClient().CreateMilvusClient(r.ctx, r.targetCluster)
			if err != nil {
				logger.Warn("create milvus client failed, retry...", zap.Error(err))
				time.Sleep(backoff.NextBackOff())
				continue
			}
			client, err := milvusClient.CreateReplicateStream(r.ctx)
			if err != nil {
				logger.Warn("create milvus replicate stream failed, retry...", zap.Error(err))
				time.Sleep(backoff.NextBackOff())
				continue
			}

			// reset client and pending messages
			r.client = client
			r.pendingMessages.SeekToHead()

			stopCh := make(chan struct{})
			sendErrCh := r.startSendLoop(stopCh)
			recvErrCh := r.startRecvLoop(stopCh)

			select {
			case <-r.ctx.Done():
				r.client.CloseSend()
				r.wg.Wait()
				logger.Info("replicate stream client closed by ctx done")
				return
			case err := <-sendErrCh:
				close(stopCh)
				r.client.CloseSend()
				r.wg.Wait()
				time.Sleep(backoff.NextBackOff())
				log.Warn("restart stream client due to send error", zap.Error(err))
			case err := <-recvErrCh:
				close(stopCh)
				r.client.CloseSend()
				r.wg.Wait()
				time.Sleep(backoff.NextBackOff())
				log.Warn("restart stream client due to recv error", zap.Error(err))
			}
		}
	}
}

func (r *replicateStreamClient) immutableMessageToProto(msg message.ImmutableMessage) *milvuspb.ReplicateRequest {
	immutableMessage := msg.IntoImmutableMessageProto()
	return &milvuspb.ReplicateRequest{
		Request: &milvuspb.ReplicateRequest_ReplicateMessage{
			ReplicateMessage: &milvuspb.ReplicateMessage{
				Message: &milvuspb.ImmutableMessage{
					Id: &milvuspb.MessageID{
						Id:      immutableMessage.GetId().GetId(),
						WALName: r.walName,
					},
					Payload:    immutableMessage.GetPayload(),
					Properties: immutableMessage.GetProperties(),
				},
			},
		},
	}
}

// Replicate replicates the message to the target cluster.
func (r *replicateStreamClient) Replicate(msg message.ImmutableMessage) error {
	select {
	case <-r.ctx.Done():
		return nil
	default:
		r.pendingMessages.Enqueue(r.ctx, msg)
		return nil
	}
}

func (r *replicateStreamClient) startSendLoop(stopCh <-chan struct{}) <-chan error {
	errCh := make(chan error, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		errCh <- r.sendLoop(stopCh)
	}()
	return errCh
}

func (r *replicateStreamClient) startRecvLoop(stopCh <-chan struct{}) <-chan error {
	errCh := make(chan error, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		errCh <- r.recvLoop(stopCh)
	}()
	return errCh
}

func (r *replicateStreamClient) sendLoop(stopCh <-chan struct{}) error {
	logger := log.With(zap.String("targetChannel", r.targetChannel))
	for {
		select {
		case <-r.ctx.Done():
			logger.Info("send loop closed by ctx done")
			return nil
		case <-stopCh:
			logger.Info("send loop closed by stopCh")
			return nil
		default:
			msg, err := r.pendingMessages.Dequeue(r.ctx)
			if err != nil {
				// context canceled, return nil
				return nil
			}
			req := r.immutableMessageToProto(msg)
			err = r.client.Send(req)
			if err != nil {
				logger.Warn("replicate stream send failed", zap.Error(err))
				return err
			}
		}
	}
}

func (r *replicateStreamClient) recvLoop(stopCh <-chan struct{}) error {
	logger := log.With(zap.String("targetChannel", r.targetChannel))
	for {
		select {
		case <-r.ctx.Done():
			logger.Info("recv loop closed by ctx done")
			return nil
		case <-stopCh:
			logger.Info("recv loop closed by stopCh")
			return nil
		default:
			resp, err := r.client.Recv()
			if err != nil {
				logger.Warn("replicate stream recv failed", zap.Error(err))
				return err
			}
			lastConfirmedMessageInfo := resp.GetReplicateConfirmedMessageInfo()
			// TODO: sheep, add metrics
			if lastConfirmedMessageInfo != nil {
				r.pendingMessages.CleanupConfirmedMessages(lastConfirmedMessageInfo.GetConfirmedTimeTick())
			}
		}
	}
}

func (r *replicateStreamClient) Close() {
	r.cancel()
	r.wg.Wait()
}
