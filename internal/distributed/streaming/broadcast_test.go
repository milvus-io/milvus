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

package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	clientbroadcast "github.com/milvus-io/milvus/internal/streamingcoord/client/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ackTestClient struct{ client.Client }

func (*ackTestClient) Broadcast() client.BroadcastService { return nil }

func TestBroadcastAckSingleAttemptUsesCallerContext(t *testing.T) {
	service := &clientbroadcast.GRPCBroadcastServiceImpl{}
	clientPatch := mockey.Mock((*ackTestClient).Broadcast).Return(service).Build()
	defer clientPatch.UnPatch()
	for _, mode := range []string{"success", "caller-deadline", "failure", "timeout", "caller-canceled", "closed"} {
		t.Run(mode, func(t *testing.T) {
			b := broadcast{&walAccesserImpl{lifetime: typeutil.NewLifetime(), streamingCoordClient: &ackTestClient{}}}
			var ctx context.Context
			var cancel context.CancelFunc
			switch mode {
			case "caller-deadline":
				ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			case "timeout":
				ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
			default:
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			if mode == "caller-canceled" {
				cancel()
			}
			if mode == "closed" {
				b.lifetime.SetState(typeutil.LifetimeStateStopped)
			}
			patch := mockey.Mock((*clientbroadcast.GRPCBroadcastServiceImpl).Ack).To(
				func(_ *clientbroadcast.GRPCBroadcastServiceImpl, callCtx context.Context, _ message.ImmutableMessage) error {
					require.Same(t, ctx, callCtx, "client must not add or shorten the caller's deadline")
					switch mode {
					case "success", "caller-deadline":
						return nil
					case "failure":
						return context.DeadlineExceeded
					default:
						<-callCtx.Done()
						return callCtx.Err()
					}
				}).Build()
			defer patch.UnPatch()
			err := b.Ack(ctx, nil)
			switch mode {
			case "success", "caller-deadline":
				require.NoError(t, err)
			case "closed":
				require.ErrorIs(t, err, ErrWALAccesserClosed)
			case "caller-canceled":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if mode == "closed" {
				require.Zero(t, patch.Times())
			} else {
				require.Equal(t, 1, patch.Times())
			}
			b.lifetime.SetState(typeutil.LifetimeStateStopped)
			b.lifetime.Wait()
		})
	}
}
