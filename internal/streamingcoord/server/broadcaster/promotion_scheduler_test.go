package broadcaster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type promotionTestCatalog struct {
	metastore.StreamingCoordCataLog
}

// Exercise the actual Broadcast/Execute/ACK/callback path with all ordinary
// workers blocked inside AppendMessages. The WAL stub models an unfinished
// replicated transaction that only a successful ForcePromote append can end.
func TestForcePromoteWithSaturatedBroadcastWorkers(t *testing.T) {
	for _, workers := range []int{1, 3} {
		for _, recovered := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/recovered=%t", workers, recovered), func(t *testing.T) {
				paramtable.Init()
				cfg := &paramtable.Get().StreamingCfg.WALBroadcasterConcurrencyRatio
				old := cfg.SwapTempValue(fmt.Sprint(workers))
				defer cfg.SwapTempValue(old)
				cpu := mockey.Mock(hardware.GetCPUNum).Return(1).Build()
				defer cpu.UnPatch()
				registry.ResetRegistration()
				defer registry.ResetRegistration()
				registry.RegisterAlterReplicateConfigV2AckCallback(func(context.Context, message.BroadcastResultAlterReplicateConfigMessageV2) error { return nil })
				catalog := &promotionTestCatalog{}
				resource.InitForTest(resource.OptStreamingCatalog(catalog))
				save := mockey.Mock((*promotionTestCatalog).SaveBroadcastTask).Return(nil).Build()
				defer save.UnPatch()
				wal := &localFlushWAL{}
				patchWAL := mockey.Mock(streaming.WAL).Return(wal).Build()
				defer patchWAL.UnPatch()
				control := mockey.Mock((*localFlushWAL).ControlChannel).Return("p0_vcchan").Build()
				defer control.UnPatch()
				allocator := mockey.Mock(mockey.GetMethod(resource.Resource(), "IDAllocator")).Return(&localFlushAllocator{}).Build()
				defer allocator.UnPatch()
				var ids atomic.Uint64
				allocate := mockey.Mock((*localFlushAllocator).Allocate).To(func(_ *localFlushAllocator, _ context.Context) (uint64, error) { return ids.Add(1), nil }).Build()
				defer allocate.UnPatch()
				secondary := mockey.Mock((*broadcastTaskManager).checkClusterRoleSecondary).Return(nil).Build()
				defer secondary.UnPatch()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				entered := make(chan struct{}, workers)
				release := make(chan struct{})
				var releaseOnce sync.Once
				var attempts atomic.Int32
				var ticks atomic.Uint64
				manager := syncutil.NewFuture[*broadcastTaskManager]()
				appendPatch := mockey.Mock((*localFlushWAL).AppendMessages).To(func(_ *localFlushWAL, appendCtx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
					responses := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
					if msgs[0].MessageType() == message.MessageTypeManualFlush {
						entered <- struct{}{}
						select {
						case <-release:
						case <-appendCtx.Done():
							for i := range responses.Responses {
								responses.Responses[i].Error = appendCtx.Err()
							}
							return responses
						}
					} else if attempts.Add(1) == 1 {
						if recovered {
							for i := 0; i < workers; i++ {
								select {
								case <-entered:
								case <-appendCtx.Done():
									for j := range responses.Responses {
										responses.Responses[j].Error = appendCtx.Err()
									}
									return responses
								}
							}
						}
						// A failed promotion must retry on the reserved worker,
						// even when ordinary workers remain saturated.
						for i := range responses.Responses {
							responses.Responses[i].Error = context.DeadlineExceeded
						}
						return responses
					} else {
						releaseOnce.Do(func() { close(release) })
					}
					for i, msg := range msgs {
						tick := ticks.Add(1)
						id := walimplstest.NewTestMessageID(int64(tick))
						responses.Responses[i].AppendResult = &types.AppendResult{MessageID: id, LastConfirmedMessageID: id, TimeTick: tick}
						immutable := msg.WithTimeTick(tick).WithLastConfirmed(id).IntoImmutableMessage(id)
						require.NoError(t, manager.Get().Ack(appendCtx, immutable))
					}
					return responses
				}).Build()
				defer appendPatch.UnPatch()

				flushMsg := func() message.BroadcastMutableMessage {
					return message.NewManualFlushMessageBuilderV2().WithUnreplicable().
						WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).WithBody(&message.ManualFlushMessageBody{}).
						WithBroadcast([]string{"p0_1v0"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
				}
				promoteMsg := message.NewAlterReplicateConfigMessageBuilderV2().
					WithHeader(&message.AlterReplicateConfigMessageHeader{ForcePromote: true}).WithBody(&message.AlterReplicateConfigMessageBody{}).
					WithBroadcast([]string{"p0"}).MustBuildBroadcast()
				var protos []*streamingpb.BroadcastTask
				if recovered {
					for i := 0; i < workers; i++ {
						msg := message.WithBroadcastControlChannel(flushMsg().OverwriteBroadcastHeader(ids.Add(1), message.NewSharedClusterResourceKey(), message.NewExclusiveCollectionNameResourceKey("db", fmt.Sprint(i))), "p0_vcchan")
						protos = append(protos, &streamingpb.BroadcastTask{Message: msg.IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING})
					}
					msg := message.WithBroadcastControlChannel(promoteMsg.OverwriteBroadcastHeader(ids.Add(1), message.NewExclusiveClusterResourceKey()), "p0_vcchan")
					// Put promotion after the flushes, as catalog order must not
					// determine whether recovery can make progress.
					protos = append(protos, &streamingpb.BroadcastTask{Message: msg.IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING})
				}
				bm := newBroadcastTaskManager(protos)
				manager.Set(bm)
				defer bm.Close()
				if !recovered {
					done := make(chan error, workers)
					for i := 0; i < workers; i++ {
						api, err := bm.WithUnreplicableResourceKeys(ctx, message.NewExclusiveCollectionNameResourceKey("db", fmt.Sprint(i)))
						require.NoError(t, err)
						go func() { defer api.Close(); _, err := api.Broadcast(ctx, flushMsg()); done <- err }()
					}
					for i := 0; i < workers; i++ {
						select {
						case <-entered:
						case <-ctx.Done():
							t.Fatal("ordinary workers did not saturate")
						}
					}
					api, err := bm.WithSecondaryClusterResourceKey(ctx)
					require.NoError(t, err)
					defer api.Close()
					_, err = api.Broadcast(ctx, promoteMsg)
					require.NoError(t, err)
					for i := 0; i < workers; i++ {
						require.NoError(t, <-done)
					}
				} else {
					require.Eventually(t, func() bool {
						bm.mu.Lock()
						defer bm.mu.Unlock()
						for _, task := range bm.tasks {
							select {
							case <-task.done:
							default:
								return false
							}
						}
						return true
					}, 5*time.Second, time.Millisecond)
				}
				require.GreaterOrEqual(t, attempts.Load(), int32(2))
				guards, err := bm.resourceKeyLocker.FastLock(message.NewExclusiveClusterResourceKey())
				require.NoError(t, err, "completed Flush tasks must release their resource locks")
				guards.Unlock()
			})
		}
	}
}
