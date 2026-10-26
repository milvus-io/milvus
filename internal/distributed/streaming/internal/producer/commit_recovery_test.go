package producer_test

import (
	"context"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	handlerproducer "github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Inject response loss ABOVE successful server Commit handling. Exercise the
// real producer retry loop and TimeTick interceptor, not backend Append retries.
func TestLostCommitResponseDoesNotReplayTransaction(t *testing.T) {
	for _, mode := range []string{"before cleanup", "after cleanup", "result evicted"} {
		for _, cas := range []bool{false, true} {
			name := mode
			if cas {
				name += "/CAS"
			}
			t.Run(name, func(t *testing.T) {
				resource.InitForTest(t)
				params := paramtable.Get()
				if mode == "result evicted" {
					require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxCount.Key, "0"))
					defer params.Reset(params.StreamingCfg.TxnTombstoneMaxCount.Key)
				}
				manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
				floor := walimplstest.NewTestMessageID(0)
				barrier := timetick.NewTimeTickMsg(1, floor, 0, true).IntoImmutableMessage(floor)
				server := timetick.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
					ChannelInfo: types.PChannelInfo{Name: t.Name()}, LastTimeTickMessage: barrier, LastConfirmedMessageID: floor,
					TxnManager: manager, MVCCManager: mvcc.NewMVCCManager(1),
				})
				defer server.Close()
				var mu sync.Mutex
				var sequence int64
				begins, commits, attempts := 0, 0, 0
				var original *types.AppendResult
				transport := &partialUpdateCASRejectProducer{}
				patch := mockey.Mock((*partialUpdateCASRejectProducer).Append).To(func(_ *partialUpdateCASRejectProducer, ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
					extra := &utility.ExtraAppendResult{}
					id, err := server.DoAppend(utility.WithExtraAppendResult(ctx, extra), msg, func(_ context.Context, m message.MutableMessage) (message.MessageID, error) {
						mu.Lock()
						defer mu.Unlock()
						sequence++
						if m.MessageType() == message.MessageTypeBeginTxn {
							begins++
						}
						if m.MessageType() == message.MessageTypeCommitTxn {
							commits++
						}
						return walimplstest.NewTestMessageID(sequence), nil
					})
					if err != nil {
						return nil, err
					}
					result := &types.AppendResult{MessageID: id, TimeTick: extra.TimeTick, LastConfirmedMessageID: extra.LastConfirmedMessageID, TxnCtx: extra.TxnCtx}
					if msg.MessageType() == message.MessageTypeCommitTxn {
						attempts++
						if attempts == 1 {
							original = result
							if mode != "before cleanup" {
								manager.CleanupTxnUntil(extra.TimeTick + 1)
							}
							return nil, status.NewUnknownError("injected: successful Commit response lost")
						}
					}
					return result, nil
				}).Build()
				defer patch.UnPatch()
				available := mockey.Mock((*partialUpdateCASRejectProducer).IsAvailable).Return(true).Build()
				defer available.UnPatch()
				wait := mockey.Mock((*partialUpdateCASRejectProducer).Available).Return(make(chan struct{})).Build()
				defer wait.UnPatch()
				closePatch := mockey.Mock((*partialUpdateCASRejectProducer).Close).Return().Build()
				defer closePatch.UnPatch()
				rp := producer.NewResumableProducer(func(context.Context, *handler.ProducerOptions) (handlerproducer.Producer, error) {
					return transport, nil
				}, &producer.ProducerOptions{PChannel: t.Name()})
				defer rp.Close()
				msgs := []message.MutableMessage{createRealInsertMessage(t, "test-v"), createRealInsertMessage(t, "test-v")}
				if cas {
					msgs = []message.MutableMessage{createRealPartialUpdateInsertMessage(t, "test-v")}
				}
				guard, err := rp.BeginProduce(context.Background(), msgs...)
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				responses := producer.BatchCommitProduce(ctx, guard)
				result, err := responses.Responses[0].AppendResult, responses.Responses[0].Error
				if mode == "result evicted" {
					require.ErrorContains(t, err, "not found")
					require.True(t, status.AsStreamingError(err).IsUnrecoverable())
					require.False(t, status.AsStreamingError(err).IsTxnExpired())
					require.False(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS())
				} else {
					require.NoError(t, err)
					require.Equal(t, original, result)
				}
				require.Equal(t, 1, begins, "must not allocate a second TxnID")
				require.Equal(t, 1, commits, "exactly one logical Commit")
			})
		}
	}
}

type partialUpdateCASRejectProducer struct{ handlerproducer.Producer }

// Business decisions must be identical with and without a preceding transport
// failure. The real server distinguishes a live, expired session from no record.
func TestCommitRetryUsesServerOutcome(t *testing.T) {
	for _, outcome := range []string{"expired", "not found"} {
		for _, disconnect := range []bool{false, true} {
			for _, cas := range []bool{false, true} {
				name := outcome
				if disconnect {
					name += "/disconnected"
				}
				if cas {
					name += "/CAS"
				}
				t.Run(name, func(t *testing.T) {
					resource.InitForTest(t)
					manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
					defer manager.FailTxnAtVChannel("")
					floor := walimplstest.NewTestMessageID(0)
					barrier := timetick.NewTimeTickMsg(1, floor, 0, true).IntoImmutableMessage(floor)
					server := timetick.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
						ChannelInfo: types.PChannelInfo{Name: t.Name()}, LastTimeTickMessage: barrier, LastConfirmedMessageID: floor,
						TxnManager: manager, MVCCManager: mvcc.NewMVCCManager(1),
					})
					defer server.Close()
					var mu sync.Mutex
					var firstTxn message.TxnID
					var attempts []message.TxnID
					var sequence int64
					begins, commits := 0, 0
					var serverErr error
					transport := &partialUpdateCASRejectProducer{}
					patch := mockey.Mock((*partialUpdateCASRejectProducer).Append).To(func(_ *partialUpdateCASRejectProducer, ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
						if msg.MessageType() == message.MessageTypeCommitTxn {
							id := msg.TxnContext().TxnID
							attempts = append(attempts, id)
							if firstTxn == 0 {
								firstTxn = id
							}
							if disconnect && len(attempts) == 1 {
								return nil, io.ErrUnexpectedEOF
							}
							if id == firstTxn {
								if outcome == "expired" {
									session, err := manager.GetSessionOfTxn(id)
									require.NoError(t, err)
									require.True(t, status.AsStreamingError(session.AddNewMessage(ctx, math.MaxUint64)).IsTxnExpired())
								} else {
									manager.FailTxnAtVChannel(msg.VChannel())
								}
							}
						}
						extra := &utility.ExtraAppendResult{}
						id, err := server.DoAppend(utility.WithExtraAppendResult(ctx, extra), msg, func(_ context.Context, m message.MutableMessage) (message.MessageID, error) {
							mu.Lock()
							defer mu.Unlock()
							sequence++
							if m.MessageType() == message.MessageTypeBeginTxn {
								begins++
							}
							if m.MessageType() == message.MessageTypeCommitTxn {
								commits++
							}
							return walimplstest.NewTestMessageID(sequence), nil
						})
						if err != nil {
							serverErr = err
							return nil, err
						}
						return &types.AppendResult{MessageID: id, TimeTick: extra.TimeTick, LastConfirmedMessageID: extra.LastConfirmedMessageID, TxnCtx: extra.TxnCtx}, nil
					}).Build()
					defer patch.UnPatch()
					available := mockey.Mock((*partialUpdateCASRejectProducer).IsAvailable).Return(true).Build()
					defer available.UnPatch()
					wait := mockey.Mock((*partialUpdateCASRejectProducer).Available).Return(make(chan struct{})).Build()
					defer wait.UnPatch()
					closePatch := mockey.Mock((*partialUpdateCASRejectProducer).Close).Return().Build()
					defer closePatch.UnPatch()
					p := producer.NewResumableProducer(func(context.Context, *handler.ProducerOptions) (handlerproducer.Producer, error) {
						return transport, nil
					}, &producer.ProducerOptions{PChannel: t.Name()})
					defer p.Close()
					msgs := []message.MutableMessage{createRealInsertMessage(t, "test-v"), createRealInsertMessage(t, "test-v")}
					if cas {
						msgs = []message.MutableMessage{createRealPartialUpdateInsertMessage(t, "test-v")}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					guard, err := p.BeginProduce(ctx, msgs...)
					require.NoError(t, err)
					response := producer.BatchCommitProduce(ctx, guard).Responses[0]
					if outcome == "not found" {
						require.ErrorContains(t, response.Error, "not found")
						require.ErrorIs(t, response.Error, serverErr)
						require.False(t, status.AsStreamingError(response.Error).IsTxnExpired())
						require.True(t, status.AsStreamingError(response.Error).IsUnrecoverable())
						require.Equal(t, 1, begins)
						require.Zero(t, commits)
					} else if cas {
						require.True(t, status.AsStreamingError(response.Error).IsPartialUpdateRetryableCAS(), "%v", response.Error)
						require.Equal(t, 1, begins)
						require.Zero(t, commits)
					} else {
						require.NoError(t, response.Error)
						require.Equal(t, 2, begins)
						require.Equal(t, 1, commits)
					}
					if disconnect {
						require.GreaterOrEqual(t, len(attempts), 2)
						require.Equal(t, attempts[0], attempts[1], "transport retry must resend the same Commit")
					}
				})
			}
		}
	}
}

func createRealInsertMessage(t *testing.T, vchannel string) message.MutableMessage {
	t.Helper()
	return message.NewInsertMessageBuilderV1().WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1}).WithVChannel(vchannel).MustBuildMutable()
}

func createRealPartialUpdateInsertMessage(t *testing.T, vchannel string) message.MutableMessage {
	t.Helper()
	builder := message.NewInsertMessageBuilderV1().WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1}).WithVChannel(vchannel)
	require.NoError(t, builder.AddPartialUpdateCAS(&messagespb.PartialUpdateCAS{ReadTs: 100, ObservedPchannelTerm: 2}))
	return builder.MustBuildMutable()
}
