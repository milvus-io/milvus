package adaptor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// Pause the first operation after the real shard and transaction checks, while
// it still holds the WAL lifecycle lock. Admit its competitor to the outer
// chain before releasing it. Both orders must preserve the split boundary.
func TestSplitShardFenceConcurrentWithCommit(t *testing.T) {
	for _, commitFirst := range []bool{true, false} {
		name := "fence_first"
		if commitFirst {
			name = "commit_first"
		}
		t.Run(name, func(t *testing.T) {
			walimplstest.Reset()
			initResourceForTest(t)
			message.RegisterDefaultWALName(message.WALNameTest)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			firstType, secondType := message.MessageTypeSplitShard, message.MessageTypeCommitTxn
			if commitFirst {
				firstType, secondType = secondType, firstType
			}
			entered, attempted, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var enterOnce, attemptOnce, releaseOnce sync.Once
			releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
			opener, err := registry.MustGetBuilder(message.WALNameTest,
				splitAppendHook(func(ctx context.Context, msg message.MutableMessage) error {
					if msg.MessageType() == secondType {
						attemptOnce.Do(func() { close(attempted) })
					}
					return nil
				}),
				redo.NewInterceptorBuilder(), lock.NewInterceptorBuilder(),
				replicate.NewInterceptorBuilder(), timetick.NewInterceptorBuilder(), shard.NewInterceptorBuilder(),
				splitAppendHook(func(ctx context.Context, msg message.MutableMessage) error {
					if msg.MessageType() != firstType {
						return nil
					}
					enterOnce.Do(func() { close(entered) })
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}),
			).Build()
			require.NoError(t, err)
			defer opener.Close()
			w, err := opener.Open(ctx, &wal.OpenOption{
				Channel: types.PChannelInfo{Name: "split-txn", Term: 1, AccessMode: types.AccessModeRW}, DisableFlusher: true,
			})
			require.NoError(t, err)
			defer w.Close()
			// Release before closing the WAL, including on a failed assertion.
			defer releaseFirst()
			_, err = w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel(splitTxnSource).
				WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{2}}).
				WithBody(&msgpb.CreateCollectionRequest{CollectionID: 1, CollectionSchema: splitTxnSchema()}).MustBuildMutable())
			require.NoError(t, err)
			txn := beginSplitTxn(ctx, t, w)
			_, err = w.Append(ctx, splitTxnDelete().WithTxnContext(*txn))
			require.NoError(t, err)
			type answer struct {
				result *types.AppendResult
				err    error
			}
			appendAsync := func(msg message.MutableMessage) <-chan answer {
				done := make(chan answer, 1)
				go func() {
					result, err := w.Append(ctx, msg)
					done <- answer{result: result, err: err}
				}()
				return done
			}
			first, second := splitTxnFence(), splitTxnCommit(*txn)
			if commitFirst {
				first, second = second, first
			}
			firstDone := appendAsync(first)
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("first append did not enter the locked section")
			}
			secondDone := appendAsync(second)
			select {
			case <-attempted:
			case <-ctx.Done():
				t.Fatal("competing append did not enter the chain")
			}
			select {
			case <-secondDone:
				t.Fatal("competing append completed while the first held its lifecycle lock")
			case <-time.After(20 * time.Millisecond):
			}
			releaseFirst()
			var a, b answer
			select {
			case a = <-firstDone:
			case <-ctx.Done():
				t.Fatal("first append did not complete")
			}
			select {
			case b = <-secondDone:
			case <-ctx.Done():
				t.Fatal("competing append did not complete")
			}
			require.NoError(t, a.err)
			if commitFirst {
				require.NoError(t, b.err)
				require.Less(t, a.result.TimeTick, b.result.TimeTick, "commit must be below the fence")
			} else {
				requireSplitTxnFenced(t, b.err)
				require.Nil(t, b.result, "the source fence interrupted this transaction before it could append a commit")
			}
		})
	}
}

type splitAppendHook func(context.Context, message.MutableMessage) error

func (h splitAppendHook) Build(*interceptors.InterceptorBuildParam) interceptors.Interceptor {
	return h
}

func (h splitAppendHook) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	if err := h(ctx, msg); err != nil {
		return nil, err
	}
	return appendOp(ctx, msg)
}

func (splitAppendHook) Close() {}
