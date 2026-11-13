package redo

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type testTxnManager struct{}

func (m *testTxnManager) RecoverDone() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestRedoInterceptorWaitUntilGrowingSegmentReadyReturnsUnrecoverableForDroppedCollectionOrPartition(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	tests := []struct {
		name       string
		vchannels  map[string]moduleapi.VChannelWritePathRecoveryState
		collection int64
		partition  int64
	}{
		{
			name:       "collection not found",
			vchannels:  map[string]moduleapi.VChannelWritePathRecoveryState{},
			collection: 1,
			partition:  2,
		},
		{
			name: "partition not found",
			vchannels: map[string]moduleapi.VChannelWritePathRecoveryState{
				"test-vchannel": {
					VChannel:     "test-vchannel",
					CollectionID: 1,
					PartitionIDs: []int64{2},
				},
			},
			collection: 1,
			partition:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardManager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
				ChannelInfo: types.PChannelInfo{Name: "test-channel", Term: 1},
				WAL:         syncutil.NewFuture[wal.WAL](),
				InitialRecoverSnapshot: &recovery.RecoverySnapshot{
					WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
						VChannels:       tt.vchannels,
						GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{},
					},
					Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
				},
				TxnManager: &testTxnManager{},
			})
			defer shardManager.Close()

			interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
				ShardManager: shardManager,
			})
			defer interceptor.Close()

			msg := message.NewInsertMessageBuilderV1().
				WithVChannel("test-vchannel").
				WithHeader(&messagespb.InsertMessageHeader{
					CollectionId: tt.collection,
					Partitions: []*messagespb.PartitionSegmentAssignment{
						{PartitionId: tt.partition, Rows: 1},
					},
				}).
				WithBody(&msgpb.InsertRequest{}).
				MustBuildMutable()

			msgID, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("append should not be called when collection or partition is already dropped")
				return nil, nil
			})
			require.Nil(t, msgID)
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		})
	}
}

// An inner append that returns WaitRequiredError makes the redo interceptor
// call Wait and then run the inner append again. A Wait error ends the append.
func TestRedoInterceptorWaitRequired(t *testing.T) {
	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable()
	interceptor := &redoAppendInterceptor{gracefulStop: make(chan struct{})}

	appends, waits := 0, 0
	msgID, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appends++
		if appends == 1 {
			return nil, &interceptors.WaitRequiredError{Wait: func(context.Context) error {
				require.Equal(t, 1, appends, "Wait runs before the append is run again")
				waits++
				return nil
			}}
		}
		return walimplstest.NewTestMessageID(1), nil
	})
	require.NoError(t, err)
	require.True(t, msgID.EQ(walimplstest.NewTestMessageID(1)))
	require.Equal(t, 2, appends)
	require.Equal(t, 1, waits)

	waitErr := errors.New("wait failed")
	appends = 0
	msgID, err = interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appends++
		return nil, &interceptors.WaitRequiredError{Wait: func(context.Context) error { return waitErr }}
	})
	require.Nil(t, msgID)
	require.ErrorIs(t, err, waitErr)
	require.Equal(t, 1, appends)
}

// The caller's cancellation ends a wait, because nothing is appended yet.
func TestRedoInterceptorWaitEndsOnCallerCancel(t *testing.T) {
	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable()
	interceptor := &redoAppendInterceptor{gracefulStop: make(chan struct{})}

	caller, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := WithCallerContext(caller)
	ctx = context.WithoutCancel(ctx)

	msgID, err := interceptor.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, &interceptors.WaitRequiredError{Wait: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}
	})
	require.Nil(t, msgID)
	require.ErrorIs(t, err, context.Canceled)
}
