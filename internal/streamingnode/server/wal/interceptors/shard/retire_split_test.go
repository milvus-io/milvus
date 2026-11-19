package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestRetireSplitShardRequiresTheLocalFencedSource(t *testing.T) {
	manager := newSplitTestSourceManager(t, splitTestTaskID)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	ctx := context.Background()
	build := func(taskID int64, local bool) message.MutableMessage {
		b := message.NewRetireSplitShardMessageBuilderV2().WithVChannel(splitTestSource).
			WithHeader(&message.RetireSplitShardMessageHeader{CollectionId: splitTestCollectionID, SplitTaskId: taskID}).
			WithBody(&message.RetireSplitShardMessageBody{})
		if local {
			b.WithUnreplicable()
		}
		return b.MustBuildMutable().WithTimeTick(250)
	}
	_, err := interceptor.DoAppend(ctx, build(splitTestTaskID, false), wal.append)
	require.Error(t, err)
	_, err = interceptor.DoAppend(ctx, build(splitTestTaskID+1, true), wal.append)
	require.Error(t, err)
	require.Empty(t, wal.appended)
	_, err = interceptor.DoAppend(ctx, build(splitTestTaskID, true), wal.append)
	require.NoError(t, err)
	_, err = interceptor.DoAppend(ctx, build(splitTestTaskID, true), wal.append)
	require.NoError(t, err, "retry of a local retirement is harmless")
	require.Len(t, wal.appended, 2)
	_, err = interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 300), wal.append)
	requireShardFenced(t, err)
	_, err = interceptor.DoAppend(ctx, newSplitTestDelete(splitTestSource, 310), wal.append)
	requireShardFenced(t, err)
	require.Len(t, wal.appended, 2)
}
