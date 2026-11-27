package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function"
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

func TestRetireSplitShardReleasesOnlyDurableSourceRunner(t *testing.T) {
	manager := newSplitTestSourceManager(t, splitTestTaskID)
	interceptor := newSplitTestInterceptor(manager)
	allocWALSchemaForTest(t, splitTestCollectionID, splitTestSource, 1)
	allocWALSchemaForTest(t, splitTestCollectionID, splitTestTarget1, 1)
	materialize := func(channel string) error {
		_, err := function.GetManager().Materialize(t.Context(), splitTestCollectionID, WALFunctionRunnerKey(channel), 1, message.MustAsMutableInsertMessageV1(newSplitTestInsert(channel, 300)))
		return err
	}
	build := func() message.MutableMessage {
		return message.NewRetireSplitShardMessageBuilderV2().WithVChannel(splitTestSource).
			WithHeader(&message.RetireSplitShardMessageHeader{CollectionId: splitTestCollectionID, SplitTaskId: splitTestTaskID}).
			WithBody(&message.RetireSplitShardMessageBody{}).WithUnreplicable().MustBuildMutable().WithTimeTick(250)
	}
	wal := &appendRecorder{err: errors.New("append failed")}
	_, err := interceptor.DoAppend(t.Context(), build(), wal.append)
	require.ErrorIs(t, err, wal.err)
	require.NoError(t, materialize(splitTestSource))
	wal.err = nil
	_, err = interceptor.DoAppend(t.Context(), build(), wal.append)
	require.NoError(t, err)
	require.Error(t, materialize(splitTestSource))
	require.NoError(t, materialize(splitTestTarget1))
}

func TestDropCollectionRemovesSplitTargetGateAfterAppend(t *testing.T) {
	manager := newSplitTestTargetManager(t)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	_, err := interceptor.DoAppend(t.Context(), newSplitShardCopy(splitTestTarget1, splitTestTaskID, 250), wal.append)
	require.NoError(t, err)
	require.Contains(t, interceptor.splitGates, splitTestTarget1)
	t.Cleanup(func() { function.GetManager().Release(splitTestCollectionID, WALFunctionRunnerKey(splitTestTarget1)) })
	build := func() message.MutableMessage {
		return message.NewDropCollectionMessageBuilderV1().WithVChannel(splitTestTarget1).
			WithHeader(&message.DropCollectionMessageHeader{CollectionId: splitTestCollectionID}).
			WithBody(&message.DropCollectionRequest{}).MustBuildMutable().WithTimeTick(300).WithLastConfirmedUseMessageID()
	}
	wal.err = errors.New("append failed")
	_, err = interceptor.DoAppend(t.Context(), build(), wal.append)
	require.ErrorIs(t, err, wal.err)
	require.Contains(t, interceptor.splitGates, splitTestTarget1)
	wal.err = nil
	_, err = interceptor.DoAppend(t.Context(), build(), wal.append)
	require.NoError(t, err)
	require.Empty(t, interceptor.splitGates)
}
