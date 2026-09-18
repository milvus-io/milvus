package testcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// ManualFlush must wait for its own L1/L0 persistence without waiting for
// unrelated collections to release the shared physical WAL checkpoint.
func TestManualFlushSharedWAL(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, target := hp.CollPrepare.CreateCollection(ctx, t, mc, cp,
		hp.TNewFieldOptions(), hp.TNewSchemaOption(), hp.TWithShardNum(2))
	targetInfo, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(target.CollectionName))
	require.NoError(t, err)
	// Placement is round-robin; find another collection sharing a physical WAL.
	shared := false
	for attempt := 0; attempt < 16 && !shared; attempt++ {
		_, blocker := prepare.CreateCollection(ctx, t, mc, cp,
			hp.TNewFieldOptions(), hp.TNewSchemaOption(), hp.TWithShardNum(2))
		info, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(blocker.CollectionName))
		require.NoError(t, err)
		for _, channel := range info.PhysicalChannels {
			for _, targetChannel := range targetInfo.PhysicalChannels {
				shared = shared || channel == targetChannel
			}
		}
		if shared {
			// Keep a small Insert pending in both blocker shards; do not flush it.
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(blocker), hp.TNewDataOption().TWithNb(1000))
		}
	}
	require.True(t, shared, "regression requires collections sharing a physical WAL")
	flush := func(expectSegments bool) {
		t.Helper()
		flushCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		task, err := mc.Flush(flushCtx, client.NewFlushOption(target.CollectionName))
		require.NoError(t, err)
		pending, flushed, flushTs, _ := task.GetFlushStats()
		require.Empty(t, pending)
		require.Zero(t, flushTs)
		if expectSegments {
			require.NotEmpty(t, flushed)
		}
		require.NoError(t, task.Await(flushCtx))
	}
	flush(false) // An empty collection also completes without a checkpoint.
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(target), hp.TNewDataOption().TWithNb(100))
	flush(true)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(target))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(target.CollectionName))

	_, err = mc.Delete(ctx, client.NewDeleteOption(target.CollectionName).
		WithInt64IDs(common.DefaultInt64FieldName, []int64{0, 1, 2}))
	require.NoError(t, err)
	flush(true) // Delete-only Flush must wait for the L0 as well.
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(target.CollectionName)))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(target.CollectionName))
	result, err := mc.Query(ctx, client.NewQueryOption(target.CollectionName).
		WithFilter("int64 >= 0").WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 97, result.ResultCount)
}
