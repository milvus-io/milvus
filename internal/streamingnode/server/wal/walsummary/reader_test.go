package walsummary

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func observeReadBarrier(m *Manager, tt uint64) {
	msg := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable().
		WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt))).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt + 1)))
	m.ObserveMessage(context.Background(), msg)
}

func TestBoundedReadAcrossStorageStates(t *testing.T) {
	ctx := context.Background()
	m, _ := newTransformTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "v1", 100, &released)
	observeTransformDelete(t, m, "v1", 200, &released)
	sc := m.seal()
	observeTransformDelete(t, m, "v1", 300, &released)
	observeReadBarrier(m, 400)
	for _, limits := range []ReadLimits{{MaxRows: 1}, {MaxBytes: 1}} {
		var cursor uint64
		for _, want := range []uint64{100, 200, 400} {
			b, err := m.ReadTransform(ctx, "v1", cursor, 500, limits)
			require.NoError(t, err)
			require.Len(t, b.Entries, 1)
			require.Equal(t, want, b.CoveredThrough)
			require.Equal(t, uint64(400), b.ReadableThrough)
			cursor = b.CoveredThrough
		}
	}
	b, err := m.ReadTransform(ctx, "v1", 0, 400, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, b.Entries, 3)
	// Results are caller-owned even when sourced from the hot tail.
	b.Entries[2].GetDelete().Blocks[0].PrimaryKeys.GetIntId().Data[0] = -1
	require.NoError(t, m.writeChunk(ctx, sc))
	b, err = m.ReadTransform(ctx, "v1", 0, 400, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, b.Entries, 3)
	require.Equal(t, int64(300), b.Entries[2].GetDelete().Blocks[0].PrimaryKeys.GetIntId().Data[0])
	// No record for this VChannel is still a proven interval, including barriers.
	b, err = m.ReadTransform(ctx, "v2", 0, 900, ReadLimits{})
	require.NoError(t, err)
	require.Empty(t, b.Entries)
	require.Equal(t, uint64(400), b.CoveredThrough)
	select {
	case <-b.Changed:
		t.Fatal("unchanged snapshot was signaled")
	default:
	}
	observeReadBarrier(m, 500)
	select {
	case <-b.Changed:
	default:
		t.Fatal("barrier must notify readers")
	}
}

func TestReadKeepsWholeOversizedTransaction(t *testing.T) {
	ctx := context.Background()
	m, _ := newTransformTestManagerWithStore(t)
	original := message.AsImmutableTxnMessage(newTestIdempotentTxnMessage(t, "v1", 100, "txn", [][]int64{{1}, {2}}))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(original.Begin()))
	builder.Add(newTestDeleteMessage(t, "v1", 101, 10, 1, 2))
	builder.Add(newTestDeleteMessage(t, "v1", 102, 11, 3, 4))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(original.Commit()))
	require.NoError(t, err)
	m.ObserveMessage(ctx, txn)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 200, 10, 5))
	for _, durable := range []bool{false, true} {
		if durable {
			require.NoError(t, persistSummary(ctx, m))
		}
		b, err := m.ReadTransform(ctx, "v1", 0, 200, ReadLimits{MaxRows: 1, MaxBytes: 1})
		require.NoError(t, err)
		require.Len(t, b.Entries, 1)
		require.Len(t, b.Entries[0].GetDelete().GetBlocks(), 2)
		require.Equal(t, txn.TimeTick(), b.CoveredThrough)
	}
}

func TestReadDetectsDurableTruncationAfterRestart(t *testing.T) {
	ctx := context.Background()
	m, store := newTransformTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "v1", 100, &released)
	m.AdvanceGCTimeTick("v1", 100)
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, m))
	require.Empty(t, m.Manifest().GetChunks())
	restored := newTestManager(t, store, 1)
	require.NoError(t, restored.Restore(ctx))
	_, err := restored.ReadTransform(ctx, "v1", 99, 200, ReadLimits{})
	require.ErrorIs(t, err, ErrTransformTruncated)
	b, err := restored.ReadTransform(ctx, "v1", 100, 200, ReadLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), b.CoveredThrough)
	// Per-channel boundaries do not invent deleted history for another channel.
	b, err = restored.ReadTransform(ctx, "v2", 0, 200, ReadLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), b.CoveredThrough)
}

func TestReadSnapshotPinsObjectsAcrossGC(t *testing.T) {
	ctx := context.Background()
	m, _ := newTransformTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "v1", 100, &released)
	observeTransformDelete(t, m, "v1", 200, &released)
	sc := m.seal()
	// While the reader holds a captured durable/hot snapshot, move the hot
	// section to disk and release the older manifest entry. Physical GC needs
	// the exclusive read lock and must wait for this read to return.
	var origin func(*Store, context.Context, uint64, int64, string, *streamingpb.VChannelSummaryChunkIndex) ([]*streamingpb.VChannelSummaryTransformRecord, error)
	patch := mockey.Mock((*Store).ReadTransformSection).Origin(&origin).To(func(store *Store, ctx context.Context, gen uint64, term int64, vc string, index *streamingpb.VChannelSummaryChunkIndex) ([]*streamingpb.VChannelSummaryTransformRecord, error) {
		require.NoError(t, m.writeChunk(ctx, sc))
		m.AdvanceGCTimeTick("v1", 100)
		m.cfg.RetentionMaxBytes = 1
		require.NoError(t, m.GCOnce(ctx))
		require.False(t, m.readMu.TryLock(), "physical GC cannot acquire its lock during a read")
		return origin(store, ctx, gen, term, vc, index)
	}).Build()
	defer patch.UnPatch()
	b, err := m.ReadTransform(ctx, "v1", 0, 200, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, b.Entries, 2, "snapshot must not omit or duplicate a section moving to disk")
	require.Equal(t, uint64(200), b.CoveredThrough)
	require.NoError(t, gcSummary(ctx, m))
	_, err = m.ReadTransform(ctx, "v1", 0, 200, ReadLimits{})
	require.ErrorIs(t, err, ErrTransformTruncated)
}

func TestConcurrentReadObserveAndSeal(t *testing.T) {
	ctx := context.Background()
	m, _ := newTransformTestManagerWithStore(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tt := uint64(1); tt <= 100; tt++ {
			m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", tt, 10, int64(tt)))
			if tt%10 == 0 {
				if sc := m.seal(); sc != nil {
					require.NoError(t, m.writeChunk(ctx, sc))
				}
			}
		}
	}()
	var cursor uint64
	for cursor < 100 {
		b, err := m.ReadTransform(ctx, "v1", cursor, 100, ReadLimits{MaxRows: 3})
		require.NoError(t, err)
		for _, entry := range b.Entries {
			cursor++
			require.Equal(t, cursor, entry.TimeTick)
		}
		require.Equal(t, cursor, b.CoveredThrough)
		if cursor < 100 && b.ReadableThrough == cursor {
			<-b.Changed
		}
	}
	wg.Wait()
}
