package walsummary

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func newTransformTestManagerWithStore(t *testing.T) (*Manager, *Store) {
	t.Helper()
	store := newTestStore(t)
	return newTestManager(t, store, 1<<30), store
}

func flushTransform(t *testing.T, manager *Manager, vchannel string, tt uint64, finalized *bool) {
	t.Helper()
	observeTransformDelete(t, manager, vchannel, tt, finalized)
	require.NoError(t, persistSummary(context.Background(), manager))
}

func observeTransformDelete(t *testing.T, manager *Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	msg := newTestDeleteMessage(t, vchannel, timetick, 10, int64(timetick))
	owner := message.NewOwnedImmutableMessage(msg, func() { *finalized = true })
	retained := owner.Clone()
	manager.ObserveMessage(context.Background(), retained.Message())
	retained.Release()
	owner.Release()
}

func TestManagerRestoreTransform(t *testing.T) {
	manager, _ := newTransformTestManagerWithStore(t)
	ctx := context.Background()

	// Two flushes produce two chunks.
	var unused bool
	flushTransform(t, manager, "v1", 100, &unused)
	flushTransform(t, manager, "v1", 200, &unused)

	// A new manager over the same store recovers both chunks and continues
	// generations after them.
	recovered := newTestManager(t, manager.cfg.Store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	assert.Equal(t, uint64(2), recovered.nextGeneration)
	assert.Equal(t, uint64(200), recovered.LatestCoveredTimeTick())
	assert.Len(t, recovered.Manifest().GetChunks(), 2)
}

func TestManagerGCReleaseAndMaterializationFloorTransform(t *testing.T) {
	manager, _ := newTransformTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushTransform(t, manager, "v1", 100, &unused)
	flushTransform(t, manager, "v1", 200, &unused)
	flushTransform(t, manager, "v1", 300, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 3)

	// Without a GC position nothing is eligible, even under budget pressure.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, manager))
	assert.Len(t, manager.Manifest().GetChunks(), 3)

	// Advance the GC position through 200 (a completed materialization):
	// chunks 0 (end 100) and 1 (end 200) are fully consumed and released;
	// chunk 2 (end 300) still holds records past the position and stays.
	manager.AdvanceGCTimeTick("v1", 200)
	require.NoError(t, gcSummary(ctx, manager))
	chunks := manager.Manifest().GetChunks()
	require.Len(t, chunks, 1)
	assert.Equal(t, uint64(2), chunks[0].GetGeneration())
	// The released object is gone.
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err)

	// Advance past everything: all chunks are released.
	manager.AdvanceGCTimeTick("v1", 400)
	require.NoError(t, gcSummary(ctx, manager))
	assert.Empty(t, manager.Manifest().GetChunks())
}

func TestAdvanceGCTimeTickDroppedAllowsGCReleaseTransform(t *testing.T) {
	manager, _ := newTransformTestManagerWithStore(t)
	ctx := context.Background()

	var unused bool
	flushTransform(t, manager, "v1", 100, &unused)
	require.Len(t, manager.Manifest().GetChunks(), 1)

	// Without any GC position the chunk is not releasable.
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, manager))
	require.Len(t, manager.Manifest().GetChunks(), 1)

	// The GC boundary of a dropped vchannel makes its chunks releasable
	// regardless of materialization. The notification touches nothing else.
	manager.AdvanceGCTimeTick("v1", DroppedVChannelTimeTick)
	require.NoError(t, gcSummary(ctx, manager))
	assert.Empty(t, manager.Manifest().GetChunks())
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err, "chunk object must be deleted after release")
}

func TestDurableTimeTickDerivedFromManifestTransform(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTransformTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))
	assert.Zero(t, manager.DurableTimeTick("v1"))

	var unused bool
	flushTransform(t, manager, "v1", 100, &unused)
	flushTransform(t, manager, "v1", 200, &unused)
	assert.Equal(t, uint64(200), manager.DurableTimeTick("v1"))

	// A vchannel with no records has no frontier.
	assert.Zero(t, manager.DurableTimeTick("v2"))
}

func TestManagerReadTransformEntriesAcrossChunksTransform(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTransformTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))

	// Two flushes produce two chunks; recovery-style reads span them.
	var unused bool
	flushTransform(t, manager, "v1", 100, &unused)
	flushTransform(t, manager, "v1", 200, &unused)
	entries, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())
	assert.Equal(t, uint64(200), entries[1].GetTimeTick())

	// The from-boundary is exclusive.
	entries, err = manager.ReadTransformEntries(ctx, "v1", 100, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(200), entries[0].GetTimeTick())
}

func TestMixedSummaryConsumersPersistRecoverAndGC(t *testing.T) {
	ctx := context.Background()
	manager, store := newTransformTestManagerWithStore(t)
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "mixed", 100, 10, 1))
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "mixed", 200, "key", []int64{2}, []uint32{0}))
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "insert-only", 300, "other", []int64{3}, []uint32{0}))
	require.Empty(t, manager.Manifest().GetChunks(), "observation must not persist")
	require.NoError(t, persistSummary(ctx, manager))
	require.NoError(t, persistSummary(ctx, manager), "empty retry must not add another chunk")
	require.Len(t, manager.Manifest().GetChunks(), 1)
	recovered := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 3), 1)
	require.NoError(t, recovered.Restore(ctx))
	entries, err := recovered.ReadTransformEntries(ctx, "mixed", 0, 100)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []int64{1}, entries[0].GetDelete().GetBlocks()[0].GetPrimaryKeys().GetIntId().GetData())
	empty, err := recovered.ReadTransformEntries(ctx, "insert-only", 0, 1000)
	require.NoError(t, err)
	require.Empty(t, empty)
	keys, err := recovered.ReadIdempotencyEntries(ctx, "mixed", 0, 1000)
	require.NoError(t, err)
	require.Len(t, keys.Idempotency, 1)
	require.Equal(t, "key", keys.Idempotency[0].GetKey())
	require.Equal(t, []int64{2}, keys.Inserts[0].GetIds().GetIntId().GetData())
	require.NoError(t, recovered.GCOnce(ctx))
	require.Len(t, recovered.Manifest().GetChunks(), 1, "unknown transform frontier pins its chunk")
	recovered.RestoreTransformGCTimeTicks(map[string]*streamingpb.VChannelMeta{
		"mixed": {TransformMaterializedTimeTick: 100},
	})
	require.NoError(t, recovered.GCOnce(ctx))
	require.Empty(t, recovered.Manifest().GetChunks(), "later inserts and insert-only vchannels do not pin consumed transforms")
}

func TestMixedSummaryDDLPreservesRequestHistory(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTransformTestManagerWithStore(t)
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 110, "old-key", []int64{2}, []uint32{0}))
	manager.ObserveMessage(ctx, newTestSummaryDDL(t, "truncate", 120))
	require.Len(t, manager.pending, 2, "DDL preserves both transform and idempotency records")
	require.NoError(t, persistSummary(ctx, manager))
	transforms, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, transforms, 1)
	keys, err := manager.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, keys.Inserts, 1)
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, manager))
	require.Len(t, manager.Manifest().GetChunks(), 1, "DDL observation is not durable transform materialization")
	manager.RestoreTransformGCTimeTicks(map[string]*streamingpb.VChannelMeta{
		"v1": {State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, TransformMaterializedTimeTick: 100},
	})
	require.NoError(t, gcSummary(ctx, manager))
	require.Empty(t, manager.Manifest().GetChunks())
}

func TestTransformSectionsRoundTripAndRetry(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	record := func(tt uint64, pk int64) *streamingpb.VChannelSummaryTransformRecord {
		return &streamingpb.VChannelSummaryTransformRecord{TimeTick: tt, Delete: &streamingpb.TransformDeleteEntry{
			Blocks: []*streamingpb.TransformDeleteBlock{{PartitionId: 10, PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}},
			}}},
		}}
	}
	sections := writeSections(map[string][]uint64{"v1": {200}})
	sections["v1"].Transform = []*streamingpb.VChannelSummaryTransformRecord{record(110, 2), record(100, 1)}
	sections["delete-only"] = &ChunkSections{Transform: []*streamingpb.VChannelSummaryTransformRecord{record(90, 3)}}
	footer, _, err := store.WriteChunk(ctx, 0, sections, testRecordCoverage(sections))
	require.NoError(t, err)
	require.Equal(t, uint64(90), footer.GetStartTimeTick())
	require.Equal(t, uint64(200), footer.GetEndTimetick())
	decoded, _, err := store.ReadChunk(ctx, 0, store.Term())
	require.NoError(t, err)
	require.True(t, chunkSectionsByVChannelEqual(sections, decoded))
	require.Equal(t, uint64(100), decoded["v1"].Transform[0].GetTimeTick())
	require.Empty(t, decoded["delete-only"].Inserts)
	_, _, err = store.WriteChunk(ctx, 0, decoded, testRecordCoverage(decoded))
	require.NoError(t, err, "sorted rewrite has the same content")
	decoded["v1"].Transform[0] = record(100, 999)
	_, _, err = store.WriteChunk(ctx, 0, decoded, testRecordCoverage(decoded))
	require.ErrorIs(t, err, ErrStoreCorrupted, "a transform-only difference is not an idempotent retry")
}

func TestTransformSectionRejectsCorruptRefs(t *testing.T) {
	sections := &streamingpb.VChannelSummaryTransformSection{Records: []*streamingpb.VChannelSummaryTransformRecord{{TimeTick: 10}}}
	buf := bytes.NewBuffer(newChunkHeader())
	ref, err := appendSection(buf, sections, 1)
	require.NoError(t, err)
	for name, alter := range map[string]func(*streamingpb.VChannelSummarySectionRef){
		"offset":   func(r *streamingpb.VChannelSummarySectionRef) { r.Offset = 0 },
		"length":   func(r *streamingpb.VChannelSummarySectionRef) { r.Length++ },
		"overflow": func(r *streamingpb.VChannelSummarySectionRef) { r.Offset = math.MaxUint64 },
		"count":    func(r *streamingpb.VChannelSummarySectionRef) { r.RecordCount++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(ref).(*streamingpb.VChannelSummarySectionRef)
			alter(bad)
			_, err := unmarshalTransformSection(buf.Bytes(), uint64(buf.Len()), &streamingpb.VChannelSummaryChunkIndex{Vchannel: "v1", Transform: &streamingpb.VChannelSummaryTransformIndex{Ref: bad}})
			require.ErrorIs(t, err, ErrStoreCorrupted)
		})
	}
}

func TestTransformReadBoundsAndMissingObject(t *testing.T) {
	ctx := context.Background()
	manager, store := newTransformTestManagerWithStore(t)
	var finalized bool
	flushTransform(t, manager, "v1", 100, &finalized)
	flushTransform(t, manager, "v1", 200, &finalized)
	records, err := manager.ReadTransformEntries(ctx, "v1", 0, 99)
	require.NoError(t, err)
	require.Empty(t, records)
	manager.AdvanceGCTimeTick("v1", 100)
	require.True(t, manager.chunkReleasedLocked(manager.manifest.Chunks[0]))
	require.NoError(t, store.DeleteChunk(ctx, 0, store.Term()))
	_, err = manager.ReadTransformEntries(ctx, "v1", 0, 100)
	require.Error(t, err, "a missing durable transform object must fail recovery")
}

func TestTransformSectionRejectsMalformedPayload(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	index := &streamingpb.VChannelSummaryChunkIndex{Vchannel: "v1"}
	_, err := unmarshalTransformSection(newChunkHeader(), chunkHeaderSize, index)
	require.ErrorIs(t, err, ErrStoreCorrupted)
	payload := append(newChunkHeader(), byte(0xff))
	index.Transform = &streamingpb.VChannelSummaryTransformIndex{Ref: &streamingpb.VChannelSummarySectionRef{Offset: chunkHeaderSize, Length: 1, RecordCount: 1}}
	_, err = unmarshalTransformSection(payload, uint64(len(payload)), index)
	require.ErrorIs(t, err, ErrStoreCorrupted)
	require.NoError(t, store.chunkManager.Write(ctx, store.ChunkKey(0), payload))
	_, err = store.ReadTransformSection(ctx, 0, store.Term(), "v1", index)
	require.ErrorIs(t, err, ErrStoreCorrupted, "invalid object framing must be rejected before reading a section")
}

func TestMixedTransactionHistorySurvivesDDL(t *testing.T) {
	ctx := context.Background()
	original := message.AsImmutableTxnMessage(newTestIdempotentTxnMessage(t, "v1", 100, "txn-key", [][]int64{{1}, {2}}))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(original.Begin()))
	builder.Add(newTestIdempotentInsertMessage(t, "v1", 101, "", []int64{1}, []uint32{0}))
	builder.Add(newTestDeleteMessage(t, "v1", 102, 10, 99))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(original.Commit()))
	require.NoError(t, err)
	manager, _ := newTransformTestManagerWithStore(t)
	manager.ObserveMessage(ctx, txn)
	require.Len(t, manager.pending, 1)
	require.NotNil(t, manager.pending[0].entry)
	require.NotNil(t, manager.pending[0].insert)
	manager.ObserveMessage(ctx, newTestSummaryDDL(t, "drop-partition", 200))
	require.Len(t, manager.pending, 1, "DDL preserves the complete transaction summary")
	require.NoError(t, persistSummary(ctx, manager))
	transforms, err := manager.ReadTransformEntries(ctx, "v1", 0, 200)
	require.NoError(t, err)
	require.Len(t, transforms, 1)
	require.Equal(t, txn.TimeTick(), transforms[0].GetTimeTick())
	require.Equal(t, []int64{99}, transforms[0].GetDelete().GetBlocks()[0].GetPrimaryKeys().GetIntId().GetData())
	keys, err := manager.ReadIdempotencyEntries(ctx, "v1", 0, 200)
	require.NoError(t, err)
	require.Len(t, keys.Idempotency, 1)
	require.Equal(t, "txn-key", keys.Idempotency[0].GetKey())
}

func TestSummaryDoesNotPersistBarrierEntries(t *testing.T) {
	for _, withDelete := range []bool{false, true} {
		name := "barriers only"
		if withDelete {
			name = "delete followed by barriers"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			manager, store := newTransformTestManagerWithStore(t)
			require.NoError(t, manager.Restore(ctx))
			expectedRecords := 0
			if withDelete {
				manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
				expectedRecords = 1
			}
			barriers := []message.MutableMessage{
				message.NewCreatePartitionMessageBuilderV1().WithVChannel("v1").
					WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 1, PartitionId: 20}).
					WithBody(&msgpb.CreatePartitionRequest{}).MustBuildMutable(),
				message.NewManualFlushMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable(),
				message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
					WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable(),
				message.NewTimeTickMessageBuilderV1().WithVChannel("").
					WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable(),
			}
			for i, mutable := range barriers {
				tt := uint64(200 + i)
				manager.ObserveMessage(ctx, mutable.WithTimeTick(tt).
					WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt-1))).
					IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt))))
				require.Len(t, manager.pending, expectedRecords)
			}
			require.NoError(t, persistSummary(ctx, manager))
			require.Len(t, manager.Manifest().GetChunks(), expectedRecords, "barriers must not create chunks")
			require.Equal(t, uint64(203), manager.LastAcked())
			recovered := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 2), 1<<30)
			require.NoError(t, recovered.Restore(ctx))
			entries, err := recovered.ReadTransformEntries(ctx, "v1", 0, math.MaxUint64)
			require.NoError(t, err)
			require.Len(t, entries, expectedRecords)
			if withDelete {
				require.Equal(t, uint64(100), entries[0].GetTimeTick())
				require.NotNil(t, entries[0].GetDelete())
			}
		})
	}
}

func TestDroppedVChannelRetirementSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	manager, store := newTransformTestManagerWithStore(t)
	var released bool
	flushTransform(t, manager, "dropped", 100, &released)
	meta := map[string]*streamingpb.VChannelMeta{
		"dropped": {
			State:              streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick: 100, TransformMaterializedTimeTick: 100,
		},
	}
	manager.RestoreTransformGCTimeTicks(meta)
	require.NoError(t, gcSummary(ctx, manager))
	require.False(t, manager.CanCleanupVChannel("dropped", 100), "retained chunk still needs the tombstone")
	recovered := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 3), 1)
	require.NoError(t, recovered.Restore(ctx))
	recovered.RestoreTransformGCTimeTicks(meta)
	require.NoError(t, recovered.GCOnce(ctx))
	require.Empty(t, recovered.Manifest().GetChunks())
	require.False(t, recovered.CanCleanupVChannel("dropped", 100), "in-memory retirement is not enough")
	require.NoError(t, (&manifestWriteTask{manager: recovered}).Execute(ctx))
	require.True(t, recovered.CanCleanupVChannel("dropped", 100))
	// Crash after manifest publication and catalog deletion but before object GC.
	again := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 4), 1)
	require.NoError(t, again.Restore(ctx))
	again.RestoreTransformGCTimeTicks(map[string]*streamingpb.VChannelMeta{})
	require.NoError(t, (&manifestWriteTask{manager: again}).Execute(ctx))
	require.Empty(t, again.Manifest().GetChunks())
	require.True(t, again.CanCleanupVChannel("dropped", 100))
}
