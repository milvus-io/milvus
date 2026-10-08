package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGrowingPublicationRetriesBeforeCompletingInsert(t *testing.T) {
	ctx := context.Background()
	view := newObserveTestSegment(5)
	view.lifecycle = &segmentLifecycleWriter{coord: &coordStub{}, serverID: 1}
	view.packWriter = &growingBulkPackWriter{}
	writes := 0
	write := mockey.Mock((*growingBulkPackWriter).FlushInsertBuffer).To(func(_ *growingBulkPackWriter, _ context.Context, pack *flushPack) (*flushResult, error) {
		writes++
		return &flushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: pack.FromTimeTick, ToTimeTick: pack.ToTimeTick}},
		}}, nil
	}).Build()
	t.Cleanup(func() { write.UnPatch() })
	var requests []*datapb.SaveBinlogPathsRequest
	fail := true
	publication := mockey.Mock((*coordStub).SaveBinlogPaths).To(func(_ *coordStub, _ context.Context, req *datapb.SaveBinlogPathsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
		requests = append(requests, proto.Clone(req).(*datapb.SaveBinlogPathsRequest))
		if fail {
			return nil, merr.WrapErrServiceUnavailableMsg("coordinator unavailable")
		}
		if req.GetFlushed() {
			return dataview.FlushResultStatus(&viewpb.DataVersion{StreamingVersion: 2}), nil
		}
		return merr.Success(), nil
	}).Build()
	t.Cleanup(func() { publication.UnPatch() })

	observe := func(tt uint64, done *bool) {
		raw := newObserveTestInsert(t, tt, []*messagespb.PartitionSegmentAssignment{newObserveTestAssignment(1, 3, 4)})
		batches, err := BuildInsertBatches(raw)
		require.NoError(t, err)
		owned := message.NewOwnedImmutableMessage(raw, func() { *done = true })
		retained := owned.Clone()
		view.ObserveInsert(ctx, retained, batches[1])
		retained.Release()
		owned.Release()
	}
	first, later := false, false
	observe(10, &first)
	view.mu.Lock()
	through := view.enqueuePendingFlushChunkLocked()
	view.mu.Unlock()
	observe(20, &later)
	require.Error(t, view.FlushInsertChunk(ctx, through))
	require.False(t, first)
	require.Nil(t, view.ConsumeDirtyAndGetSnapshot(), "failed publication must not expose a durable checkpoint")
	fail = false
	require.NoError(t, view.FlushInsertChunk(ctx, through))
	require.Equal(t, 1, writes, "RPC retry must reuse the written pack")
	require.True(t, first)
	require.False(t, later)
	require.Len(t, requests, 2)
	require.True(t, proto.Equal(requests[0], requests[1]))
	require.False(t, requests[1].GetFlushed())
	require.True(t, requests[1].GetWithFullBinlogs())
	require.Equal(t, int64(3), requests[1].GetCheckPoints()[0].GetNumOfRows())
	require.Equal(t, uint64(10), requests[1].GetCheckPoints()[0].GetPosition().GetTimestamp())
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	require.Equal(t, uint64(3), snapshot.GetStat().GetModifiedRows())
	require.Len(t, snapshot.GetPersistedStorage().GetBinlogs(), 1)
	require.Len(t, requests[1].GetStartPositions(), 1)
	start := requests[1].GetStartPositions()[0].GetStartPosition()
	require.Equal(t, uint64(10), start.GetTimestamp(), "first Insert, not CreateSegment")
	require.Equal(t, adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(10)).Serialize(), start.GetMsgID(), "LastConfirmed, not the raw message ID 11")
	require.Equal(t, commonpb.WALName_RocksMQ, start.GetWALName())
	require.True(t, proto.Equal(start, requests[1].GetCheckPoints()[0].GetPosition()))

	// Simulate losing the successful first publication's SN snapshot. Replay
	// must rebuild the same first position already accepted by Coordinator.
	replayed := newObserveTestSegment(5)
	replayed.lifecycle, replayed.packWriter = view.lifecycle, view.packWriter
	original := view
	view = replayed
	completed := false
	observe(10, &completed)
	view.RequestPersistThrough(10)
	require.NoError(t, view.FlushInsertChunk(ctx, 10))
	require.True(t, completed)
	require.True(t, proto.Equal(start, requests[2].GetStartPositions()[0].GetStartPosition()))
	view = original

	// Once the SN snapshot is durable, recovery skips the first pack and
	// subsequent packs leave Coordinator's original StartPosition untouched.
	recovered := newSegmentViewFromMeta(snapshot, nil, runtimeConfig{lifecycle: view.lifecycle, packWriter: view.packWriter, owner: testSegmentOwner{}})
	view = recovered
	completed = false
	observe(20, &completed)
	view.mu.Lock()
	through = view.enqueuePendingFlushChunkLocked()
	view.mu.Unlock()
	require.NoError(t, view.FlushInsertChunk(ctx, through))
	require.True(t, completed)
	require.Empty(t, requests[3].GetStartPositions())
	require.Equal(t, uint64(20), requests[3].GetCheckPoints()[0].GetPosition().GetTimestamp())
	require.Equal(t, adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(20)).Serialize(), requests[3].GetCheckPoints()[0].GetPosition().GetMsgID())
	_, err := view.lifecycle.CommitL1Segment(ctx, view.AssignmentMeta())
	require.NoError(t, err)
	require.True(t, requests[4].GetFlushed())
	require.Empty(t, requests[4].GetStartPositions())
	require.Empty(t, requests[4].GetCheckPoints(), "final commit preserves the already registered DmlPosition")
	view = original
	view.mu.Lock()
	pending := view.pending.takeAll()
	view.mu.Unlock()
	releaseMessages(pending.retainedHandles())
}
