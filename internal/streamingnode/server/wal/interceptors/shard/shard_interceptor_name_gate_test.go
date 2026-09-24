package shard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// gatedDDLMessages builds one message of every collection-keyed DDL/control
// type the name gate covers, addressed to vchannel.
func gatedDDLMessages(vchannel string, collectionID int64, partitionID int64) map[string]message.MutableMessage {
	return map[string]message.MutableMessage{
		"ManualFlush": message.NewManualFlushMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&messagespb.ManualFlushMessageHeader{CollectionId: collectionID, FlushTs: 300}).
			WithBody(&messagespb.ManualFlushMessageBody{}).
			MustBuildMutable().WithTimeTick(300),
		"CreatePartition": message.NewCreatePartitionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&messagespb.CreatePartitionMessageHeader{CollectionId: collectionID, PartitionId: partitionID + 1}).
			WithBody(&msgpb.CreatePartitionRequest{}).
			MustBuildMutable().WithTimeTick(300),
		"DropPartition": message.NewDropPartitionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&messagespb.DropPartitionMessageHeader{CollectionId: collectionID, PartitionId: partitionID}).
			WithBody(&msgpb.DropPartitionRequest{}).
			MustBuildMutable().WithTimeTick(300),
		"SchemaChange": message.NewSchemaChangeMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&messagespb.SchemaChangeMessageHeader{CollectionId: collectionID}).
			WithBody(&messagespb.SchemaChangeMessageBody{}).
			MustBuildMutable().WithTimeTick(300),
		"AlterCollection": message.NewAlterCollectionMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: collectionID,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
			}).
			WithBody(&message.AlterCollectionMessageBody{
				Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Name: "col", Version: 4}},
			}).
			MustBuildMutable().WithTimeTick(300),
		"TruncateCollection": message.NewTruncateCollectionMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&messagespb.TruncateCollectionMessageHeader{CollectionId: collectionID}).
			WithBody(&messagespb.TruncateCollectionMessageBody{}).
			MustBuildMutable().WithTimeTick(300),
		"DropCollection": message.NewDropCollectionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&messagespb.DropCollectionMessageHeader{CollectionId: collectionID}).
			WithBody(&msgpb.DropCollectionRequest{}).
			MustBuildMutable().WithTimeTick(300),
	}
}

func newTestImportMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewImportMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{CollectionID: collectionID, PartitionIDs: []int64{2}}).
		MustBuildMutable().WithTimeTick(300)
}

func newTestInsertMutableMessage(vchannel string, collectionID int64, partitionID int64) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions:   []*messagespb.PartitionSegmentAssignment{{PartitionId: partitionID, Rows: 1, BinarySize: 100}},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(300)
}

func newTestDeleteMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(300)
}

// appendAndCapture appends msg through the interceptor with an extra append
// result in the context, as the WAL adaptor provides, and returns what the
// backend received and the extra response.
func appendAndCapture(t *testing.T, i interceptors.Interceptor, msg message.MutableMessage) (message.MessageID, message.MutableMessage, *utility.ExtraAppendResult, error) {
	t.Helper()
	extra := &utility.ExtraAppendResult{}
	ctx := utility.WithExtraAppendResult(context.Background(), extra)
	var appended message.MutableMessage
	msgID, err := i.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appended = msg
		return rmq.NewRmqID(1), nil
	})
	return msgID, appended, extra, err
}

// assertEmptyManualFlushExtra checks the proxy can decode a ManualFlush extra
// response carrying no segment: without the extra it fails the whole Flush.
func assertEmptyManualFlushExtra(t *testing.T, extra *utility.ExtraAppendResult, appended message.MutableMessage) {
	t.Helper()
	resp, ok := extra.Extra.(*message.ManualFlushExtraResponse)
	require.True(t, ok, "a ManualFlush append must always carry its extra response")
	assert.Empty(t, resp.GetSegmentIds())
	assert.Empty(t, message.MustAsMutableManualFlushMessageV2(appended).Header().GetSegmentIds())
}

// TestNameGateFencedSourceDDLIsAppendedWithoutEffect: every collection-keyed
// DDL/control replica addressed to a fenced source is appended and never
// reaches a handler. The mock shard manager fails the test on any call other
// than the gate's own check.
func TestNameGateFencedSourceDDLIsAppendedWithoutEffect(t *testing.T) {
	for name, msg := range gatedDDLMessages("v0", 1, 2) {
		t.Run(name, func(t *testing.T) {
			i, shardManager := newTestShardInterceptor(t)
			shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()

			msgID, appended, extra, err := appendAndCapture(t, i, msg)
			require.NoError(t, err)
			require.NotNil(t, appended)
			assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
			if msg.MessageType() == message.MessageTypeManualFlush {
				assertEmptyManualFlushExtra(t, extra, appended)
			}
		})
	}
}

// TestNameGateAppendFailureIsReturned: a failed append of a replica the gate
// lets through without effect is reported as is.
func TestNameGateAppendFailureIsReturned(t *testing.T) {
	for name, msg := range gatedDDLMessages("v0", 1, 2) {
		t.Run(name, func(t *testing.T) {
			i, shardManager := newTestShardInterceptor(t)
			shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()

			ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
			msgID, err := i.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
				return nil, assert.AnError
			})
			assert.ErrorIs(t, err, assert.AnError)
			assert.Nil(t, msgID)
		})
	}
}

// TestNameGateUnknownVChannelKeepsPreviousBehaviour: a vchannel this pchannel
// neither holds nor has fenced. ManualFlush stays refused, as it was before the
// gate (CollectionNotFound, unrecoverable); every broadcast DDL replica stays
// appended, now without calling the collection-keyed shard-manager apply.
func TestNameGateUnknownVChannelKeepsPreviousBehaviour(t *testing.T) {
	for name, msg := range gatedDDLMessages("v-unknown", 1, 2) {
		t.Run(name, func(t *testing.T) {
			i, shardManager := newTestShardInterceptor(t)
			shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v-unknown").Return(shards.ErrCollectionNotFound).Once()

			msgID, appended, _, err := appendAndCapture(t, i, msg)
			if msg.MessageType() == message.MessageTypeManualFlush {
				assert.Nil(t, msgID)
				assert.Nil(t, appended)
				streamErr := status.AsStreamingError(err)
				assert.True(t, streamErr.IsUnrecoverable())
				assert.False(t, streamErr.IsShardFenced())
				return
			}
			require.NoError(t, err)
			require.NotNil(t, appended)
			assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
		})
	}
}

// TestNameGateImportOnAFencedSourceIsAppended: Import takes no shard-manager
// action on any vchannel; an Import replica landing on a fenced source must
// still be appended, since refusing a broadcast replica wedges the broadcaster.
func TestNameGateImportOnAFencedSourceIsAppended(t *testing.T) {
	i, _ := newTestShardInterceptor(t)
	msgID, appended, _, err := appendAndCapture(t, i, newTestImportMutableMessage("v0", 1))
	require.NoError(t, err)
	require.NotNil(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
}

type recoveredTxnManager struct{}

func (recoveredTxnManager) RecoverDone() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// newTestShardManagerWithSplitTarget recovers a real shard manager holding the
// state the name gate exists for: collection 1's source "v0" fenced by a split
// (SPLITTED, so only its tombstone is kept), and the split's target "v1" of the
// SAME collection registered on the same pchannel, with growing segment 1000 in
// partition 2.
func newTestShardManagerWithSplitTarget(t *testing.T) shards.ShardManager {
	return newTestShardManagerWithSplitTargetSegments(t, map[int64]*streamingpb.SegmentAssignmentMeta{
		1000: {
			CollectionId: 1,
			PartitionId:  2,
			SegmentId:    1000,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			Stat: &streamingpb.SegmentAssignmentStat{
				MaxBinarySize:         200,
				ModifiedBinarySize:    100,
				CreateSegmentTimeTick: 50,
			},
		},
	})
}

// newTestShardManagerWithSplitTargetSegments is newTestShardManagerWithSplitTarget
// with the target's segment assignments given by the caller.
func newTestShardManagerWithSplitTargetSegments(t *testing.T, segments map[int64]*streamingpb.SegmentAssignmentMeta) shards.ShardManager {
	m, _ := newTestShardManagerWithSplitTargetAllocs(t, segments)
	return m
}

// newTestShardManagerWithSplitTargetAllocs is newTestShardManagerWithSplitTargetSegments
// that also returns the CreateSegment messages the manager's segment-alloc
// workers append to the WAL. A test that makes the manager allocate must wait
// for that append with waitForSegmentAllocWorker before it returns: the worker
// runs on its own goroutine that nothing joins, and a worker still reading the
// node-wide resource while the next test's resource.InitForTest replaces it
// dereferences a resource whose ID allocator is not set yet.
func newTestShardManagerWithSplitTargetAllocs(t *testing.T, segments map[int64]*streamingpb.SegmentAssignmentMeta) (shards.ShardManager, <-chan message.MutableMessage) {
	paramtable.Init()
	resource.InitForTest(t)
	allocs := make(chan message.MutableMessage, 16)
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Unavailable().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
		if msg.MessageType() == message.MessageTypeCreateSegment {
			select {
			case allocs <- msg:
			default:
			}
		}
		return &types.AppendResult{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1000,
		}, nil
	}).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	vchannelMeta := func(vchannel string, state streamingpb.VChannelState, splitTimeTick uint64) *streamingpb.VChannelMeta {
		return &streamingpb.VChannelMeta{
			Vchannel:      vchannel,
			State:         state,
			SplitTimeTick: splitTimeTick,
			SplitTaskId:   100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 2}},
			},
		}
	}
	m := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test_name_gate_channel", Term: 1},
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v0": vchannelMeta("v0", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 150),
				"v1": vchannelMeta("v1", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0),
			},
			SegmentAssignments: segments,
			Checkpoint:         &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: recoveredTxnManager{},
	})
	t.Cleanup(m.Close)
	return m, allocs
}

// waitForSegmentAllocWorker waits until the segment-alloc worker the test
// started has appended its CreateSegment for vchannel. After that append the
// worker returns without touching the node-wide resource again.
func waitForSegmentAllocWorker(t *testing.T, allocs <-chan message.MutableMessage, vchannel string) {
	t.Helper()
	select {
	case msg := <-allocs:
		require.Equal(t, vchannel, msg.VChannel())
	case <-time.After(10 * time.Second):
		t.Fatalf("the segment-alloc worker of %s never appended its CreateSegment", vchannel)
	}
}

func newTestShardInterceptorWithManager(t *testing.T, m shards.ShardManager) interceptors.Interceptor {
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ShardManager: m})
	t.Cleanup(i.Close)
	return i
}

// assertTargetUntouched checks the split target still holds the slot with its
// partition and its growing segment exactly as recovered.
func assertTargetUntouched(t *testing.T, m shards.ShardManager) {
	t.Helper()
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))
	assert.NoError(t, m.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: 1, PartitionID: 2}))
	assert.ErrorIs(t, m.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: 1, PartitionID: 3}), shards.ErrPartitionNotFound)
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(shards.PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1000), shards.ErrSegmentOnGrowing,
		"the target's growing segment must not be sealed on behalf of the fenced source")
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v0"), shards.ErrVChannelFenced)
}

// TestNameGateFencedSourceDoesNotTouchTheSplitTarget runs every gated DDL type,
// addressed to the fenced source, against a real shard manager whose slot for
// the same collection is held by the split target. Before the gate, ManualFlush
// sealed the target's segments, CreatePartition/DropPartition changed the
// target's partitions and DropCollection tore the target down.
func TestNameGateFencedSourceDoesNotTouchTheSplitTarget(t *testing.T) {
	for name, msg := range gatedDDLMessages("v0", 1, 2) {
		t.Run(name, func(t *testing.T) {
			m := newTestShardManagerWithSplitTarget(t)
			i := newTestShardInterceptorWithManager(t, m)
			assertTargetUntouched(t, m)

			msgID, appended, extra, err := appendAndCapture(t, i, msg)
			require.NoError(t, err)
			require.NotNil(t, appended)
			assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
			if msg.MessageType() == message.MessageTypeManualFlush {
				assertEmptyManualFlushExtra(t, extra, appended)
			}
			assertTargetUntouched(t, m)
		})
	}

	t.Run("Import", func(t *testing.T) {
		m := newTestShardManagerWithSplitTarget(t)
		i := newTestShardInterceptorWithManager(t, m)
		_, appended, _, err := appendAndCapture(t, i, newTestImportMutableMessage("v0", 1))
		require.NoError(t, err)
		require.NotNil(t, appended)
		assertTargetUntouched(t, m)
	})
}

// TestNameGateStaleVChannelDoesNotTouchTheSplitTarget: a vchannel of the same
// collection that is neither held nor fenced here (for example a source whose
// retired meta was already collected, so no tombstone survived a restart) while
// the target holds the slot. ManualFlush stays refused; the broadcast DDL
// replicas are appended; nothing reaches the target.
func TestNameGateStaleVChannelDoesNotTouchTheSplitTarget(t *testing.T) {
	for name, msg := range gatedDDLMessages("v-stale", 1, 2) {
		t.Run(name, func(t *testing.T) {
			m := newTestShardManagerWithSplitTarget(t)
			i := newTestShardInterceptorWithManager(t, m)

			_, appended, _, err := appendAndCapture(t, i, msg)
			if msg.MessageType() == message.MessageTypeManualFlush {
				assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
				assert.Nil(t, appended)
			} else {
				require.NoError(t, err)
				require.NotNil(t, appended)
			}
			assertTargetUntouched(t, m)
		})
	}
}

// TestNameGateFencedSourceDMLIsStillShardFenced: the gate does not change DML:
// Insert and Delete to the fenced source are rejected with SHARD_FENCED so the
// proxy refreshes its route, and never appended.
func TestNameGateFencedSourceDMLIsStillShardFenced(t *testing.T) {
	m := newTestShardManagerWithSplitTarget(t)
	i := newTestShardInterceptorWithManager(t, m)

	for name, msg := range map[string]message.MutableMessage{
		"Insert": newTestInsertMutableMessage("v0", 1, 2),
		"Delete": newTestDeleteMutableMessage("v0", 1),
	} {
		t.Run(name, func(t *testing.T) {
			msgID, appended, _, err := appendAndCapture(t, i, msg)
			assert.Nil(t, msgID)
			assert.Nil(t, appended)
			assert.True(t, status.AsStreamingError(err).IsShardFenced())
		})
	}
	assertTargetUntouched(t, m)
}

// TestNameGateHeldVChannelIsUnchanged is the regression side: the same
// ManualFlush addressed to the vchannel that holds the slot still seals its
// growing segment and reports it.
func TestNameGateHeldVChannelIsUnchanged(t *testing.T) {
	m := newTestShardManagerWithSplitTarget(t)
	i := newTestShardInterceptorWithManager(t, m)

	msg := gatedDDLMessages("v1", 1, 2)["ManualFlush"]
	_, appended, extra, err := appendAndCapture(t, i, msg)
	require.NoError(t, err)
	require.NotNil(t, appended)
	resp, ok := extra.Extra.(*message.ManualFlushExtraResponse)
	require.True(t, ok)
	assert.Equal(t, []int64{1000}, resp.GetSegmentIds())
	assert.Equal(t, []int64{1000}, message.MustAsMutableManualFlushMessageV2(appended).Header().GetSegmentIds())
}
