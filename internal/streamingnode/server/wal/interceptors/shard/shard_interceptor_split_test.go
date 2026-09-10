package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// newTestSplitShardMutableMessage builds one replica of a split broadcast: the
// vchannel it landed on decides its role, the header is shared by every replica
// of the broadcast, and the body carries the genesis the targets register from.
func newTestSplitShardMutableMessage(vchannel string, header *message.SplitShardMessageHeader, schema *schemapb.CollectionSchema) message.MutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: schema},
		}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
}

// newTestSplitShardHeader is the header the split coordinator broadcasts: one
// source (v0) carved into two targets (v1, v2).
func newTestSplitShardHeader(collectionID int64, splitTaskID int64, source string, targets ...string) *message.SplitShardMessageHeader {
	header := &message.SplitShardMessageHeader{
		CollectionId:    collectionID,
		SplitTaskId:     splitTaskID,
		SourceVchannels: []string{source},
		RoutingModulus:  2,
		PartitionIds:    []int64{2},
	}
	for i, target := range targets {
		header.Targets = append(header.Targets, &message.SplitShardTarget{
			Vchannel: target,
			Routing:  &schemapb.HashRouting{Buckets: []uint64{uint64(i)}},
		})
	}
	return header
}

// newTestRetireAlterCollectionMutableMessage builds the routing commit that
// retires a vchannel: the shard-split routing mask plus a new vchannel list
// that no longer names it.
func newTestRetireAlterCollectionMutableMessage(vchannel string, collectionID int64, kept []string) message.MutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{VirtualChannelNames: kept},
		}).
		MustBuildMutable().
		WithTimeTick(200).
		WithLastConfirmedUseMessageID()
}

func newTestShardInterceptor(t *testing.T) (interceptors.Interceptor, *mock_shards.MockShardManager) {
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	t.Cleanup(i.Close)
	return i, shardManager
}

func TestSplitShardOnSourceFencesAndSealsGrowing(t *testing.T) {
	collectionID := int64(99301)
	vchannel := "by-dev-rootcoord-dml_9_99301v0"

	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(collectionID, vchannel).Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(collectionID, uint64(100)).Return([]int64{7, 8}, nil).Once()
	shardManager.EXPECT().SplitShard(mock.Anything).Once()

	// The vchannel took the WAL function-runner key when it was created; the
	// fence is where it stops taking writes and loses its registration, so it
	// is also where the key has to go back. Waiting for the retire would leak
	// it across a WAL close in between: Close releases by REGISTERED vchannel,
	// and this one no longer has a registration to be found under.
	key := walFunctionRunnerKey(vchannel)
	require.NoError(t, function.GetManager().Alloc(collectionID, key, &schemapb.CollectionSchema{}))

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage(vchannel, newTestSplitShardHeader(collectionID, 42, vchannel, "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.NotNil(t, appendedMsg)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	// the auto-flushed segment ids are embedded into the split message header,
	// the single seal record for T_switch.
	header := message.MustAsMutableSplitShardMessageV2(appendedMsg).Header()
	assert.Equal(t, []int64{7, 8}, header.GetFlushedSegmentIds())

	_, err = function.GetManager().Materialize(context.Background(), collectionID, key, 0,
		&stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.Error(t, err, "the fenced source's function runner key must be released at the fence")
}

// TestSplitShardOnSourceAlreadyFencedByThisTaskAppendsAndRaisesTheTick pins the
// property that lets the broadcaster re-drive a split.
//
// A split whose source replica landed but whose task was not yet persisted is
// re-driven from the beginning, so the fence arrives at an already-fenced
// source. Refusing it there would spin forever; appending it again is what the
// re-drive needs, and the apply that follows only raises the recorded fence
// tick to this record's -- T_switch is the tick of the task's LATEST fence
// record, the value DataCoord recorded, and the two records seal the same data
// because the vchannel took no DML in between. The re-fence must NOT re-seal:
// the growing segments were sealed by the first attempt, and asking for a seal
// at this tick is what would make the second append disagree with the first.
func TestSplitShardOnSourceAlreadyFencedByThisTaskAppendsAndRaisesTheTick(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()
	shardManager.EXPECT().GetSplitFence(int64(1), "v0").
		Return(shards.SplitFence{TimeTick: 100, TaskID: 42}).Once()
	shardManager.EXPECT().SplitShard(mock.Anything).Once()

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	require.NotNil(t, appendedMsg)
	// no re-seal: the segments were sealed by the attempt that placed the fence.
	shardManager.AssertNotCalled(t, "FlushAndFenceSegmentAllocUntil", mock.Anything, mock.Anything)
	assert.Empty(t, message.MustAsMutableSplitShardMessageV2(appendedMsg).Header().GetFlushedSegmentIds())
}

// TestSplitShardOnSourceFencedByAnotherTaskIsRefused: one active split task per
// source is a coordinator invariant, and this refusal is what keeps two tasks
// from carving one source twice. The recorded T_switch and task id travel back
// on the error so the coordinator can recover them after a crash.
func TestSplitShardOnSourceFencedByAnotherTaskIsRefused(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()
	shardManager.EXPECT().GetSplitFence(int64(1), "v0").
		Return(shards.SplitFence{TimeTick: 1900, TaskID: 41}).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a source fenced by another task must not be appended")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.True(t, streamErr.IsUnrecoverable())
	assert.True(t, streamErr.IsShardFenced())
	assert.Equal(t, uint64(1900), streamErr.FencedTimeTick)
	assert.Equal(t, int64(41), streamErr.FencedSplitTaskId)
}

// TestSplitShardOnSourceFencedWithZeroTaskIDIsRefused: a recorded fence whose
// TaskID is zero is not "no task recorded" -- GetSplitFence is only reached
// after CheckIfVChannelCanBeWritten reports the vchannel fenced, so a fence
// exists. A zero task id there is a coordinator bug (every fence SplitShard
// places carries the placing task's id), never a legacy/absent fence, so it
// must be refused exactly like a fence placed by another task: no append, no
// re-seal.
func TestSplitShardOnSourceFencedWithZeroTaskIDIsRefused(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()
	shardManager.EXPECT().GetSplitFence(int64(1), "v0").
		Return(shards.SplitFence{TimeTick: 100, TaskID: 0}).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a fence recorded with a zero task id must not be appended")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.True(t, streamErr.IsUnrecoverable())
	assert.True(t, streamErr.IsShardFenced())
	assert.Equal(t, uint64(100), streamErr.FencedTimeTick)
	assert.Equal(t, int64(0), streamErr.FencedSplitTaskId)
	shardManager.AssertNotCalled(t, "FlushAndFenceSegmentAllocUntil", mock.Anything, mock.Anything)
}

func TestSplitShardOnTargetRegistersTheGenesis(t *testing.T) {
	// A split target is created live, so nothing else registers its WAL
	// function-runner lifecycle key until the WAL is next recovered. Without it
	// every insert to the new shard is rejected at materializeFunctionFields
	// with "function runner schema for key WAL-<vchannel> is not available" —
	// which made a rehashed collection unwritable for as long as the process
	// stayed up, even though it declares no function at all.
	collectionID := int64(99101)
	vchannel := "by-dev-rootcoord-dml_9_99101v2"
	key := walFunctionRunnerKey(vchannel)
	schema := &schemapb.CollectionSchema{
		Version: 3,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}

	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(collectionID, vchannel).Return(nil).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()
	// the genesis registered, so the function-runner key is allocated.
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(collectionID, vchannel).Return(nil).Once()

	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage(vchannel,
			newTestSplitShardHeader(collectionID, 100, "v0", vchannel), schema),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	defer function.GetManager().Release(collectionID, key)

	// Materialize is what the insert path calls; before the fix it failed here.
	_, err = function.GetManager().Materialize(context.Background(), collectionID, key,
		schema.GetVersion(), &stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.NoError(t, err)
}

// The shard manager would register a nil schema and the recovery storage an
// empty non-nil one, so the vchannel would behave differently before and after a
// restart. The interceptor is the only point that can enforce this against any
// coordinator version, and CreateCollection has had the same guard all along.
func TestSplitShardOnTargetWithoutSchemaIsRefused(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v1", newTestSplitShardHeader(7, 100, "v0", "v1"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a genesis without a schema must not be appended")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

// TestSplitShardOnTargetConflictIsRefused pins the contract the split
// coordinator has to honor.
//
// The shard manager keys its registrations by collection id, one entry per
// pchannel. If a target lands on a pchannel that still holds another vchannel of
// the same collection, appending its genesis anyway would give the new shard a
// WAL entry and a recovery-storage entry but no segment assignment -- and, when
// the incumbent is the fenced split source, an inherited fence that leaves the
// new shard unwritable for as long as the process lives. Refusing the append is
// what turns that silent, permanent breakage into a visible failure.
func TestSplitShardOnTargetConflictIsRefused(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v1").
		Return(errors.Wrap(shards.ErrVChannelConflict, "registered as v0")).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v1", newTestSplitShardHeader(7, 100, "v0", "v1"),
			&schemapb.CollectionSchema{Name: "test"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the genesis must not be appended when the pchannel holds another vchannel of the collection")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestSplitShardOnTargetExistingIsIdempotent(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// the collection already exists on this pchannel: the genesis is still
	// appended and applied (idempotent), only a warning is logged.
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v1").Return(shards.ErrCollectionExists).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(7), "v1").Return(nil).Once()

	appended := false
	_, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v1", newTestSplitShardHeader(7, 100, "v0", "v1"),
			&schemapb.CollectionSchema{Name: "test"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
}

// The apply re-checks under the write lock and may skip the registration, so
// the function-runner key must not be allocated for a genesis that did not
// register: Close releases by REGISTERED vchannel, and an unregistered one is
// never released.
func TestSplitShardOnTargetNotRegisteredSkipsFunctionRunner(t *testing.T) {
	collectionID := int64(99102)
	vchannel := "by-dev-rootcoord-dml_9_99102v2"

	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(collectionID, vchannel).Return(nil).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(collectionID, vchannel).
		Return(shards.ErrVChannelConflict).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage(vchannel,
			newTestSplitShardHeader(collectionID, 100, "v0", vchannel),
			&schemapb.CollectionSchema{Name: "test"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))

	_, err = function.GetManager().Materialize(context.Background(), collectionID,
		walFunctionRunnerKey(vchannel), 0, &stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.Error(t, err, "an unregistered genesis must not leak a function runner key")
}

func TestSplitShardOnTargetAppendFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v1").Return(nil).Once()

	_, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v1", newTestSplitShardHeader(7, 100, "v0", "v1"),
			&schemapb.CollectionSchema{Name: "test"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}

// TestSplitShardOnUnknownVChannelIsRefused: a replica on a vchannel the split
// neither fences nor creates is a coordinator bug. Fencing or registering a
// stranger would take a live shard down, so it is refused.
func TestSplitShardOnUnknownVChannelIsRefused(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v9", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a misrouted split replica must not be appended")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.True(t, streamErr.IsUnrecoverable())
	assert.False(t, streamErr.IsShardFenced())
}

// TestSplitShardOnBystanderAppendsWithoutEffect: the broadcast now covers
// every vchannel of the collection, so a replica landing on a vchannel that
// is neither a source, a target nor the control channel -- but still belongs
// to the same collection -- is a BYSTANDER, not a misroute. It must be
// appended so every replica of the broadcast is observable, but it must take
// no shard-manager action: there is nothing here to fence or register.
func TestSplitShardOnBystanderAppendsWithoutEffect(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	// "p0_1v3" is collection 1's third shard: neither the source "p0_1v0" nor
	// one of the targets "p0_1v1"/"p0_1v2", but the same collection.
	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("p0_1v3", newTestSplitShardHeader(1, 42, "p0_1v0", "p0_1v1", "p0_1v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	// no mock expectation was set on shardManager: any call would fail the test.
}

// TestSplitShardOnControlChannelIsPassedThrough pins the assumption the
// refusal above rests on: the control-channel replica of the broadcast, which
// only orders the ack callback, is skipped by DoAppend before the role
// dispatcher ever sees it. If it did reach the dispatcher it would be refused
// as misrouted, and every split would fail on its control replica.
func TestSplitShardOnControlChannelIsPassedThrough(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage(funcutil.GetControlChannel("by-dev-rootcoord-dml_9"),
			newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
}

func TestSplitShardOnSourceUnknownCollectionIsRefused(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrCollectionNotFound).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on an unknown collection")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
	assert.False(t, status.AsStreamingError(err).IsShardFenced())
}

func TestSplitShardOnSourceFlushFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).
		Return(nil, errors.New("mock flush error")).Once()

	msgID, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called when the flush fails")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.Error(t, err)
}

func TestSplitShardOnSourceAppendFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).Return([]int64{7}, nil).Once()

	_, err := i.DoAppend(context.Background(),
		newTestSplitShardMutableMessage("v0", newTestSplitShardHeader(1, 42, "v0", "v1", "v2"), nil),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}

// TestAlterCollectionRetireIsAppendedWithoutEffect: nothing at all is left for
// the retire to do. The source's registration went at the fence and so did its
// function-runner key, so this replica is appended and that is all -- it must
// not go through the collection-wide alter path, which is keyed by collection
// id and would flush and re-schema whichever vchannel now holds the entry.
func TestAlterCollectionRetireIsAppendedWithoutEffect(t *testing.T) {
	collectionID := int64(99203)
	vchannel := "by-dev-rootcoord-dml_9_99203v0"

	// no expectation is set on the shard manager: ANY call to it fails the test.
	i, shardManager := newTestShardInterceptor(t)
	key := walFunctionRunnerKey(vchannel)
	require.NoError(t, function.GetManager().Alloc(collectionID, key, &schemapb.CollectionSchema{}))
	defer function.GetManager().Release(collectionID, key)

	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestRetireAlterCollectionMutableMessage(vchannel, collectionID, []string{"v1", "v2"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	shardManager.AssertNotCalled(t, "AlterCollection", mock.Anything)

	// the retire releases nothing: a key still allocated here is one the fence
	// has not passed yet, and it belongs to whoever holds it.
	_, err = function.GetManager().Materialize(context.Background(), collectionID, key, 0,
		&stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.NoError(t, err)
}

func TestAlterCollectionRetireAppendFailure(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	_, err := i.DoAppend(context.Background(),
		newTestRetireAlterCollectionMutableMessage("v0", 1, []string{"v1", "v2"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}

// TestAlterCollectionOnAListedVChannelIsUnchanged: the same routing commit
// lands on every shard of the collection, and on the ones it keeps it is an
// ordinary AlterCollection.
func TestAlterCollectionOnAListedVChannelIsUnchanged(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// this pchannel holds the vchannel the replica names, so the collection-wide
	// apply runs as it always did.
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(nil).Once()
	shardManager.EXPECT().AlterCollection(mock.Anything).Return(nil, nil).Once()

	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestRetireAlterCollectionMutableMessage("v0", 1, []string{"v0", "v1", "v2"}),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
}

func TestShardInterceptorInsertOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckWritableAndSchemaVersion("v1", mock.Anything).
		Return(int32(-1), shards.ErrVChannelFenced).Once()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsShardFenced())
}

func TestShardInterceptorDeleteOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrVChannelFenced).Once()

	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.DeleteMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsShardFenced())
}

// A split is the first thing that puts two vchannels of one collection on one
// pchannel in sequence, so a proxy holding a stale route to a retired source can
// reach a shard manager that holds only the successor. The admission check
// answers ErrCollectionNotFound for that; letting it through would append the
// message carrying the retired vchannel while its segment belongs to the live
// one -- the flusher then finds no data sync service for it and drops the batch,
// recovery counts the rows against the other segment, and the client is told the
// append succeeded. Silent loss, so it has to be a rejection.
func TestShardInterceptorInsertOnAVChannelTheManagerDoesNotHold(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckWritableAndSchemaVersion("v1", mock.Anything).
		Return(int32(-1), shards.ErrCollectionNotFound).Once()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append must not run for a vchannel this shard manager does not hold")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.False(t, streamErr.IsShardFenced(), "not a fence: refreshing the route does not make this vchannel writable here")
	assert.True(t, streamErr.IsUnrecoverable())
}

func TestShardInterceptorDeleteOnAVChannelTheManagerDoesNotHold(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrCollectionNotFound).Once()

	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.DeleteMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append must not run for a vchannel this shard manager does not hold")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.False(t, streamErr.IsShardFenced())
	assert.True(t, streamErr.IsUnrecoverable())
}

// stubInsertMessage is the smallest thing satisfying function.InsertMessage:
// Materialize only reads and rewrites the body, and this test is about whether
// the runner key exists at all, not about what the body holds.
type stubInsertMessage struct{ body *msgpb.InsertRequest }

func (s *stubInsertMessage) MustBody() *msgpb.InsertRequest { return s.body }

func (s *stubInsertMessage) OverwriteBody(body *msgpb.InsertRequest) { s.body = body }

// newTestSchemaChangeAlterCollectionMutableMessage builds an ordinary
// collection update -- a schema change, the alter that does the most work on a
// shard -- addressed to one vchannel of the collection.
func newTestSchemaChangeAlterCollectionMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{Name: "col", Version: 4},
			},
		}).
		MustBuildMutable().
		WithTimeTick(200).
		WithLastConfirmedUseMessageID()
}

// A collection-wide DDL broadcast reaches EVERY vchannel of the collection, and
// a fenced source stays on that list until adoption retires it -- hours later.
// Its replica has nothing to do here (the registration went at the fence), but
// it must still be appended: refusing it is unrecoverable, and the broadcaster
// retries an unrecoverable replica forever while holding the collection's
// exclusive key, so one schema change during a split would wedge every later
// DDL on the collection.
func TestSchemaChangeOnAFencedSourceIsAppendedWithoutEffect(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()

	msg := message.NewSchemaChangeMessageBuilderV2().
		WithVChannel("v0").
		WithHeader(&messagespb.SchemaChangeMessageHeader{CollectionId: 1}).
		WithBody(&messagespb.SchemaChangeMessageBody{}).
		MustBuildMutable().WithTimeTick(200)

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	require.NotNil(t, appendedMsg)
	shardManager.AssertNotCalled(t, "FlushAndFenceSegmentAllocUntil", mock.Anything, mock.Anything)
	assert.Empty(t, message.MustAsMutableSchemaChangeMessageV2(appendedMsg).Header().GetFlushedSegmentIds())
}

func TestAlterCollectionOnAFencedSourceIsAppendedWithoutEffect(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(),
		newTestSchemaChangeAlterCollectionMutableMessage("v0", 1),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	require.NotNil(t, appendedMsg)
	shardManager.AssertNotCalled(t, "AlterCollection", mock.Anything)
	assert.Empty(t, message.MustAsMutableAlterCollectionMessageV2(appendedMsg).Header().GetFlushedSegmentIds())
}

func TestTruncateOnAFencedSourceIsAppendedWithoutEffect(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v0").Return(shards.ErrVChannelFenced).Once()

	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v0").
		WithHeader(&messagespb.TruncateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&messagespb.TruncateCollectionMessageBody{}).
		MustBuildMutable().WithTimeTick(200)

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	require.NotNil(t, appendedMsg)
	shardManager.AssertNotCalled(t, "FlushAndFenceSegmentAllocUntil", mock.Anything, mock.Anything)
	assert.Empty(t, message.MustAsMutableTruncateCollectionMessageV2(appendedMsg).Header().GetSegmentIds())
}

// TestSourceAddressedAlterCollectionDoesNotTouchTheSuccessor: the fence frees
// the pchannel slot, so a successor vchannel of the SAME collection can already
// hold this pchannel's entry when a replica addressed to the old source
// arrives. Every remaining action in these handlers is keyed by collection id
// alone -- FlushAndFenceSegmentAllocUntil and AlterCollection both are -- so
// acting on a source-addressed replica would seal the successor's growing
// segments and overwrite its schema.
func TestSourceAddressedAlterCollectionDoesNotTouchTheSuccessor(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// the entry names "v1-successor", so the manager does not hold "v1".
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrCollectionNotFound).Once()

	appended := false
	msgID, err := i.DoAppend(context.Background(),
		newTestSchemaChangeAlterCollectionMutableMessage("v1", 1),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	shardManager.AssertNotCalled(t, "AlterCollection", mock.Anything)
	shardManager.AssertNotCalled(t, "FlushAndFenceSegmentAllocUntil", mock.Anything, mock.Anything)
}
