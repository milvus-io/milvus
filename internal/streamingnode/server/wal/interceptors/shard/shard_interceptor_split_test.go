package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
)

func newTestSplitShardMutableMessage() message.MutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: 1,
			SplitTaskId:  100,
			Targets: []*message.SplitShardTarget{
				{Vchannel: "v2", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
				{Vchannel: "v3", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
			},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().
		WithTimeTick(100).
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

func TestShardInterceptorSplitShardMessage(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).Return([]int64{7}, nil).Once()
	shardManager.EXPECT().SplitShard(mock.Anything).Once()

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
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
	assert.Equal(t, []int64{7}, header.GetFlushedSegmentIds())
}

func newTestCreateVChannelMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:         collectionID,
			PartitionIds:         []int64{2},
			SplitTaskId:          100,
			SplitSourceVchannels: []string{"v1"},
			Routing:              &schemapb.HashRouting{Buckets: []uint64{0}},
			RoutingModulus:       2,
		}).
		WithBody(&message.CreateCollectionRequest{
			// A genesis without a schema is refused by the interceptor, so the
			// ordinary fixture carries the minimal one.
			CollectionSchema: &schemapb.CollectionSchema{Name: "test"},
		}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
}

// The shard manager would register a nil schema and the recovery storage an
// empty non-nil one, so the vchannel would behave differently before and after a
// restart. The interceptor is the only point that can enforce this against any
// coordinator version, and CreateCollection has had the same guard all along.
func TestShardInterceptorRefusesCreateVChannelWithoutSchema(t *testing.T) {
	i, _ := newTestShardInterceptor(t)

	msg := message.NewCreateVChannelMessageBuilderV2().
		WithVChannel("v2").
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:   7,
			PartitionIds:   []int64{2},
			Routing:        &schemapb.HashRouting{Buckets: []uint64{0}},
			RoutingModulus: 2,
		}).
		WithBody(&message.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a genesis without a schema must not be appended")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestShardInterceptorCreateVChannelMessage(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v2").Return(nil).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()
	// the genesis registered, so the function-runner key is allocated.
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(7), "v2").Return(nil).Once()

	appended := false
	msgID, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
}

func TestShardInterceptorCreateVChannelMessageAppendFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v2").Return(nil).Once()

	_, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}

func TestShardInterceptorCreateVChannelMessageOnExistingCollection(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// the collection already exists on this pchannel: the genesis is still
	// appended and applied (idempotent), only a warning is logged.
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v2").Return(shards.ErrCollectionExists).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(7), "v2").Return(nil).Once()

	appended := false
	_, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
}

func TestShardInterceptorSplitShardMessageOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// idempotent: the vchannel is already fenced by a previous split message;
	// the recorded T_switch is carried back on the error for crash recovery.
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrVChannelFenced).Once()
	shardManager.EXPECT().GetSplitTimeTick(int64(1), "v1").Return(uint64(1900)).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	streamErr := status.AsStreamingError(err)
	assert.True(t, streamErr.IsShardFenced())
	assert.Equal(t, uint64(1900), streamErr.FencedTimeTick)
}

func TestShardInterceptorSplitShardMessageOnUnknownCollection(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrCollectionNotFound).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on an unknown collection")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
	assert.False(t, status.AsStreamingError(err).IsShardFenced())
}

func TestShardInterceptorSplitShardMessageFlushFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).Return(nil, errors.New("mock flush error")).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called when the flush fails")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.Error(t, err)
}

func TestShardInterceptorInsertOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrVChannelFenced).Once()

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
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1), "v1").Return(shards.ErrCollectionNotFound).Once()

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

// newTestCreateVChannelMutableMessageWithSchema builds a split target's genesis
// message carrying the collection schema, as the coordinator sends it.
func newTestCreateVChannelMutableMessageWithSchema(vchannel string, collectionID int64, schema *schemapb.CollectionSchema) message.MutableMessage {
	return message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId: collectionID,
			PartitionIds: []int64{2},
			SplitTaskId:  100,
		}).
		WithBody(&message.CreateCollectionRequest{CollectionSchema: schema}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
}

func TestShardInterceptorCreateVChannelAllocatesFunctionRunners(t *testing.T) {
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
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(collectionID, vchannel).Return(nil).Once()

	_, err := i.DoAppend(context.Background(),
		newTestCreateVChannelMutableMessageWithSchema(vchannel, collectionID, schema),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	defer function.GetManager().Release(collectionID, key)

	// Materialize is what the insert path calls; before the fix it failed here.
	_, err = function.GetManager().Materialize(context.Background(), collectionID, key,
		schema.GetVersion(), &stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.NoError(t, err)
}

// stubInsertMessage is the smallest thing satisfying function.InsertMessage:
// Materialize only reads and rewrites the body, and this test is about whether
// the runner key exists at all, not about what the body holds.
type stubInsertMessage struct{ body *msgpb.InsertRequest }

func (s *stubInsertMessage) MustBody() *msgpb.InsertRequest { return s.body }

func (s *stubInsertMessage) OverwriteBody(body *msgpb.InsertRequest) { s.body = body }

func newTestDropVChannelMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewDropVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.DropVChannelMessageHeader{
			CollectionId: collectionID,
			SplitTaskId:  100,
		}).
		WithBody(&message.DropVChannelMessageBody{}).
		MustBuildMutable().
		WithTimeTick(200).
		WithLastConfirmedUseMessageID()
}

// TestShardInterceptorCreateVChannelRejectsForeignVChannel pins the contract the
// split coordinator has to honor.
//
// The shard manager keys its registrations by collection id, one entry per
// pchannel. If a target lands on a pchannel that still holds another vchannel of
// the same collection, appending its genesis anyway would give the new shard a
// WAL entry and a recovery-storage entry but no segment assignment -- and, when
// the incumbent is the fenced split source, an inherited fence that leaves the
// new shard unwritable for as long as the process lives. Refusing the append is
// what turns that silent, permanent breakage into a visible failure.
func TestShardInterceptorCreateVChannelRejectsForeignVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeCreated(int64(7), "v2").
		Return(errors.Wrap(shards.ErrVChannelConflict, "registered as v1")).Once()

	msgID, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the genesis must not be appended when the pchannel holds another vchannel of the collection")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestShardInterceptorDropVChannel(t *testing.T) {
	collectionID := int64(99201)
	vchannel := "by-dev-rootcoord-dml_9_99201v0"

	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeDropped(collectionID, vchannel).Return(nil).Once()
	shardManager.EXPECT().DropVChannel(mock.Anything).Once()

	// Creation took the WAL function-runner key per vchannel; the teardown must
	// give back exactly that one, or the key outlives the shard it belongs to.
	key := walFunctionRunnerKey(vchannel)
	require.NoError(t, function.GetManager().Alloc(collectionID, key, &schemapb.CollectionSchema{}))

	appended := false
	msgID, err := i.DoAppend(context.Background(), newTestDropVChannelMutableMessage(vchannel, collectionID),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))

	_, err = function.GetManager().Materialize(context.Background(), collectionID, key, 0,
		&stubInsertMessage{body: &msgpb.InsertRequest{}})
	assert.Error(t, err, "the retired vchannel's function runner key must be released")
}

// TestShardInterceptorDropVChannelRefusesLiveShard: a teardown may only retire a
// vchannel a shard split has fenced. Applying one to a live shard deletes its
// segment assignment with no way back -- every later DML on it fails with
// collection-not-found and nothing recreates the registration.
func TestShardInterceptorDropVChannelRefusesLiveShard(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeDropped(int64(1), "v1").
		Return(errors.Wrap(shards.ErrVChannelNotFenced, "state NORMAL")).Once()

	msgID, err := i.DoAppend(context.Background(), newTestDropVChannelMutableMessage("v1", 1),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "a live shard must never be torn down")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestShardInterceptorDropVChannelAppendFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeDropped(int64(1), "v1").Return(nil).Once()

	_, err := i.DoAppend(context.Background(), newTestDropVChannelMutableMessage("v1", 1),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}
