package shards

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestShardManager(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1000,
	}, nil).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	m := RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: channel,
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v1": {
					Vchannel: "v1",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 1,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 2},
							{PartitionId: 3},
						},
					},
				},
				"v2": {
					Vchannel: "v2",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 4,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 5},
							{PartitionId: 6},
						},
					},
				},
			},
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
				1001: {
					CollectionId:   1,
					PartitionId:    2,
					SegmentId:      1001,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						ModifiedBinarySize:    50,
						CreateSegmentTimeTick: 101,
					},
				},
				1002: {
					CollectionId:   1,
					PartitionId:    3,
					SegmentId:      1002,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						ModifiedBinarySize:    0,
						CreateSegmentTimeTick: 100,
					},
				},
				1013: {
					CollectionId:   4,
					PartitionId:    5,
					SegmentId:      1013,
					State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					StorageVersion: 2,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         100,
						ModifiedBinarySize:    0,
						CreateSegmentTimeTick: 100,
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{
				TimeTick: 300,
			},
		},
		TxnManager: &mockedTxnManager{},
	}).(*shardManagerImpl)
	assert.Equal(t, channel, m.Channel())

	// Test Checkers
	err := m.CheckIfCollectionCanBeCreated(1)
	assert.ErrorIs(t, err, ErrCollectionExists)
	err = m.CheckIfCollectionCanBeCreated(3)
	assert.NoError(t, err)

	err = m.CheckIfCollectionExists(3)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfCollectionExists(1)
	assert.NoError(t, err)

	err = m.CheckIfPartitionCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: common.AllPartitionsID})
	assert.ErrorIs(t, err, ErrPartitionExists)
	err = m.CheckIfPartitionCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: 2})
	assert.ErrorIs(t, err, ErrPartitionExists)
	err = m.CheckIfPartitionCanBeCreated(PartitionUniqueKey{CollectionID: 3, PartitionID: 9})
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfPartitionCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: 7})
	assert.NoError(t, err)
	err = m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 1, PartitionID: 7})
	assert.ErrorIs(t, err, ErrPartitionNotFound)
	err = m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 1, PartitionID: 2})
	assert.NoError(t, err)

	err = m.CheckIfSegmentCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1001)
	assert.ErrorIs(t, err, ErrSegmentExists)
	err = m.CheckIfSegmentCanBeCreated(PartitionUniqueKey{CollectionID: 3, PartitionID: 2}, 1001)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfSegmentCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: 4}, 1001)
	assert.ErrorIs(t, err, ErrPartitionNotFound)
	err = m.CheckIfSegmentCanBeCreated(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1003)
	assert.NoError(t, err)

	err = m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1001)
	assert.ErrorIs(t, err, ErrSegmentOnGrowing)
	err = m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1003)
	assert.ErrorIs(t, err, ErrSegmentNotFound)
	err = m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 3, PartitionID: 8}, 1001)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 1, PartitionID: 7}, 1001)
	assert.ErrorIs(t, err, ErrPartitionNotFound)

	// Test Create and Drop
	createCollectionMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v3").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 7,
			PartitionIds: []int64{8, 9},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(400).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(1))
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createCollectionMsg))
	assert.NoError(t, m.CheckIfCollectionExists(7))
	assert.NoError(t, m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 7, PartitionID: 8}))
	assert.NoError(t, m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 7, PartitionID: 9}))
	assert.NoError(t, m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 7, PartitionID: common.AllPartitionsID}))

	createPartitionMsg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v3").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 7,
			PartitionId:  10,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(500).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(2))
	m.CreatePartition(message.MustAsImmutableCreatePartitionMessageV1(createPartitionMsg))
	assert.NoError(t, m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 7, PartitionID: 10}))

	createSegmentMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v3").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   7,
			PartitionId:    10,
			SegmentId:      1003,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(600).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(3))
	m.partitionManagers[PartitionUniqueKey{CollectionID: 7, PartitionID: 10}].onAllocating = make(chan struct{})
	ch, err := m.WaitUntilGrowingSegmentReady(PartitionUniqueKey{CollectionID: 7, PartitionID: 10})
	assert.NoError(t, err)
	select {
	case <-time.After(10 * time.Millisecond):
	case <-ch:
		t.Error("segment should not be ready")
	}
	m.CreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createSegmentMsg))
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 7, PartitionID: 10}, 1003), ErrSegmentOnGrowing)
	<-ch
	ch, err = m.WaitUntilGrowingSegmentReady(PartitionUniqueKey{CollectionID: 7, PartitionID: 10})
	assert.NoError(t, err)
	<-ch

	flushSegmentMsg := message.NewFlushMessageBuilderV2().
		WithVChannel("v3").
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 7,
			PartitionId:  10,
			SegmentId:    1003,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(700).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))
	m.AsyncFlushSegment(utils.SealSegmentSignal{
		SegmentBelongs: utils.SegmentBelongs{
			CollectionID: 7,
			PartitionID:  10,
			SegmentID:    1003,
		},
		SealPolicy: policy.PolicyCapacity(),
	})
	m.FlushSegment(message.MustAsImmutableFlushMessageV2(flushSegmentMsg))
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 7, PartitionID: 10}, 1003), ErrSegmentNotFound)

	dropPartitionMsg := message.NewDropPartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  2,
		}).
		WithBody(&msgpb.DropPartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(600).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(7))
	m.DropPartition(message.MustAsImmutableDropPartitionMessageV1(dropPartitionMsg))
	assert.ErrorIs(t, m.CheckIfPartitionExists(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}), ErrPartitionNotFound)

	dropCollectionMsg := message.NewDropCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(700).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(8))
	m.DropCollection(message.MustAsImmutableDropCollectionMessageV1(dropCollectionMsg))
	assert.ErrorIs(t, m.checkIfCollectionExists(1), ErrCollectionNotFound)

	result, err := m.AssignSegment(&AssignSegmentRequest{
		CollectionID: 4,
		PartitionID:  5,
		TimeTick:     800,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       1,
			BinarySize: 20,
		},
	})
	assert.NoError(t, err)
	result.Ack()
	assert.Equal(t, result.SegmentID, int64(1013))

	segmentIDs, err := m.FlushAndFenceSegmentAllocUntil(4, 1000)
	assert.NoError(t, err)
	assert.Equal(t, len(segmentIDs), 1)
	assert.Equal(t, segmentIDs[0], int64(1013))
	m.Close()
}

func TestShardManagerSchemaVersionCheck(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel_schema",
		Term: 1,
	}
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1000,
	}, nil).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	m := RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: channel,
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels:          map[string]*streamingpb.VChannelMeta{},
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
			Checkpoint:         &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &mockedTxnManager{},
	}).(*shardManagerImpl)

	// Test 1: CheckIfCollectionSchemaVersionMatch on non-existent collection
	_, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  999,
		SchemaVersion: proto.Int32(1),
	})
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	// Test 2: Create collection with schema, then check version match
	schema := &schemapb.CollectionSchema{
		Name:    "test_schema_collection",
		Version: 1,
	}
	createMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v_schema").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{200},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionSchema: schema,
		}).
		MustBuildMutable().
		WithTimeTick(200).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(10))
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createMsg))

	// version match should succeed when header carries explicit schema version
	ver, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  100,
		SchemaVersion: proto.Int32(1),
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), ver)

	// legacy insert (no schema_version field): skip schema validation and return current version
	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 100})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), ver)

	// version mismatch should fail
	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  100,
		SchemaVersion: proto.Int32(2),
	})
	assert.ErrorIs(t, err, ErrCollectionSchemaVersionNotMatch)
	assert.Equal(t, int32(1), ver)

	// Test 3: Create collection without schema (legacy), then check version
	createMsgNoSchema := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v_no_schema").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 101,
			PartitionIds: []int64{201},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(300).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(11))
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createMsgNoSchema))

	// collection exists but has no schema — explicit version in header requires schema metadata
	_, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  101,
		SchemaVersion: proto.Int32(1),
	})
	assert.ErrorIs(t, err, ErrCollectionSchemaNotFound)

	_, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  101,
		SchemaVersion: proto.Int32(0),
	})
	assert.ErrorIs(t, err, ErrCollectionSchemaNotFound)

	// legacy insert while collection schema version is still 0: allow
	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 101})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), ver)

	createMsgSchemaVersionZero := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v_schema_zero").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 103,
			PartitionIds: []int64{203},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{
				Name:    "test_schema_zero_collection",
				Version: 0,
			},
		}).
		MustBuildMutable().
		WithTimeTick(350).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(12))
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createMsgSchemaVersionZero))

	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  103,
		SchemaVersion: proto.Int32(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), ver)

	// Test 4: AlterCollection updates schema version
	updatedSchema := &schemapb.CollectionSchema{
		Name:    "test_schema_collection",
		Version: 2,
	}
	alterMsg := message.MustAsMutableAlterCollectionMessageV2(
		message.NewAlterCollectionMessageBuilderV2().
			WithVChannel("v_schema").
			WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: 100,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
			}).
			WithBody(&message.AlterCollectionMessageBody{
				Updates: &message.AlterCollectionMessageUpdates{
					Schema: updatedSchema,
				},
			}).
			MustBuildMutable().
			WithTimeTick(500),
	)
	_, err = m.AlterCollection(alterMsg)
	assert.NoError(t, err)

	// now version 2 matches, version 1 doesn't
	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  100,
		SchemaVersion: proto.Int32(2),
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), ver)

	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  100,
		SchemaVersion: proto.Int32(1),
	})
	assert.ErrorIs(t, err, ErrCollectionSchemaVersionNotMatch)
	assert.Equal(t, int32(2), ver)

	// Test 5: AlterCollection on non-existent collection
	alterMsgBad := message.MustAsMutableAlterCollectionMessageV2(
		message.NewAlterCollectionMessageBuilderV2().
			WithVChannel("v_schema").
			WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: 999,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
			}).
			WithBody(&message.AlterCollectionMessageBody{
				Updates: &message.AlterCollectionMessageUpdates{
					Schema: updatedSchema,
				},
			}).
			MustBuildMutable().
			WithTimeTick(600),
	)
	_, err = m.AlterCollection(alterMsgBad)
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	// Test 6: outer CollectionSchemaOfVChannel set but inner schema nil — treat as no schema (no panic).
	m.mu.Lock()
	m.collections[102] = &CollectionInfo{
		VChannel: "v_corrupt",
		PartitionIDs: map[int64]struct{}{
			common.AllPartitionsID: {},
		},
		Schema: &streamingpb.CollectionSchemaOfVChannel{
			Schema:             nil,
			CheckpointTimeTick: 1,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		},
	}
	m.mu.Unlock()
	_, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId:  102,
		SchemaVersion: proto.Int32(1),
	})
	assert.ErrorIs(t, err, ErrCollectionSchemaNotFound)

	// legacy insert while effective collection schema version is still 0 (nil inner schema)
	ver, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 102})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), ver)

	m.Close()
}

// newShardManagerWithGrowingSegment builds a minimal shard manager that has
// collection collID / partition partID / growing segment segID pre-loaded.
func newShardManagerWithGrowingSegment(t *testing.T, collID, partID, segID int64) *shardManagerImpl {
	t.Helper()
	channel := types.PChannelInfo{Name: "test_alter_channel", Term: 1}
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  9999,
	}, nil).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	return RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: channel,
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v_alter": {
					Vchannel: "v_alter",
					State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: collID,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: partID},
						},
					},
				},
			},
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
				segID: {
					CollectionId: collID,
					PartitionId:  partID,
					SegmentId:    segID,
					State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					Stat: &streamingpb.SegmentAssignmentStat{
						MaxBinarySize:         200,
						ModifiedBinarySize:    100,
						CreateSegmentTimeTick: 50,
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &mockedTxnManager{},
	}).(*shardManagerImpl)
}

func TestAlterCollectionSchemaChange(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	const (
		collID   = int64(10)
		partID   = int64(20)
		segID    = int64(3001)
		vchan    = "v_alter"
		timeTick = uint64(500)
	)

	schemaV1 := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	schemaV2 := &schemapb.CollectionSchema{Name: "coll", Version: 2}

	// helper to build an AlterCollection mutable message
	buildAlterMsg := func(mask []string, schema *schemapb.CollectionSchema, _ int64) message.MutableAlterCollectionMessageV2 {
		var updateMask *fieldmaskpb.FieldMask
		if mask != nil {
			updateMask = &fieldmaskpb.FieldMask{Paths: mask}
		}
		return message.MustAsMutableAlterCollectionMessageV2(
			message.NewAlterCollectionMessageBuilderV2().
				WithVChannel(vchan).
				WithHeader(&message.AlterCollectionMessageHeader{
					CollectionId: collID,
					UpdateMask:   updateMask,
				}).
				WithBody(&message.AlterCollectionMessageBody{
					Updates: &message.AlterCollectionMessageUpdates{Schema: schema},
				}).
				MustBuildMutable().
				WithTimeTick(timeTick),
		)
	}

	t.Run("schema change flushes and fences growing segments and updates schema", func(t *testing.T) {
		m := newShardManagerWithGrowingSegment(t, collID, partID, segID)
		defer m.Close()

		// Set initial schema on the collection.
		m.mu.Lock()
		m.collections[collID].Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schemaV1,
			CheckpointTimeTick: 100,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		m.mu.Unlock()

		// AlterCollection with schema change mask + new schema body.
		segmentIDs, err := m.AlterCollection(buildAlterMsg([]string{message.FieldMaskCollectionSchema}, schemaV2, 20))
		assert.NoError(t, err)
		// The growing segment must have been flushed and fenced.
		assert.Equal(t, []int64{segID}, segmentIDs)
		// Schema must be updated to v2.
		ver, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
			CollectionId:  collID,
			SchemaVersion: proto.Int32(2),
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(2), ver)
	})

	t.Run("non-schema alter does not flush segments", func(t *testing.T) {
		m := newShardManagerWithGrowingSegment(t, collID, partID, segID)
		defer m.Close()

		// AlterCollection with a non-schema mask (e.g. properties only).
		segmentIDs, err := m.AlterCollection(buildAlterMsg([]string{"properties"}, nil, 21))
		assert.NoError(t, err)
		// No segments should be flushed.
		assert.Empty(t, segmentIDs)
	})

	t.Run("schema change with nil schema body returns error", func(t *testing.T) {
		m := newShardManagerWithGrowingSegment(t, collID, partID, segID)
		defer m.Close()

		// AlterCollection with schema change mask but nil schema in body — malformed message.
		segmentIDs, err := m.AlterCollection(buildAlterMsg([]string{message.FieldMaskCollectionSchema}, nil, 22))
		assert.Error(t, err)
		assert.Nil(t, segmentIDs)
	})

	t.Run("non-schema-change alter with non-nil schema body does not update schema", func(t *testing.T) {
		m := newShardManagerWithGrowingSegment(t, collID, partID, segID)
		defer m.Close()

		// Set initial schema v1.
		m.mu.Lock()
		m.collections[collID].Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schemaV1,
			CheckpointTimeTick: 100,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		m.mu.Unlock()

		// UpdateMask does not include schema field, but body carries a schema — should be ignored.
		segmentIDs, err := m.AlterCollection(buildAlterMsg([]string{"properties"}, schemaV2, 23))
		assert.NoError(t, err)
		assert.Empty(t, segmentIDs)
		// Schema must remain at v1, not updated to v2.
		ver, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
			CollectionId:  collID,
			SchemaVersion: proto.Int32(1),
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(1), ver)
	})

	t.Run("alter on non-existent collection returns ErrCollectionNotFound", func(t *testing.T) {
		m := newShardManagerWithGrowingSegment(t, collID, partID, segID)
		defer m.Close()

		badMsg := message.MustAsMutableAlterCollectionMessageV2(
			message.NewAlterCollectionMessageBuilderV2().
				WithVChannel(vchan).
				WithHeader(&message.AlterCollectionMessageHeader{
					CollectionId: 999,
					UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
				}).
				WithBody(&message.AlterCollectionMessageBody{
					Updates: &message.AlterCollectionMessageUpdates{Schema: schemaV2},
				}).
				MustBuildMutable().
				WithTimeTick(timeTick),
		)
		_, err := m.AlterCollection(badMsg)
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})
}

func TestCollectionInfoSchemaVersion(t *testing.T) {
	assert.Equal(t, int32(0), (*CollectionInfo)(nil).SchemaVersion())
	ci := &CollectionInfo{}
	assert.Equal(t, int32(0), ci.SchemaVersion())
	ci.Schema = &streamingpb.CollectionSchemaOfVChannel{}
	assert.Equal(t, int32(0), ci.SchemaVersion())
	ci.Schema = &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{Name: "x", Version: 3},
	}
	assert.Equal(t, int32(3), ci.SchemaVersion())
}
