package growing

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestGrowingManagerReturnsNoBarrierForIrrelevantMessage(t *testing.T) {
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(10).IntoImmutableMessage(nil)

	manager := NewManager(nil, nil, nil)
	result := manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)

	manager.metaAndData = true
	result = manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerTruncateCollectionAdvancesVChannelMeta(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
			CheckpointTimeTick: 1,
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	mutableMsg := message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	result := manager.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	vchannel := manager.vChannels()["v1"].AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, vchannel.GetState())
	assert.Equal(t, uint64(10), vchannel.GetCheckpointTimeTick())
}

func TestGrowingManagerDoesNotFilterExistingVChannelByCollectionID(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
			CheckpointTimeTick: 1,
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 2,
			PartitionId:  20,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	result := manager.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	assert.True(t, hasPartitionMeta(manager.vChannels()["v1"].AssignmentMeta(), 20))
}

func TestSegmentViewObserveInsertUsesMetaAndDataWatermarksSeparately(t *testing.T) {
	segment := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat: &streamingpb.SegmentAssignmentStat{
				ModifiedRows:       7,
				ModifiedBinarySize: 70,
			},
		},
		0,
		0,
		false,
		writeOnlyInsertBuffer{},
		&schemapb.CollectionSchema{},
		runtimeConfig{
			metaAndData: true,
			flushPolicy: neverFlushPolicy{},
		},
	)
	assignment := &messagespb.PartitionSegmentAssignment{
		PartitionId:       10,
		Rows:              3,
		BinarySize:        30,
		SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
	}
	msg := newTestInsertMessage(t, 50, assignment)

	result := segment.ObserveInsertMessageV1(context.Background(), msg, assignment)

	assert.Nil(t, result.Meta)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(100), segment.meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(7), segment.meta.GetStat().GetModifiedRows())
	assert.Len(t, segment.pending.entries, 1)
	assert.Equal(t, uint64(50), segment.pending.DataTimeTick())

	duplicate := segment.ObserveInsertMessageV1(context.Background(), msg, assignment)
	assert.Nil(t, duplicate.Meta)
	assert.Nil(t, duplicate.Data)
	assert.Len(t, segment.pending.entries, 1)
}

func TestGrowingManagerObserveDeleteUsesDataWatermarkAndBufferTail(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogBufferMaxRows(100),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {CheckpointTimeTick: 10},
		}),
	)
	manager.metaAndData = true
	vchannel := manager.vChannels()["v1"]
	vchannel.metaAndData = true
	msg := newTestDeleteMessage(t, 50)

	result := manager.observeTransformLogMessage(vchannel, msg)

	require.NotNil(t, result.Data)

	duplicate := manager.observeTransformLogMessage(vchannel, msg)
	assert.Nil(t, duplicate.Meta)
	assert.Nil(t, duplicate.Data)

	persisted := newTestDeleteMessage(t, 8)
	persistedResult := manager.observeTransformLogMessage(vchannel, persisted)
	assert.Nil(t, persistedResult.Meta)
	assert.Nil(t, persistedResult.Data)
}

func TestGrowingManagerFlushTransformLogWritesChunkAndMeta(t *testing.T) {
	store := &recordingTransformLogStore{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogStore(store),
		WithTransformLogBufferMaxRows(100),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {CheckpointTimeTick: 10},
		}),
	)
	manager.metaAndData = true
	vchannel := manager.vChannels()["v1"]
	vchannel.metaAndData = true
	msg := newTestDeleteMessage(t, 50)

	result := manager.observeTransformLogMessage(vchannel, msg)
	require.NotNil(t, result.Data)

	task := manager.startFlushTransformLogBufferTask("v1", 50)
	require.NotNil(t, task)
	require.NoError(t, task.Run(context.Background()))

	require.Len(t, store.chunks, 1)
	assert.Equal(t, "v1", store.vchannels[0])
	assert.Equal(t, uint64(0), store.chunks[0].GetChunkId())
	require.Len(t, store.chunks[0].GetEntries(), 1)
	entry := store.chunks[0].GetEntries()[0]
	assert.Equal(t, uint64(50), entry.GetTimeTick())
	require.NotNil(t, entry.GetDelete())
	require.Len(t, entry.GetDelete().GetBlocks(), 1)
	assert.True(t, proto.Equal(
		&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		entry.GetDelete().GetBlocks()[0].GetPrimaryKeys(),
	))

	transformMeta := manager.transformLog("v1").log.SnapshotMeta()
	assert.Equal(t, uint64(50), transformMeta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(0), transformMeta.GetFirstChunkId())
	assert.Equal(t, uint64(1), transformMeta.GetNextChunkId())
}

func TestGrowingManagerTxnDeleteStoredAsSingleTransformLogEntry(t *testing.T) {
	store := &recordingTransformLogStore{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogStore(store),
		WithTransformLogBufferMaxRows(100),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {CheckpointTimeTick: 10},
		}),
	)
	manager.metaAndData = true
	vchannel := manager.vChannels()["v1"]
	vchannel.metaAndData = true
	txn := newTestTxnDeleteMessage(t, 50, []int64{1, 2})

	result := manager.observeTxnMessage(context.Background(), txn)
	require.NotNil(t, result.Data)

	task := manager.startFlushTransformLogBufferTask("v1", 50)
	require.NotNil(t, task)
	require.NoError(t, task.Run(context.Background()))

	require.Len(t, store.chunks, 1)
	require.Len(t, store.chunks[0].GetEntries(), 1)
	entry := store.chunks[0].GetEntries()[0]
	assert.Equal(t, uint64(50), entry.GetTimeTick())
	require.NotNil(t, entry.GetDelete())
	require.Len(t, entry.GetDelete().GetBlocks(), 2)
	assert.Equal(t, []int64{1}, entry.GetDelete().GetBlocks()[0].GetPrimaryKeys().GetIntId().GetData())
	assert.Equal(t, []int64{2}, entry.GetDelete().GetBlocks()[1].GetPrimaryKeys().GetIntId().GetData())
}

func TestObjectTransformLogChunkStoreRoundTrip(t *testing.T) {
	root := t.TempDir()
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	store := waltransformlog.NewObjectChunkStore(chunkManager, "p1")
	chunk := &streamingpb.TransformLogChunk{
		ChunkId: 3,
		Entries: []*streamingpb.TransformLogEntry{
			{
				TimeTick: 50,
				Entry: &streamingpb.TransformLogEntry_Delete{
					Delete: &streamingpb.TransformDeleteEntry{
						Blocks: []*streamingpb.TransformDeleteBlock{
							{
								PartitionId: 10,
								PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
							},
						},
					},
				},
			},
		},
	}

	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", chunk))
	loaded, err := store.ReadTransformLogChunk(context.Background(), "v1", 3)
	require.NoError(t, err)

	assert.True(t, proto.Equal(chunk, loaded))
	_, err = chunkManager.Read(context.Background(), path.Join(root, "transform-log/p1/v1/chunks/3.pb"))
	assert.NoError(t, err)
}

func TestGrowingManagerRecoverTransformLogReadsRetainedChunks(t *testing.T) {
	root := t.TempDir()
	store := waltransformlog.NewObjectChunkStore(storage.NewLocalChunkManager(objectstorage.RootPath(root)), "p1")
	chunk := &streamingpb.TransformLogChunk{
		ChunkId: 3,
		Entries: []*streamingpb.TransformLogEntry{
			{
				TimeTick: 50,
				Entry: &streamingpb.TransformLogEntry_Delete{
					Delete: &streamingpb.TransformDeleteEntry{
						Blocks: []*streamingpb.TransformDeleteBlock{
							{
								PartitionId: 10,
								PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", chunk))
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogStore(store),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {
				CheckpointTimeTick: 50,
				FirstChunkId:       3,
				NextChunkId:        4,
			},
		}),
	)

	require.NoError(t, manager.RecoverTransformLogs(context.Background()))

	scanner := manager.Read(context.Background(), transformlog.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()
	entryEvent := <-scanner.Chan()
	require.NotNil(t, entryEvent.Entry)
	assert.True(t, proto.Equal(chunk.GetEntries()[0], entryEvent.Entry))
}

func TestGrowingManagerReadTransformLogReplaysRetainedEntriesAndCaughtUp(t *testing.T) {
	root := t.TempDir()
	store := waltransformlog.NewObjectChunkStore(storage.NewLocalChunkManager(objectstorage.RootPath(root)), "p1")
	chunk := &streamingpb.TransformLogChunk{
		ChunkId: 3,
		Entries: []*streamingpb.TransformLogEntry{
			{
				TimeTick: 50,
				Entry: &streamingpb.TransformLogEntry_Delete{
					Delete: &streamingpb.TransformDeleteEntry{
						Blocks: []*streamingpb.TransformDeleteBlock{
							{
								PartitionId: 10,
								PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", chunk))
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogStore(store),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {
				CheckpointTimeTick: 50,
				FirstChunkId:       3,
				NextChunkId:        4,
			},
		}),
	)
	require.NoError(t, manager.RecoverTransformLogs(context.Background()))

	scanner := manager.Read(context.Background(), transformlog.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()

	entryEvent := <-scanner.Chan()
	require.NotNil(t, entryEvent.Entry)
	assert.Equal(t, uint64(50), entryEvent.Entry.GetTimeTick())

	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)
	assert.Equal(t, uint64(10), caughtUpEvent.CaughtUp.StartAfterTimeTick)
	assert.NoError(t, scanner.Error())
}

func TestGrowingManagerReadTransformLogForwardsLiveEntriesAfterCaughtUp(t *testing.T) {
	store := &recordingTransformLogStore{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil,
		WithTransformLogStore(store),
		WithTransformLogBufferMaxRows(100),
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {CheckpointTimeTick: 10},
		}),
	)
	manager.metaAndData = true
	vchannel := manager.vChannels()["v1"]
	vchannel.metaAndData = true

	scanner := manager.Read(context.Background(), transformlog.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()

	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)

	msg := newTestDeleteMessage(t, 50)
	result := manager.observeTransformLogMessage(vchannel, msg)
	require.NotNil(t, result.Data)
	task := manager.startFlushTransformLogBufferTask("v1", 50)
	require.NotNil(t, task)
	require.NoError(t, task.Run(context.Background()))

	entryEvent := <-scanner.Chan()
	require.NotNil(t, entryEvent.Entry)
	assert.Equal(t, uint64(50), entryEvent.Entry.GetTimeTick())
	assert.NoError(t, scanner.Error())
}

func TestVChannelViewObserveCreatePartitionUsesMetaWatermark(t *testing.T) {
	vchannel := newVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
		},
		0,
		false,
		runtimeConfig{},
	)
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  20,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(50).
		WithLastConfirmed(walimplstest.NewTestMessageID(50)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(51))

	result := vchannel.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(msg))

	assert.Nil(t, result.Meta)
	assert.False(t, hasPartitionMeta(vchannel.AssignmentMeta(), 20))
}

type recordingTransformLogStore struct {
	vchannels []string
	chunks    []*streamingpb.TransformLogChunk
}

func (w *recordingTransformLogStore) WriteTransformLogChunk(_ context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	w.vchannels = append(w.vchannels, vchannel)
	w.chunks = append(w.chunks, proto.Clone(chunk).(*streamingpb.TransformLogChunk))
	return nil
}

func (w *recordingTransformLogStore) ReadTransformLogChunk(_ context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	for idx, candidate := range w.chunks {
		if w.vchannels[idx] == vchannel && candidate.GetChunkId() == chunkID {
			return proto.Clone(candidate).(*streamingpb.TransformLogChunk), nil
		}
	}
	return nil, errors.Errorf("chunk %s/%d not found", vchannel, chunkID)
}

func TestGrowingManagerDataCheckpointTimeTickUsesMinimumTransformLogAndSegmentCheckpoint(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		100: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 60,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		},
	}, nil,
		WithTransformLogMetas(map[string]*streamingpb.VChannelTransformLogMeta{
			"v1": {CheckpointTimeTick: 40},
		}),
	)

	assert.Equal(t, uint64(40), manager.DataCheckpointTimeTick())
}

func TestGrowingManagerDataCheckpointTimeTickIgnoresIdleTransformLog(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 1},
				},
			},
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		100: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     100,
			DataCheckpointTimeTick: 60,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		},
	}, nil)

	assert.Equal(t, uint64(60), manager.DataCheckpointTimeTick())
}

type neverFlushPolicy struct{}

func (neverFlushPolicy) ShouldFlush(writeOnlyInsertBuffer, uint64) bool {
	return false
}

func newTestInsertMessage(t *testing.T, timetick uint64, assignment *messagespb.PartitionSegmentAssignment) message.ImmutableInsertMessageV1 {
	t.Helper()
	mutableMsg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*messagespb.PartitionSegmentAssignment{assignment},
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableInsertMessageV1(msg)
}

func newTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableDeleteMessageV1 {
	t.Helper()
	mutableMsg := newTestDeleteMutableMessage(t, 1, timetick)
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func newTestDeleteMutableMessage(t *testing.T, pk int64, timetick uint64) message.MutableMessage {
	t.Helper()
	return message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
}

func newTestTxnDeleteMessage(t *testing.T, timetick uint64, pks []int64) message.ImmutableTxnMessage {
	t.Helper()
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: time.Second}
	beginMutable := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	begin := beginMutable.WithTxnContext(txnCtx).
		WithTimeTick(timetick - 2).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 2))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick - 2)))
	beginMsg := message.MustAsImmutableBeginTxnMessageV2(begin)

	builder := message.NewImmutableTxnMessageBuilder(beginMsg)
	for idx, pk := range pks {
		bodyTimeTick := timetick - 1
		immutableDelete := newTestDeleteMutableMessage(t, pk, bodyTimeTick).
			WithTxnContext(txnCtx).
			WithTimeTick(bodyTimeTick).
			WithLastConfirmed(walimplstest.NewTestMessageID(int64(bodyTimeTick))).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(bodyTimeTick) + int64(idx) + 1))
		builder.Add(immutableDelete)
	}

	commitMutable := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()
	commit := commitMutable.WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	commitMsg := message.MustAsImmutableCommitTxnMessageV2(commit)

	txn, err := builder.Build(commitMsg)
	require.NoError(t, err)
	return txn
}

func hasPartitionMeta(meta *streamingpb.VChannelMeta, partitionID int64) bool {
	for _, partition := range meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return true
		}
	}
	return false
}
