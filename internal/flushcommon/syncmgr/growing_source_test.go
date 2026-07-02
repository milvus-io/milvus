package syncmgr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type fakeAllocator struct {
	next allocator.UniqueID
	err  error
}

func (a *fakeAllocator) Alloc(count uint32) (allocator.UniqueID, allocator.UniqueID, error) {
	if a.err != nil {
		return 0, 0, a.err
	}
	begin := a.next
	a.next += allocator.UniqueID(count)
	return begin, a.next, nil
}

func (a *fakeAllocator) AllocOne() (allocator.UniqueID, error) {
	if a.err != nil {
		return 0, a.err
	}
	id := a.next
	a.next++
	return id, nil
}

type fakeCommitGrowingFlushSource struct {
	commits []int64
}

func (s *fakeCommitGrowingFlushSource) CurrentOffset() int64 {
	return 10
}

func (s *fakeCommitGrowingFlushSource) FlushGrowingData(context.Context, int64, int64, *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return &GrowingFlushResult{ManifestPath: "manifest", NumRows: 10}, nil
}

func (s *fakeCommitGrowingFlushSource) Release() {
}

func (s *fakeCommitGrowingFlushSource) CommitGrowingFlush(targetOffset int64) {
	s.commits = append(s.commits, targetOffset)
}

func TestGrowingSourceSyncTaskBuildFlushConfigBM25(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "12345")
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.Key, "67890")
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.Key, "23456")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key)
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.Key)
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.Key)

	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "bm25",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		PartitionID:  2,
		ManifestPath: `{"ver":7,"base_path":"/root/insert_log/3/2/1"}`,
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(&fakeAllocator{next: 500}).
		WithFlush()

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	config, err := task.buildFlushConfig(segment, columnGroups)
	require.NoError(t, err)
	require.Equal(t, schema, config.Schema)
	require.Equal(t, []int64{101}, config.TextFieldIDs)
	require.Equal(t, []string{"/root/insert_log/3/2/lobs/101"}, config.TextLobPaths)
	require.EqualValues(t, 12345, config.TextInlineThreshold)
	require.EqualValues(t, 67890, config.TextMaxLobFileBytes)
	require.EqualValues(t, 23456, config.TextFlushThresholdBytes)
	require.Equal(t, []int64{102}, config.BM25FieldIDs)
	require.Equal(t, []int64{500}, config.BM25StatsLogIDs)
	require.True(t, config.WriteMergedBM25Stats)
	require.EqualValues(t, 7, config.ReadVersion)
}

func TestGrowingSourceSyncTaskBuildFlushConfigStartsFromEarliestManifest(t *testing.T) {
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:          1,
		PartitionID: 2,
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithChunkManager(cm)

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	config, err := task.buildFlushConfig(segment, columnGroups)
	require.NoError(t, err)
	require.EqualValues(t, packed.ManifestEarliest, config.ReadVersion)
}

func TestGrowingSourceSyncTaskBuildFlushConfigBM25AllocatorError(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "bm25",
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1}, pkoracle.NewBloomFilterSet(), nil)
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(&fakeAllocator{err: errors.New("alloc failed")})

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	_, err = task.buildFlushConfig(segment, columnGroups)
	require.ErrorContains(t, err, "alloc failed")
}

func TestGrowingSourceSyncTaskBuildFlushConfigBM25RequiresAllocator(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "bm25",
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1}, pkoracle.NewBloomFilterSet(), nil)
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithSchema(schema).
		WithChunkManager(cm)

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	_, err = task.buildFlushConfig(segment, columnGroups)
	require.ErrorContains(t, err, "id allocator is nil")
}

func TestGrowingSourceSyncTaskBuildFlushConfigUsesCurrentSplitPattern(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "text",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"ver":7,"base_path":"/root/insert_log/3/2/1"}`,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100}, Format: "parquet"},
			{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
			{FieldID: 102, ChildFields: []int64{102}, Format: "parquet"},
		},
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm)

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	config, err := task.buildFlushConfig(segment, columnGroups)
	require.NoError(t, err)
	require.Equal(t, "100,101,102", config.SchemaBasedPattern)
	require.Equal(t, "parquet,vortex,parquet", config.SchemaBasedFormats)
	require.Equal(t, []int64{100, 101, 102}, config.AllowedFieldIDs)
	require.Equal(t, columnGroups, segment.GetCurrentSplit())
	require.NotEmpty(t, config.WriterFormat)
}

func TestGrowingSourceSyncTaskBuildFlushConfigRequiresCurrentSplitFormatForExistingManifest(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "text",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("/root/insert_log/3/2/1", 7),
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100}},
			{FieldID: 101, ChildFields: []int64{101}, Format: "parquet"},
		},
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm)

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	_, err = task.buildFlushConfig(segment, columnGroups)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "missing format")
}

func TestGrowingSourceSyncTaskBuildFlushConfigAllowsMissingFormatForEarliestManifest(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "text",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("/root/insert_log/3/2/1", packed.ManifestEarliest),
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100}},
			{FieldID: 101, ChildFields: []int64{101}},
		},
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm)

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	config, err := task.buildFlushConfig(segment, columnGroups)
	require.NoError(t, err)
	require.EqualValues(t, packed.ManifestEarliest, config.ReadVersion)
	require.NotEmpty(t, config.SchemaBasedFormats)
}

func TestGrowingSourceSyncTaskBuildFlushConfigProjectsToCurrentSplit(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "text",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			{FieldID: 103, Name: "added_scalar", DataType: schemapb.DataType_Int64, Nullable: true},
			{FieldID: 104, Name: "added_text", DataType: schemapb.DataType_Text, Nullable: true},
			{FieldID: 105, Name: "added_sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{105}},
		},
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"ver":7,"base_path":"/root/insert_log/3/2/1"}`,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, ChildFields: []int64{100}, Format: "parquet"},
			{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
			{FieldID: 102, ChildFields: []int64{102}, Format: "parquet"},
		},
	}, pkoracle.NewBloomFilterSet(), nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(&fakeAllocator{next: 500}).
		WithFlush()

	columnGroups, err := task.getColumnGroups(segment)
	require.NoError(t, err)
	config, err := task.buildFlushConfig(segment, columnGroups)
	require.NoError(t, err)
	require.Equal(t, schema, config.Schema)
	require.Equal(t, []int64{100, 101, 102}, config.AllowedFieldIDs)
	require.Equal(t, []int64{101}, config.TextFieldIDs)
	require.Equal(t, []string{"/root/insert_log/3/2/lobs/101"}, config.TextLobPaths)
	require.Empty(t, config.BM25FieldIDs)
	require.Empty(t, config.BM25StatsLogIDs)
	require.True(t, config.WriteMergedBM25Stats)
}

func TestGrowingSourceSyncTaskBuildsColumnGroupBinlogs(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "text",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}
	mc := metacache.NewMockMetaCache(t)
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		State:          commonpb.SegmentState_Growing,
		StorageVersion: storage.StorageV3,
	}, pkoracle.NewBloomFilterSet(), nil)
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(segment)
	}).Return()

	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	metaWriter := NewMockMetaWriter(t)
	metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task *GrowingSourceSyncTask) error {
			require.Len(t, task.insertBinlogs, 3)
			require.Empty(t, task.insertBinlogs[0].GetBinlogs())
			require.Equal(t, []int64{100}, task.insertBinlogs[0].GetChildFields())
			return nil
		})

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithTargetOffset(10).
		WithMetaCache(mc).
		WithMetaWriter(metaWriter).
		WithSchema(schema).
		WithChunkManager(cm).
		WithSource(&fakeCommitGrowingFlushSource{})

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, segment.GetCurrentSplit(), 3)
	require.Equal(t, []int64{100}, segment.GetCurrentSplit()[0].Fields)
}

func TestGrowingSourceSyncTaskCommitRetainedSourceOnlyOnFinalization(t *testing.T) {
	run := func(t *testing.T, finalize func(*GrowingSourceSyncTask), expectCommit bool) {
		segmentID := int64(1)
		mc := metacache.NewMockMetaCache(t)
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID:           segmentID,
			PartitionID:  2,
			ManifestPath: "manifest",
		}, pkoracle.NewBloomFilterSet(), nil)
		metacache.UpdateNumOfRows(10)(segment)
		source := &fakeCommitGrowingFlushSource{}

		mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)
		mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
			action(segment)
		}).Return()
		mc.EXPECT().RemoveSegments(mock.Anything).Return(nil).Maybe()

		task := NewGrowingSourceSyncTask().
			WithCollectionID(3).
			WithPartitionID(2).
			WithSegmentID(segmentID).
			WithChannelName("ch").
			WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
			WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
			WithBatchRows(0).
			WithTargetOffset(10).
			WithMetaCache(mc).
			WithSource(source)
		if finalize != nil {
			finalize(task)
		}

		require.NoError(t, task.Run(context.Background()))
		if expectCommit {
			require.Equal(t, []int64{10}, source.commits)
		} else {
			require.Empty(t, source.commits)
		}
	}

	t.Run("non_final_sync_does_not_commit_retained_source", func(t *testing.T) {
		run(t, nil, false)
	})

	t.Run("flush_finalization_commits_retained_source", func(t *testing.T) {
		run(t, func(task *GrowingSourceSyncTask) {
			task.WithFlush()
		}, true)
	})

	t.Run("drop_finalization_commits_retained_source", func(t *testing.T) {
		run(t, func(task *GrowingSourceSyncTask) {
			task.WithDrop()
		}, true)
	})
}

type fakeBM25GrowingFlushSource struct {
	stats map[int64]*storage.BM25Stats
}

func (s *fakeBM25GrowingFlushSource) CurrentOffset() int64 {
	return 10
}

func (s *fakeBM25GrowingFlushSource) FlushGrowingData(context.Context, int64, int64, *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return &GrowingFlushResult{
		ManifestPath: "manifest-after-flush",
		NumRows:      10,
		BM25Stats:    s.stats,
	}, nil
}

func (s *fakeBM25GrowingFlushSource) Release() {
}

func TestGrowingSourceSyncTaskMergesReturnedBM25Stats(t *testing.T) {
	segmentID := int64(1)
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{10: 2})

	mc := metacache.NewMockMetaCache(t)
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:          segmentID,
		PartitionID: 2,
		State:       commonpb.SegmentState_Growing,
	}, pkoracle.NewBloomFilterSet(), nil)

	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(segment)
	}).Return()
	mc.EXPECT().RemoveSegments(mock.Anything).Return(nil).Maybe()
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithTargetOffset(10).
		WithMetaCache(mc).
		WithChunkManager(cm).
		WithSource(&fakeBM25GrowingFlushSource{stats: map[int64]*storage.BM25Stats{102: stats}})

	require.NoError(t, task.Run(context.Background()))
	serialized, _, err := segment.GetBM25Stats().Serialize()
	require.NoError(t, err)
	require.Contains(t, serialized, int64(102))
	restored, err := storage.NewBM25StatsWithBytes(serialized[102])
	require.NoError(t, err)
	require.EqualValues(t, 1, restored.NumRow())
	require.EqualValues(t, 2, restored.NumToken())
}
