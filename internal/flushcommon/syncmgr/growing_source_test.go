package syncmgr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

func (s *fakeCommitGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	return []int64{0, 1, 100, 101, 102}, nil
}

func (s *fakeCommitGrowingFlushSource) CurrentOffset() int64 {
	return 10
}

func (s *fakeCommitGrowingFlushSource) FlushGrowingData(_ context.Context, _, _ int64, config *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return &GrowingFlushResult{
		ManifestPath:  "manifest",
		NumRows:       10,
		TimestampFrom: 101,
		TimestampTo:   200,
		ColumnGroupMemorySizes: fakeColumnGroupMemorySizes(config, map[int64]int64{
			0:   80,
			101: 120,
			102: 160,
		}),
		FieldNullCounts: map[int64]int64{
			101: 2,
		},
	}, nil
}

func (s *fakeCommitGrowingFlushSource) Release() {
}

func (s *fakeCommitGrowingFlushSource) CommitGrowingFlush(targetOffset int64) {
	s.commits = append(s.commits, targetOffset)
}

func TestGrowingSourceSyncTaskHandleErrorSkipsFailureCallbackForStaleMetaErrors(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	testCases := []struct {
		name string
		err  error
	}{
		{
			name: "channel_not_found",
			err:  merr.WrapErrChannelNotFound("ch"),
		},
		{
			name: "segment_not_found",
			err:  merr.WrapErrSegmentNotFound(1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.DataNodeFlushBufferCount.Reset()
			callbackCalled := false
			task := NewGrowingSourceSyncTask().
				WithFailureCallback(func(error) {
					callbackCalled = true
					panic("failure callback should not be called")
				})

			require.NotPanics(t, func() {
				task.HandleError(tc.err)
				task.HandleError(tc.err)
			})
			require.False(t, callbackCalled)
			require.Zero(t, testutil.ToFloat64(metrics.DataNodeFlushBufferCount.WithLabelValues(
				paramtable.GetStringNodeID(),
				metrics.FailLabel,
				task.level.String(),
			)))
		})
	}
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
		ID:             segmentID,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"ver":7,"base_path":"/root/insert_log/3/2/1"}`,
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
		ID:             1,
		PartitionID:    2,
		StorageVersion: storage.StorageV3,
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
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, StorageVersion: storage.StorageV3}, pkoracle.NewBloomFilterSet(), nil)
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
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, StorageVersion: storage.StorageV3}, pkoracle.NewBloomFilterSet(), nil)
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
			require.Equal(t, []int64{100}, task.insertBinlogs[0].GetChildFields())
			require.Len(t, task.insertBinlogs[0].GetBinlogs(), 1)
			require.EqualValues(t, 10, task.insertBinlogs[0].GetBinlogs()[0].GetEntriesNum())
			require.EqualValues(t, 101, task.insertBinlogs[0].GetBinlogs()[0].GetTimestampFrom())
			require.EqualValues(t, 200, task.insertBinlogs[0].GetBinlogs()[0].GetTimestampTo())
			require.EqualValues(t, 80, task.insertBinlogs[0].GetBinlogs()[0].GetMemorySize())
			require.EqualValues(t, 500, task.insertBinlogs[0].GetBinlogs()[0].GetLogID())
			require.EqualValues(t, 2, task.insertBinlogs[101].GetBinlogs()[0].GetFieldNullCounts()[101])
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
		WithAllocator(&fakeAllocator{next: 500}).
		WithSource(&fakeCommitGrowingFlushSource{})

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, segment.GetCurrentSplit(), 3)
	require.Equal(t, []int64{100}, segment.GetCurrentSplit()[0].Fields)
}

func TestBuildGrowingSourceInsertBinlogsRequiresColumnGroupMemorySize(t *testing.T) {
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 10, Fields: []int64{100}},
	}
	result := &GrowingFlushResult{
		NumRows:                1,
		ColumnGroupMemorySizes: map[int64]int64{},
	}

	_, err := buildGrowingSourceInsertBinlogs(columnGroups, result, []int64{1})
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrDataIntegrity), err.Error())
}

func TestGrowingSourceSyncTaskCommitRetainedSourceOnlyOnFinalization(t *testing.T) {
	run := func(t *testing.T, finalize func(*GrowingSourceSyncTask), expectCommit bool) {
		segmentID := int64(1)
		mc := metacache.NewMockMetaCache(t)
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segmentID,
			PartitionID:    2,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
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

func (s *fakeBM25GrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	return []int64{0, 1, 100, 101, 102}, nil
}

func (s *fakeBM25GrowingFlushSource) CurrentOffset() int64 {
	return 10
}

func (s *fakeBM25GrowingFlushSource) FlushGrowingData(_ context.Context, _, _ int64, config *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return &GrowingFlushResult{
		ManifestPath:           "manifest-after-flush",
		NumRows:                10,
		ColumnGroupMemorySizes: fakeColumnGroupMemorySizes(config, nil),
		BM25Stats:              s.stats,
	}, nil
}

func fakeColumnGroupMemorySizes(config *GrowingFlushConfig, sizes map[int64]int64) map[int64]int64 {
	if config == nil || len(config.ColumnGroups) == 0 {
		return sizes
	}
	result := make(map[int64]int64, len(config.ColumnGroups))
	for _, columnGroup := range config.ColumnGroups {
		if size, ok := sizes[columnGroup.GroupID]; ok {
			result[columnGroup.GroupID] = size
			continue
		}
		result[columnGroup.GroupID] = 80
	}
	return result
}

func (s *fakeBM25GrowingFlushSource) Release() {
}

func TestGrowingSourceSyncTaskMergesReturnedBM25Stats(t *testing.T) {
	segmentID := int64(1)
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{10: 2})

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

type fakeMaterializedGrowingFlushSource struct {
	materialized []int64
}

func (s *fakeMaterializedGrowingFlushSource) CurrentOffset() int64 { return 0 }

func (s *fakeMaterializedGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	return s.materialized, nil
}

func (s *fakeMaterializedGrowingFlushSource) FlushGrowingData(ctx context.Context, startOffset, endOffset int64, config *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return nil, nil
}

func (s *fakeMaterializedGrowingFlushSource) Release() {}

func TestTrimColumnGroupsToMaterialized(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "fn_sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
	}
	groups := []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{0, 1, 100, 101, 102}, Columns: []int{0, 1, 2, 3, 4}},
		{GroupID: 1, Fields: []int64{102}, Columns: []int{4}},
	}

	// A non-materialized column (function output here) is dropped; a group
	// left empty vanishes; the parallel Columns array is trimmed in lockstep
	// so the writer pattern never names the dropped column.
	task := NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeMaterializedGrowingFlushSource{materialized: []int64{0, 1, 100, 101}})
	trimmed, err := task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 1)
	require.Equal(t, []int64{0, 1, 100, 101}, trimmed[0].Fields)
	require.Equal(t, []int{0, 1, 2, 3}, trimmed[0].Columns)

	// A materialized function output is kept.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeMaterializedGrowingFlushSource{materialized: []int64{0, 1, 100, 101, 102}})
	trimmed, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 2)

	// A non-materialized dropped ordinary field is trimmed the same way;
	// system fields stay even though they live outside the insert record.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeMaterializedGrowingFlushSource{materialized: []int64{100}})
	trimmed, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 1)
	require.Equal(t, []int64{0, 1, 100}, trimmed[0].Fields)
	require.Equal(t, []int{0, 1, 2}, trimmed[0].Columns)

	// An empty materialized report has no legal meaning: error out instead
	// of writing a layout that may disagree with the data.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeMaterializedGrowingFlushSource{})
	_, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.Error(t, err)

	// On a committed-flush ack retry the source is gone: the layout is
	// trimmed to the committed binlogs, the persisted truth. The binlog map
	// is keyed by column group id; flushed fields live in ChildFields.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithCommittedFlush("manifest", nil, map[int64]*datapb.FieldBinlog{
			0: {ChildFields: []int64{0, 1, 100, 101}},
		})
	trimmed, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 1)
	require.Equal(t, []int64{0, 1, 100, 101}, trimmed[0].Fields)
	require.Equal(t, []int{0, 1, 2, 3}, trimmed[0].Columns)
}

func TestBuildGrowingSourceInsertBinlogsTrimsToFlushedFields(t *testing.T) {
	groups := []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{0, 1, 100, 102}},
		{GroupID: 5, Fields: []int64{102}},
	}
	result := &GrowingFlushResult{
		NumRows:                3,
		FlushedFieldIDs:        []int64{0, 1, 100},
		ColumnGroupMemorySizes: map[int64]int64{0: 10, 5: 0},
		FieldNullCounts:        map[int64]int64{0: 0, 1: 0, 100: 0},
	}
	binlogs, err := buildGrowingSourceInsertBinlogs(groups, result, []int64{7, 8})
	require.NoError(t, err)
	require.Len(t, binlogs, 1)
	require.Equal(t, []int64{0, 1, 100}, binlogs[0].GetChildFields())
}
