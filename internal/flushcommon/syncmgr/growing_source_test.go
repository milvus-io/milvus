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

func TestGrowingSourceSyncTaskAutoFlushFailureMetric(t *testing.T) {
	metrics.DataNodeFlushBufferCount.Reset()
	metrics.DataNodeAutoFlushBufferCount.Reset()
	t.Cleanup(func() {
		metrics.DataNodeFlushBufferCount.Reset()
		metrics.DataNodeAutoFlushBufferCount.Reset()
	})

	task := NewGrowingSourceSyncTask()
	task.HandleError(errors.New("storage unavailable"))
	nodeID := paramtable.GetStringNodeID()
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataNodeFlushBufferCount.WithLabelValues(
		nodeID, metrics.FailLabel, task.level.String())))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataNodeAutoFlushBufferCount.WithLabelValues(
		nodeID, metrics.FailLabel, task.level.String())))

	NewGrowingSourceSyncTask().WithFlush().HandleError(errors.New("metadata unavailable"))
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.DataNodeFlushBufferCount.WithLabelValues(
		nodeID, metrics.FailLabel, task.level.String())))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataNodeAutoFlushBufferCount.WithLabelValues(
		nodeID, metrics.FailLabel, task.level.String())))
}

// growingSegment builds the standard storage-v3 growing-segment fixture these
// tests share (partition 2, fresh bloom-filter set); mutate customizes the
// proto before the metacache wrapper is built.
func growingSegment(id int64, mutate ...func(*datapb.SegmentInfo)) *metacache.SegmentInfo {
	info := &datapb.SegmentInfo{ID: id, PartitionID: 2, StorageVersion: storage.StorageV3}
	for _, m := range mutate {
		m(info)
	}
	return metacache.NewSegmentInfo(info, pkoracle.NewBloomFilterSet(), nil, nil)
}

func withState(state commonpb.SegmentState) func(*datapb.SegmentInfo) {
	return func(info *datapb.SegmentInfo) { info.State = state }
}

func withManifest(path string) func(*datapb.SegmentInfo) {
	return func(info *datapb.SegmentInfo) { info.ManifestPath = path }
}

func pkStatsAsOracle(stats *storage.PrimaryKeyStats) *storage.PkStatistics {
	return &storage.PkStatistics{
		PkFilter: stats.BF,
		MaxPK:    stats.MaxPk,
		MinPK:    stats.MinPk,
	}
}

func requirePKStatsHit(t *testing.T, stats []*storage.PrimaryKeyStats, pk storage.PrimaryKey) {
	t.Helper()
	for _, stat := range stats {
		if pkStatsAsOracle(stat).PkExist(pk) {
			return
		}
	}
	require.Failf(t, "primary key missing from stats", "pk=%v", pk)
}

func requirePKStatsMiss(t *testing.T, stats []*storage.PrimaryKeyStats, pk storage.PrimaryKey) {
	t.Helper()
	for _, stat := range stats {
		require.False(t, pkStatsAsOracle(stat).PkExist(pk), "pk=%v", pk)
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
	segment := growingSegment(segmentID, withManifest(`{"ver":7,"base_path":"/root/insert_log/3/2/1"}`))

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(500, nil)).
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
	segment := growingSegment(1)

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
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, StorageVersion: storage.StorageV3}, pkoracle.NewBloomFilterSet(), nil, nil)
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(1).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(0, errors.New("alloc failed")))

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
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, StorageVersion: storage.StorageV3}, pkoracle.NewBloomFilterSet(), nil, nil)
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
	}, pkoracle.NewBloomFilterSet(), nil, nil)

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
	require.Equal(t, []int{0}, columnGroups[0].Columns)
	require.Empty(t, segment.GetCurrentSplit()[0].Columns,
		"resolving a layout must not mutate the shared metacache segment during parallel Prepare")
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
	}, pkoracle.NewBloomFilterSet(), nil, nil)

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
	}, pkoracle.NewBloomFilterSet(), nil, nil)

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
	}, pkoracle.NewBloomFilterSet(), nil, nil)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(500, nil)).
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

func TestGrowingSourceSyncTaskPrepareFailsWhenSegmentMissing(t *testing.T) {
	segmentID := int64(1)

	newTask := func(mc metacache.MetaCache) *GrowingSourceSyncTask {
		return NewGrowingSourceSyncTask().
			WithCollectionID(3).
			WithPartitionID(2).
			WithSegmentID(segmentID).
			WithChannelName("ch").
			WithFlushFromTs(0).
			WithMetaCache(mc)
	}

	t.Run("missing_segment_fails", func(t *testing.T) {
		mc := metacache.NewMockMetaCache(t)
		mc.EXPECT().GetSegmentByID(segmentID).Return(nil, false)
		// Skipping instead of failing would let finishGrowingSourceSync ack
		// the flushed range and advance the channel checkpoint for rows that were
		// never materialized.
		task := newTask(mc)

		err := task.Prepare(context.Background())

		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrSegmentNotFound)
	})

	t.Run("missing_segment_on_drop_fails_too", func(t *testing.T) {
		// progress.syncing keeps at most one growing-source task in flight per
		// segment, so a drop task finding its segment gone is the same invariant
		// violation as any other task — no silent skip.
		mc := metacache.NewMockMetaCache(t)
		mc.EXPECT().GetSegmentByID(segmentID).Return(nil, false)
		task := newTask(mc).WithDrop()

		err := task.Prepare(context.Background())

		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrSegmentNotFound)
	})
}

func TestGrowingSourceSyncTaskBuildsColumnGroupBinlogs(t *testing.T) {
	metrics.DataNodeWriteDataCount.Reset()
	metrics.DataNodeFlushedSize.Reset()
	metrics.DataNodeFlushedRows.Reset()
	metrics.DataNodeSave2StorageLatency.Reset()
	metrics.DataNodeAutoFlushBufferCount.Reset()
	metrics.DataNodeFlushBufferCount.Reset()
	t.Cleanup(func() {
		metrics.DataNodeWriteDataCount.Reset()
		metrics.DataNodeFlushedSize.Reset()
		metrics.DataNodeFlushedRows.Reset()
		metrics.DataNodeSave2StorageLatency.Reset()
		metrics.DataNodeAutoFlushBufferCount.Reset()
		metrics.DataNodeFlushBufferCount.Reset()
	})

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
	segment := growingSegment(segmentID, withState(commonpb.SegmentState_Growing))
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Twice()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(segment)
	}).Return()

	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	source := &fakeGrowingFlushSource{
		checkConfig: func(config *GrowingFlushConfig) {
			require.EqualValues(t, 100, config.PKStatsFieldID)
			require.EqualValues(t, 503, config.PKStatsLogID)
			require.NotEmpty(t, config.PKStatsBlob)
		},
	}
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
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithMetaWriter(metaWriter).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(500, nil)).
		WithSource(source)

	require.NoError(t, runTaskForTest(context.Background(), task))
	require.Len(t, segment.GetCurrentSplit(), 3)
	require.Equal(t, []int64{100}, segment.GetCurrentSplit()[0].Fields)
	require.Len(t, segment.GetHistory(), 1)
	require.EqualValues(t, 360, task.flushedSize)

	nodeID := paramtable.GetStringNodeID()
	require.Equal(t, float64(10), testutil.ToFloat64(metrics.DataNodeWriteDataCount.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, metrics.InsertLabel, "3")))
	require.Equal(t, float64(360), testutil.ToFloat64(metrics.DataNodeFlushedSize.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, task.level.String())))
	require.Equal(t, float64(10), testutil.ToFloat64(metrics.DataNodeFlushedRows.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataNodeAutoFlushBufferCount.WithLabelValues(
		nodeID, metrics.SuccessLabel, task.level.String())))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.DataNodeFlushBufferCount.WithLabelValues(
		nodeID, metrics.SuccessLabel, task.level.String())))

	// Replaying a manifest after a lost metadata acknowledgement must not count
	// the same physical object write a second time.
	replay := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithSchema(schema).
		WithCommittedFlush(task.CommittedManifestPath(), task.CommittedBM25Stats(), task.CommittedInsertBinlogs()).
		WithCommittedPKStats(task.CommittedPKStats())
	require.NoError(t, replay.Prepare(context.Background()))
	require.Equal(t, float64(10), testutil.ToFloat64(metrics.DataNodeWriteDataCount.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, metrics.InsertLabel, "3")))
	require.Equal(t, float64(360), testutil.ToFloat64(metrics.DataNodeFlushedSize.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, task.level.String())))
	require.Equal(t, float64(10), testutil.ToFloat64(metrics.DataNodeFlushedRows.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel)))
}

func TestGrowingSourceSyncTaskBuildsMergedPKStatsForFlush(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "pk",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}
	oldStats, err := storage.NewPrimaryKeyStats(100, int64(schemapb.DataType_Int64), 2)
	require.NoError(t, err)
	oldStats.Update(storage.NewInt64PrimaryKey(1))
	oldStats.Update(storage.NewInt64PrimaryKey(2))

	bfs := pkoracle.NewBloomFilterSet()
	bfs.Roll(oldStats)
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		PartitionID:    2,
		State:          commonpb.SegmentState_Flushing,
		StorageVersion: storage.StorageV3,
		NumOfRows:      10,
	}, bfs, nil, nil)
	mc := metacache.NewMockMetaCache(t)
	mc.EXPECT().Collection().Return(int64(3)).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Maybe()

	config := &GrowingFlushConfig{}
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithSegmentID(segmentID).
		WithFlushFromTs(0).
		WithLevel(datapb.SegmentLevel_L1).
		WithFlush().
		WithMetaCache(mc).
		WithSchema(schema).
		WithAllocator(newSequentialIDAllocator(500, nil)).
		WithSource(&fakeGrowingFlushSource{
			primaryKeys: []storage.PrimaryKey{
				storage.NewInt64PrimaryKey(10),
				storage.NewInt64PrimaryKey(11),
				storage.NewInt64PrimaryKey(12),
				storage.NewInt64PrimaryKey(13),
				storage.NewInt64PrimaryKey(14),
				storage.NewInt64PrimaryKey(15),
				storage.NewInt64PrimaryKey(16),
				storage.NewInt64PrimaryKey(17),
				storage.NewInt64PrimaryKey(18),
				storage.NewInt64PrimaryKey(19),
			},
		})

	require.NoError(t, task.fillPrimaryKeyStatsConfig(context.Background(), 0, 10, 10, config))
	require.EqualValues(t, 100, config.PKStatsFieldID)
	require.EqualValues(t, 500, config.PKStatsLogID)
	require.NotEmpty(t, config.PKStatsBlob)
	require.NotEmpty(t, config.MergedPKStatsBlob)
	require.NotNil(t, task.singlePKStats)

	singleStats, err := storage.DeserializeStats([]*storage.Blob{{Value: config.PKStatsBlob}})
	require.NoError(t, err)
	require.Len(t, singleStats, 1)
	require.EqualValues(t, schemapb.DataType_Int64, singleStats[0].PkType)
	requirePKStatsHit(t, singleStats, storage.NewInt64PrimaryKey(10))
	requirePKStatsHit(t, singleStats, storage.NewInt64PrimaryKey(19))
	requirePKStatsMiss(t, singleStats, storage.NewInt64PrimaryKey(9))
	requirePKStatsMiss(t, singleStats, storage.NewInt64PrimaryKey(20))

	mergedStats, err := storage.DeserializeStatsList(&storage.Blob{Value: config.MergedPKStatsBlob})
	require.NoError(t, err)
	require.Len(t, mergedStats, 2)
	requirePKStatsHit(t, mergedStats, storage.NewInt64PrimaryKey(1))
	requirePKStatsHit(t, mergedStats, storage.NewInt64PrimaryKey(2))
	requirePKStatsHit(t, mergedStats, storage.NewInt64PrimaryKey(10))
	requirePKStatsHit(t, mergedStats, storage.NewInt64PrimaryKey(19))
	requirePKStatsMiss(t, mergedStats, storage.NewInt64PrimaryKey(0))
	requirePKStatsMiss(t, mergedStats, storage.NewInt64PrimaryKey(20))
}

func TestGrowingSourceSyncTaskBuildsVarCharPKStatsContent(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "varchar_pk",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}
	config := &GrowingFlushConfig{}
	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithSegmentID(segmentID).
		WithFlushFromTs(0).
		WithSchema(schema).
		WithAllocator(newSequentialIDAllocator(500, nil)).
		WithSource(&fakeGrowingFlushSource{
			primaryKeys: []storage.PrimaryKey{
				storage.NewVarCharPrimaryKey("alice"),
				storage.NewVarCharPrimaryKey("bob"),
				storage.NewVarCharPrimaryKey("carol"),
			},
		})

	require.NoError(t, task.fillPrimaryKeyStatsConfig(context.Background(), 0, 3, int64(3-0), config))
	require.EqualValues(t, 100, config.PKStatsFieldID)
	require.EqualValues(t, 500, config.PKStatsLogID)
	require.NotEmpty(t, config.PKStatsBlob)
	require.Empty(t, config.MergedPKStatsBlob)

	stats, err := storage.DeserializeStats([]*storage.Blob{{Value: config.PKStatsBlob}})
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.EqualValues(t, schemapb.DataType_VarChar, stats[0].PkType)
	requirePKStatsHit(t, stats, storage.NewVarCharPrimaryKey("alice"))
	requirePKStatsHit(t, stats, storage.NewVarCharPrimaryKey("bob"))
	requirePKStatsHit(t, stats, storage.NewVarCharPrimaryKey("carol"))
	requirePKStatsMiss(t, stats, storage.NewVarCharPrimaryKey("aaron"))
	requirePKStatsMiss(t, stats, storage.NewVarCharPrimaryKey("dave"))
}

func TestGrowingSourceSyncTaskRequiresPrimaryKeysForGrowingFlush(t *testing.T) {
	segmentID := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "pk",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}
	mc := metacache.NewMockMetaCache(t)
	segment := growingSegment(segmentID, withState(commonpb.SegmentState_Growing))
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)

	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	source := &fakeGrowingFlushSource{
		primaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
	}

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(500, nil)).
		WithSource(source)

	err := runTaskForTest(context.Background(), task)
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrDataIntegrity), err.Error())
	require.ErrorContains(t, err, "primary key count mismatch")
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
		segment := growingSegment(segmentID, withManifest("manifest"))
		metacache.UpdateNumOfRows(10)(segment)
		source := &fakeGrowingFlushSource{}

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
			WithFlushFromTs(0).
			WithMetaCache(mc).
			WithSource(source)
		if finalize != nil {
			finalize(task)
		}

		require.NoError(t, runTaskForTest(context.Background(), task))
		if expectCommit {
			// The source is told to release the rows through the POSITION that
			// was flushed — the same fence the flush used — not a row count.
			require.Equal(t, []uint64{200}, source.commits)
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

// TestGrowingSourceSyncTaskCommitMetaWriteFailureIsBeforePointOfNoReturn proves
// the ordering Commit documents: CommitGrowingFlush is the point of no return
// for the flushed rows, so when publishing the metadata fails, the source must
// NOT be told to release anything — no commit notification, no source release,
// no metacache publication — and the error must surface for the retry round.
func TestGrowingSourceSyncTaskCommitMetaWriteFailureIsBeforePointOfNoReturn(t *testing.T) {
	segmentID := int64(1)
	mc := metacache.NewMockMetaCache(t)
	segment := growingSegment(segmentID, withManifest("manifest"))
	metacache.UpdateNumOfRows(10)(segment)
	source := &fakeGrowingFlushSource{}

	// Only Prepare's lookup is expected. UpdateSegments / RemoveSegments carry
	// no expectations on purpose: publishing ANY metacache state after a failed
	// meta write would fail this test via mockery's unexpected-call check.
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)

	metaErr := errors.New("datacoord unavailable")
	metaWriter := NewMockMetaWriter(t)
	metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.Anything).Return(metaErr)

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(0).
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithMetaWriter(metaWriter).
		WithSource(source).
		WithFlush()

	require.NoError(t, task.Prepare(context.Background()))
	err := task.Commit(context.Background())
	require.ErrorIs(t, err, metaErr)

	require.Empty(t, source.commits,
		"CommitGrowingFlush must not run when the metadata that describes the rows is not durable")
	require.NotNil(t, task.source, "the source must be retained for the retry attempt")
	require.False(t, task.SourceFinalized())
}

// Commit before Prepare has no materialized state to publish; it must refuse
// loudly instead of acking a range that was never written.
func TestGrowingSourceSyncTaskCommitBeforePrepareFails(t *testing.T) {
	source := &fakeGrowingFlushSource{}
	task := NewGrowingSourceSyncTask().
		WithSegmentID(1).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithSource(source).
		WithFlush()

	err := task.Commit(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.ErrorContains(t, err, "commit before prepare")
	require.Empty(t, source.commits)
	require.False(t, task.SourceFinalized())
}

type fakeGrowingSourceProvider struct {
	source GrowingFlushSource
	state  GrowingSourceState
	calls  int
}

func (p *fakeGrowingSourceProvider) GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (GrowingFlushSource, GrowingSourceState) {
	p.calls++
	return p.source, p.state
}

func TestGrowingSourceRegistryRegisterUnregisterTokens(t *testing.T) {
	r := NewGrowingSourceRegistry()

	require.Nil(t, r.Register("ch", nil), "a nil provider must not register")
	require.Zero(t, r.ProviderCount("ch"))
	require.Zero(t, r.LatestRegistrationToken("ch"))

	reg1 := r.Register("ch", &fakeGrowingSourceProvider{state: GrowingSourceUnavailable})
	reg2 := r.Register("ch", &fakeGrowingSourceProvider{state: GrowingSourceUnavailable})
	require.NotNil(t, reg1)
	require.NotNil(t, reg2)
	require.Greater(t, reg2.token, reg1.token, "tokens must be monotonic")
	require.Equal(t, 2, r.ProviderCount("ch"))
	require.Equal(t, reg2.token, r.LatestRegistrationToken("ch"))

	// The counter is global across channels: a later registration anywhere gets
	// a larger token, which is what lets the write buffer detect "a provider
	// registered after my observation".
	other := r.Register("other-ch", &fakeGrowingSourceProvider{})
	require.Greater(t, other.token, reg2.token)
	require.Equal(t, reg2.token, r.LatestRegistrationToken("ch"),
		"another channel's registration must not change this channel's latest token")

	r.Unregister(nil) // no-op
	r.Unregister(reg2)
	require.Equal(t, 1, r.ProviderCount("ch"))
	require.Equal(t, reg1.token, r.LatestRegistrationToken("ch"))
	r.Unregister(reg2) // idempotent
	require.Equal(t, 1, r.ProviderCount("ch"))
	r.Unregister(reg1)
	require.Zero(t, r.ProviderCount("ch"))
	require.Zero(t, r.LatestRegistrationToken("ch"))
	require.Equal(t, 1, r.ProviderCount("other-ch"), "unregister must not leak across channels")
}

func TestGrowingSourceRegistryResolve(t *testing.T) {
	endPos := &msgpb.MsgPosition{Timestamp: 200}

	t.Run("no_providers_unavailable", func(t *testing.T) {
		r := NewGrowingSourceRegistry()
		source, state := r.Resolve("ch", 1, endPos)
		require.Nil(t, source)
		require.Equal(t, GrowingSourceUnavailable, state)
	})

	t.Run("usable_wins_and_pending_source_is_released", func(t *testing.T) {
		r := NewGrowingSourceRegistry()
		pendingSource := &fakeGrowingFlushSource{}
		usableSource := &fakeGrowingFlushSource{}
		r.Register("ch", &fakeGrowingSourceProvider{source: pendingSource, state: GrowingSourcePending})
		r.Register("ch", &fakeGrowingSourceProvider{source: usableSource, state: GrowingSourceUsable})

		source, state := r.Resolve("ch", 1, endPos)
		require.Equal(t, GrowingSourceUsable, state)
		require.Same(t, GrowingFlushSource(usableSource), source)
		require.Equal(t, 1, pendingSource.releases,
			"a pending source that loses to a usable one must be released, not leaked")
		require.Zero(t, usableSource.releases, "the returned source must NOT be released")
	})

	t.Run("first_usable_by_token_order_wins", func(t *testing.T) {
		r := NewGrowingSourceRegistry()
		first := &fakeGrowingFlushSource{}
		firstProvider := &fakeGrowingSourceProvider{source: first, state: GrowingSourceUsable}
		secondProvider := &fakeGrowingSourceProvider{source: &fakeGrowingFlushSource{}, state: GrowingSourceUsable}
		r.Register("ch", firstProvider)
		r.Register("ch", secondProvider)

		source, state := r.Resolve("ch", 1, endPos)
		require.Equal(t, GrowingSourceUsable, state)
		require.Same(t, GrowingFlushSource(first), source)
		require.Equal(t, 1, firstProvider.calls)
		require.Zero(t, secondProvider.calls, "resolution must stop at the first usable provider")
	})

	t.Run("pending_beats_unavailable", func(t *testing.T) {
		r := NewGrowingSourceRegistry()
		r.Register("ch", &fakeGrowingSourceProvider{state: GrowingSourceUnavailable})
		r.Register("ch", &fakeGrowingSourceProvider{state: GrowingSourcePending})
		r.Register("ch", &fakeGrowingSourceProvider{state: GrowingSourceUnavailable})

		source, state := r.Resolve("ch", 1, endPos)
		require.Nil(t, source)
		require.Equal(t, GrowingSourcePending, state)
	})

	t.Run("non_usable_sources_are_released", func(t *testing.T) {
		// Both a pending and an unavailable provider may hand back a real source
		// object (holding a read pin); Resolve must release every one it does
		// not return.
		r := NewGrowingSourceRegistry()
		pendingSource := &fakeGrowingFlushSource{}
		unavailableSource := &fakeGrowingFlushSource{}
		r.Register("ch", &fakeGrowingSourceProvider{source: pendingSource, state: GrowingSourcePending})
		r.Register("ch", &fakeGrowingSourceProvider{source: unavailableSource, state: GrowingSourceUnavailable})

		source, state := r.Resolve("ch", 1, endPos)
		require.Nil(t, source)
		require.Equal(t, GrowingSourcePending, state, "a released pending source still counts as pending")
		require.Equal(t, 1, pendingSource.releases)
		require.Equal(t, 1, unavailableSource.releases)
	})
}

func TestGrowingSourceSyncTaskMergesReturnedBM25Stats(t *testing.T) {
	segmentID := int64(1)
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{10: 2})
	schema := &schemapb.CollectionSchema{
		Name: "bm25",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}

	mc := metacache.NewMockMetaCache(t)
	segment := growingSegment(segmentID, withState(commonpb.SegmentState_Growing))

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
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithSchema(schema).
		WithChunkManager(cm).
		WithAllocator(newSequentialIDAllocator(500, nil)).
		WithSource(&fakeGrowingFlushSource{
			flushFunc: func(_ context.Context, _, _ uint64, config *GrowingFlushConfig) (*GrowingFlushResult, error) {
				return &GrowingFlushResult{
					ManifestPath:           "manifest-after-flush",
					NumRows:                10,
					ColumnGroupMemorySizes: fakeColumnGroupMemorySizes(config, nil),
					BM25Stats:              map[int64]*storage.BM25Stats{102: stats},
				}, nil
			},
		})

	require.NoError(t, runTaskForTest(context.Background(), task))
	serialized, _, err := segment.GetBM25Stats().Serialize()
	require.NoError(t, err)
	require.Contains(t, serialized, int64(102))
	restored, err := storage.NewBM25StatsWithBytes(serialized[102])
	require.NoError(t, err)
	require.EqualValues(t, 1, restored.NumRow())
	require.EqualValues(t, 2, restored.NumToken())
}

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
		WithSource(&fakeGrowingFlushSource{materialized: []int64{0, 1, 100, 101}})
	trimmed, err := task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 1)
	require.Equal(t, []int64{0, 1, 100, 101}, trimmed[0].Fields)
	require.Equal(t, []int{0, 1, 2, 3}, trimmed[0].Columns)

	// A materialized function output is kept.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeGrowingFlushSource{materialized: []int64{0, 1, 100, 101, 102}})
	trimmed, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 2)

	// A non-materialized dropped write-buffer field is trimmed the same way;
	// system fields stay even though they live outside the insert record.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeGrowingFlushSource{materialized: []int64{100}})
	trimmed, err = task.trimColumnGroupsToMaterialized(context.Background(), groups)
	require.NoError(t, err)
	require.Len(t, trimmed, 1)
	require.Equal(t, []int64{0, 1, 100}, trimmed[0].Fields)
	require.Equal(t, []int{0, 1, 2}, trimmed[0].Columns)

	// An empty materialized report has no legal meaning: error out instead
	// of writing a layout that may disagree with the data.
	task = NewGrowingSourceSyncTask().WithSchema(schema).
		WithSource(&fakeGrowingFlushSource{materialized: []int64{}})
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

func TestGrowingSourceFlushedSize(t *testing.T) {
	result := &GrowingFlushResult{
		ColumnGroupMemorySizes: map[int64]int64{
			10: 100,
			20: 30,
		},
	}
	require.EqualValues(t, 130, growingSourceFlushedSizeFromResult(result))

	binlogs := map[int64]*datapb.FieldBinlog{
		10: {
			Binlogs: []*datapb.Binlog{
				{LogSize: 7, MemorySize: 100},
				{MemorySize: 30},
			},
		},
	}
	require.EqualValues(t, 37, growingSourceFlushedSizeFromBinlogs(binlogs))
}

// The TSafe gate is the last thing between a published checkpoint and rows that
// exist nowhere. The source bounds its own range by what it has finished
// writing, so a source that has not consumed the fence would quietly flush LESS
// than the fence names while the caller publishes that fence as the channel
// checkpoint — with the WAL already advanced past those rows.
//
// It must refuse BEFORE writing anything, which is why this asserts the source
// was never asked to flush.
func TestGrowingSourceSyncTaskRefusesToFlushAheadOfSourceTSafe(t *testing.T) {
	segmentID := int64(1)
	mc := metacache.NewMockMetaCache(t)
	segment := growingSegment(segmentID, withManifest("manifest"))
	metacache.UpdateNumOfRows(10)(segment)
	mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(segment)
	}).Return().Maybe()
	mc.EXPECT().RemoveSegments(mock.Anything).Return(nil).Maybe()

	// The delegator has consumed only up to 150; the flush is fenced at 200.
	source := &fakeGrowingFlushSource{tsafe: 150}

	task := NewGrowingSourceSyncTask().
		WithCollectionID(3).
		WithPartitionID(2).
		WithSegmentID(segmentID).
		WithChannelName("ch").
		WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
		WithBatchRows(10).
		WithFlushFromTs(0).
		WithMetaCache(mc).
		WithSource(source)

	err := runTaskForTest(context.Background(), task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has not consumed the flush range")
	require.Zero(t, source.flushed, "nothing may be written while the source is behind the fence")
	require.Empty(t, source.commits, "and nothing may be acked back to the source")

	// Being behind is a normal outcome, so the refusal must be retryable — a
	// terminal classification here would park the segment and pin the channel
	// checkpoint forever instead of re-driving on a later timetick.
	require.Equal(t, SyncRetry, ClassifySyncError(context.Background(), err))
}
