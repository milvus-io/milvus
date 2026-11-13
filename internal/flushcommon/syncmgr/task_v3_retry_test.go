// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncmgr

import (
	"context"
	"io"
	"path"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// Use real local writers, manifests, allocators and metacache. Only the failure
// boundary and coordinator acknowledgement are patched; failed attempts must
// not become visible through the persisted manifest or cumulative statistics.
func newV3RetryTask(t *testing.T) *SyncTask {
	t.Helper()
	paramtable.Init()
	params := paramtable.Get()
	oldFormat := params.DataNodeCfg.StorageFormat.GetValue()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() { params.Save(params.DataNodeCfg.StorageFormat.Key, oldFormat) })
	root := t.TempDir()
	initcore.InitLocalArrowFileSystem(root)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
		},
	}}
	data, err := storage.NewInsertData(schema)
	require.NoError(t, err)
	for id := int64(1); id <= 3; id++ {
		require.NoError(t, data.Append(map[int64]interface{}{
			common.RowIDField: id, common.TimeStampField: id, 100: id, 101: []float32{1, 2},
		}))
	}
	cache := metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3-retry", UnflushedSegments: []*datapb.SegmentInfo{{
			ID: 3, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
			ManifestPath: packed.MarshalManifestPath(path.Join(root, "insert_log/1/2/3"), packed.ManifestEarliest),
		}}},
	}, func(*datapb.SegmentInfo) pkoracle.PkStat { return pkoracle.NewBloomFilterSet() },
		metacache.NewBM25StatsFactory)
	cache.UpdateSegments(metacache.MergeSegmentAction(metacache.UpdateBufferedRows(3), metacache.StartSyncing(3)),
		metacache.WithSegmentIDs(3))
	pack := new(SyncPack).WithCollectionID(1).WithPartitionID(2).WithSegmentID(3).
		WithChannelName("v3-retry").WithInsertData([]*storage.InsertData{data}).WithBatchRows(3).
		WithTimeRange(1, 3).WithLevel(datapb.SegmentLevel_L1).
		WithCheckpoint(&msgpb.MsgPosition{ChannelName: "v3-retry", Timestamp: 3, MsgID: []byte{3}})
	return NewSyncTask().WithSyncPack(pack).WithSchema(schema).WithMetaCache(cache).
		WithAllocator(allocator.NewLocalAllocator(1000, 2000)).
		WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(root))).
		WithStorageConfig(&indexpb.StorageConfig{StorageType: "local", RootPath: root}).
		WithWriteRetryOptions(retry.Attempts(2), retry.Sleep(time.Millisecond), retry.MaxSleepTime(time.Millisecond))
}

func requireV3RetryRows(t *testing.T, task *SyncTask, manifest string) {
	t.Helper()
	schema, err := storage.ConvertToArrowSchema(task.schema, true)
	require.NoError(t, err)
	reader, err := packed.NewFFIPackedReader(manifest, schema, []string{"100"}, 8192,
		task.storageConfig, nil, packed.ExternalReaderContext{})
	require.NoError(t, err)
	defer reader.Close()
	var ids []int64
	for {
		record, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		ids = append(ids, record.Column(0).(*array.Int64).Int64Values()...)
	}
	require.Equal(t, []int64{1, 2, 3}, ids, "retry must not publish duplicate or missing records")
}

func TestSyncTaskV3RetryTransientWriterFailure(t *testing.T) {
	for _, stage := range []string{"write", "close", "stats", "delta", "after_stats", "commit"} {
		t.Run(stage, func(t *testing.T) {
			task := newV3RetryTask(t)
			transient := merr.Wrap(packed.ErrLoonTransient, "injected writer timeout")
			writes, closes, commits, failures, metaWrites := 0, 0, 0, 0, 0
			destroyed := make(map[*packed.FFIPackedWriter]int)
			outputsDestroyed := 0
			var destroy func(*packed.FFIPackedWriter)
			defer mockey.Mock((*packed.FFIPackedWriter).Destroy).To(func(w *packed.FFIPackedWriter) {
				destroyed[w]++
				destroy(w)
			}).Origin(&destroy).Build().UnPatch()
			var destroyOutput func(*packed.ColumnGroups)
			defer mockey.Mock((*packed.ColumnGroups).Destroy).To(func(out *packed.ColumnGroups) {
				outputsDestroyed++
				destroyOutput(out)
			}).Origin(&destroyOutput).Build().UnPatch()
			var write func(*packed.FFIPackedWriter, arrow.Record) error
			defer mockey.Mock((*packed.FFIPackedWriter).WriteRecordBatch).To(func(w *packed.FFIPackedWriter, r arrow.Record) error {
				writes++
				err := write(w, r)
				if err == nil && stage == "write" && writes == 1 {
					return transient
				}
				return err
			}).Origin(&write).Build().UnPatch()
			var closeWriter func(*packed.FFIPackedWriter) (packed.WriterOutput, error)
			defer mockey.Mock((*packed.FFIPackedWriter).Close).To(func(w *packed.FFIPackedWriter) (packed.WriterOutput, error) {
				closes++
				out, err := closeWriter(w)
				if err == nil && stage == "close" && closes == 1 {
					// Fail after the real Close has consumed the writer and uploaded
					// its data, so retrying the same native handle cannot succeed.
					out.Destroy()
					return nil, transient
				}
				return out, err
			}).Origin(&closeWriter).Build().UnPatch()
			statsWrites := 0
			var writeFile func(*indexpb.StorageConfig, string, []byte) error
			defer mockey.Mock(packed.WriteFile).To(func(cfg *indexpb.StorageConfig, name string, data []byte) error {
				statsWrites++
				if stage == "stats" && statsWrites == 1 {
					return transient
				}
				return writeFile(cfg, name, data)
			}).Origin(&writeFile).Build().UnPatch()
			deltaWrites := 0
			var delta func(*BulkPackWriterV3, context.Context, *SyncPack, string) (*datapb.FieldBinlog, []packed.DeltaLogEntry, error)
			defer mockey.Mock((*BulkPackWriterV3).writeDelta).To(func(w *BulkPackWriterV3, ctx context.Context, pack *SyncPack, base string) (*datapb.FieldBinlog, []packed.DeltaLogEntry, error) {
				deltaWrites++
				if stage == "delta" && deltaWrites == 1 {
					return nil, nil, merr.Wrap(merr.ErrIoTooManyRequests, "injected delta write failure")
				}
				return delta(w, ctx, pack, base)
			}).Origin(&delta).Build().UnPatch()
			afterStats := 0
			var bm25 func(*BulkPackWriterV3, context.Context, *SyncPack, string) ([]packed.StatEntry, error)
			defer mockey.Mock((*BulkPackWriterV3).writeBM25Stasts).To(func(w *BulkPackWriterV3, ctx context.Context, pack *SyncPack, base string) ([]packed.StatEntry, error) {
				afterStats++
				if stage == "after_stats" && afterStats == 1 {
					return nil, merr.Wrap(merr.ErrIoTooManyRequests, "injected after stats accounting")
				}
				return bm25(w, ctx, pack, base)
			}).Origin(&bm25).Build().UnPatch()
			var commit func(string, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error)
			defer mockey.Mock(packed.CommitManifestUpdates).To(func(base string, cfg *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
				commits++
				if stage == "commit" && commits == 1 {
					return "", transient
				}
				return commit(base, cfg, updates)
			}).Origin(&commit).Build().UnPatch()
			defer mockey.Mock((*brokerMetaWriter).UpdateSync).To(func(_ *brokerMetaWriter, _ context.Context, actual *SyncTask) error {
				metaWrites++
				require.Same(t, task, actual)
				segment, ok := task.metacache.GetSegmentByID(task.segmentID)
				require.True(t, ok)
				require.Zero(t, segment.FlushedRows(), "only coordinator ack installs row accounting")
				require.Nil(t, segment.Statistics().Publish())
				return nil
			}).Build().UnPatch()
			task.WithMetaWriter(BrokerMetaWriter(nil, 1)).WithFailureCallback(func(err error) {
				failures++
				panic(err)
			})
			require.NotPanics(t, func() { require.NoError(t, task.Run(context.Background())) })
			require.Zero(t, failures)
			require.Equal(t, 1, metaWrites)
			expectedAttempts, expectedCommits := 2, 1
			if stage == "commit" {
				expectedAttempts, expectedCommits = 1, 2
			}
			require.Equal(t, expectedAttempts, writes)
			require.Equal(t, expectedAttempts, closes)
			require.Equal(t, expectedCommits, commits)
			require.Len(t, destroyed, expectedAttempts, "each attempt needs a fresh native writer")
			for _, count := range destroyed {
				require.Equal(t, 1, count, "each writer must release resources exactly once")
			}
			require.Equal(t, expectedAttempts, outputsDestroyed)
			_, version, err := packed.UnmarshalManifestPath(task.manifestPath)
			require.NoError(t, err)
			require.EqualValues(t, 1, version)
			requireV3RetryRows(t, task, task.manifestPath)
			stats, err := packed.GetManifestStats(task.manifestPath, task.storageConfig)
			require.NoError(t, err)
			require.Len(t, stats["bloom_filter.100"].Paths, 1, "only the successful stats blob is published")
			blob, err := packed.ReadFile(task.storageConfig, stats["bloom_filter.100"].Paths[0])
			require.NoError(t, err)
			require.EqualValues(t, len(blob), task.stats.GetStatsBinlogSize())
			require.EqualValues(t, len(blob), task.flushedSize, "failed-attempt accounting must be discarded")
			segment, ok := task.metacache.GetSegmentByID(task.segmentID)
			require.True(t, ok)
			require.EqualValues(t, 3, segment.FlushedRows())
			require.EqualValues(t, 3, segment.NumOfRows())
			require.Zero(t, segment.SyncingRows())
			require.True(t, proto.Equal(task.stats, segment.Statistics().Publish()))
			require.Empty(t, task.pack.insertData)
		})
	}
}

func TestSyncTaskV3RetryStopsOnTerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts int
		noRetry  bool
		cancel   bool
	}{
		{name: "exhausted", err: packed.ErrLoonTransient, attempts: 2},
		{name: "permanent", err: packed.ErrLoonPermanent, attempts: 1},
		{name: "no_options", err: packed.ErrLoonTransient, attempts: 1, noRetry: true},
		{name: "canceled", err: packed.ErrLoonTransient, attempts: 1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := newV3RetryTask(t)
			if tc.noRetry {
				task.WithWriteRetryOptions()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closes, failures, metaWrites := 0, 0, 0
			var closeWriter func(*packed.FFIPackedWriter) (packed.WriterOutput, error)
			defer mockey.Mock((*packed.FFIPackedWriter).Close).To(func(w *packed.FFIPackedWriter) (packed.WriterOutput, error) {
				closes++
				out, err := closeWriter(w)
				require.NoError(t, err)
				out.Destroy()
				if tc.cancel {
					cancel()
				}
				return nil, tc.err
			}).Origin(&closeWriter).Build().UnPatch()
			defer mockey.Mock((*brokerMetaWriter).UpdateSync).To(func(*brokerMetaWriter, context.Context, *SyncTask) error {
				metaWrites++
				return nil
			}).Build().UnPatch()
			task.WithMetaWriter(BrokerMetaWriter(nil, 1)).WithFailureCallback(func(err error) {
				failures++
				require.ErrorIs(t, err, tc.err)
			})
			require.ErrorIs(t, task.Run(ctx), tc.err)
			require.Equal(t, tc.attempts, closes)
			require.Equal(t, 1, failures)
			require.Zero(t, metaWrites)
			segment, ok := task.metacache.GetSegmentByID(task.segmentID)
			require.True(t, ok)
			require.Zero(t, segment.FlushedRows())
			require.Nil(t, segment.Statistics().Publish())
			_, version, err := packed.UnmarshalManifestPath(segment.ManifestPath())
			require.NoError(t, err)
			require.EqualValues(t, packed.ManifestEarliest, version)
			require.Len(t, task.pack.insertData, 1, "failed persistence must retain input data")
		})
	}
}

func TestSyncTaskV3RetryStopsAfterCommit(t *testing.T) {
	task := newV3RetryTask(t)
	writes, failures := 0, 0
	var write func(*packed.FFIPackedWriter, arrow.Record) error
	defer mockey.Mock((*packed.FFIPackedWriter).WriteRecordBatch).To(func(w *packed.FFIPackedWriter, r arrow.Record) error {
		writes++
		return write(w, r)
	}).Origin(&write).Build().UnPatch()
	defer mockey.Mock((*BulkPackWriterV2).finalizeStats).Return(nil, packed.ErrLoonTransient).Build().UnPatch()
	task.WithFailureCallback(func(error) { failures++ })
	err := task.Run(context.Background())
	require.ErrorIs(t, err, packed.ErrLoonTransient)
	require.False(t, retry.IsRecoverable(err))
	require.Equal(t, 1, writes, "post-commit failure must not write the same batch again")
	require.Equal(t, 1, failures)
	_, version, err := packed.UnmarshalManifestPath(task.manifestPath)
	require.NoError(t, err)
	require.EqualValues(t, 1, version)
	requireV3RetryRows(t, task, task.manifestPath)
	segment, ok := task.metacache.GetSegmentByID(task.segmentID)
	require.True(t, ok)
	require.Zero(t, segment.FlushedRows())
	require.Nil(t, segment.Statistics().Publish())
	require.NotEmpty(t, task.pack.insertData)
}
