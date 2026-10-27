// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"context"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// A different batch can change DefaultPolicies' result even with identical
// schema and configuration. Exercise the native manifest commit, including
// a control that discards the recovered split as the old recovery path did.
func TestV3ColumnSplitDriftAfterRecovery(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&params.CommonCfg.StorageType, "local"},
		{&params.DataNodeCfg.StorageFormat, "parquet"},
		{&params.CommonCfg.Stv2SplitSystemColumn, "true"},
		{&params.CommonCfg.Stv2SystemColumnIncludePK, "true"},
		{&params.CommonCfg.Stv2SplitByAvgSize, "true"},
		{&params.CommonCfg.Stv2SplitAvgSizeThreshold, "64"},
	} {
		key, previous := setting.item.Key, setting.item.GetValue()
		require.NoError(t, params.Save(key, setting.value))
		t.Cleanup(func() { _ = params.Save(key, previous) })
	}
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(t.TempDir())
	t.Cleanup(initcore.CleanArrowFileSystem)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "4096"}}},
		{FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}
	for _, discardSplit := range []bool{true, false} {
		name := "restored_split"
		if discardSplit {
			name = "lost_split_control"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			config := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			watch := &datapb.ChannelWatchInfo{Schema: schema, Vchan: &datapb.VchannelInfo{
				CollectionID: 1, ChannelName: "v1", UnflushedSegments: []*datapb.SegmentInfo{{
					ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
					ManifestPath: packed.MarshalManifestPath(filepath.Join(root, "segment"), packed.ManifestEarliest),
				}},
			}}
			initial := metacache.NewMetaCache(watch, func(*datapb.SegmentInfo) pkoracle.PkStat {
				return pkoracle.NewBloomFilterSet()
			}, metacache.NewBM25StatsFactory)
			assertRows := func(manifest string, expectedPKs []int64, expectedTexts []string) {
				readSchema := arrow.NewSchema([]arrow.Field{
					{Name: "100", Type: arrow.PrimitiveTypes.Int64},
					{Name: "101", Type: arrow.BinaryTypes.String},
				}, nil)
				reader, err := packed.NewFFIPackedReader(manifest, readSchema, []string{"100", "101"}, 8192, config, nil, packed.ExternalReaderContext{})
				require.NoError(t, err)
				defer func() { require.NoError(t, reader.Close()) }()
				var pks []int64
				var texts []string
				for {
					record, err := reader.ReadNext()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					for i := 0; i < int(record.NumRows()); i++ {
						pks = append(pks, record.Column(0).(*array.Int64).Value(i))
						texts = append(texts, strings.Clone(record.Column(1).(*array.String).Value(i)))
					}
				}
				require.Equal(t, expectedPKs, pks)
				require.Equal(t, expectedTexts, texts)
			}
			publish := func(cache metacache.MetaCache, timestamp int64, text string, expectSuccess bool) (*datapb.SegmentInfo, error) {
				b := broker.NewMockBroker(t)
				var saved *datapb.SegmentInfo
				if expectSuccess {
					b.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) error {
						saved = &datapb.SegmentInfo{
							ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3,
							Level: datapb.SegmentLevel_L1, ManifestPath: req.GetManifestPath(), Stats: proto.Clone(req.GetStats()).(*datapb.Statistics),
							NumOfRows: req.GetCheckPoints()[0].GetNumOfRows(),
						}
						return nil
					}).Once()
				}
				cache.UpdateSegments(metacache.UpdateBufferedRows(1), metacache.WithSegmentIDs(10))
				cache.UpdateSegments(metacache.StartSyncing(1), metacache.WithSegmentIDs(10))
				data := &storage.InsertData{Data: map[int64]storage.FieldData{
					0: &storage.Int64FieldData{Data: []int64{timestamp}}, 1: &storage.Int64FieldData{Data: []int64{timestamp}},
					100: &storage.Int64FieldData{Data: []int64{timestamp}}, 101: &storage.StringFieldData{Data: []string{text}},
					102: &storage.Int64FieldData{Data: []int64{timestamp}}, 103: &storage.FloatVectorFieldData{Data: []float32{1, 2}, Dim: 2},
				}}
				pack := new(syncmgr.SyncPack).WithCollectionID(1).WithPartitionID(2).WithSegmentID(10).WithChannelName("v1").
					WithInsertData([]*storage.InsertData{data}).WithBatchRows(1).WithLevel(datapb.SegmentLevel_L1).
					WithTimeRange(uint64(timestamp), uint64(timestamp)).WithCheckpoint(&msgpb.MsgPosition{Timestamp: uint64(timestamp)})
				task := syncmgr.NewSyncTask().WithSyncPack(pack).WithSchema(schema).WithMetaCache(cache).
					WithStorageConfig(config).WithChunkManager(cm).WithAllocator(allocator.NewLocalAllocator(timestamp*100, math.MaxInt64)).
					WithWriteRetryOptions(retry.Attempts(2), retry.Sleep(time.Millisecond)).WithMetaWriter(syncmgr.BrokerMetaWriter(b, 1))
				err := task.Run(context.Background())
				return saved, err
			}
			saved, err := publish(initial, 10, "short", true)
			require.NoError(t, err)
			require.Len(t, saved.GetStats().GetLoadResource().GetColumnGroups(), 3)
			encoded, err := proto.Marshal(saved)
			require.NoError(t, err)
			reloaded := &datapb.SegmentInfo{}
			require.NoError(t, proto.Unmarshal(encoded, reloaded))
			require.Empty(t, reloaded.GetBinlogs())
			cache, err := initMetaCache(context.Background(), cm, watch, nil, []*datapb.SegmentInfo{reloaded}, nil, nil)
			require.NoError(t, err)
			segment, ok := cache.GetSegmentByID(10)
			require.True(t, ok)
			require.Len(t, segment.GetCurrentSplit(), 3)
			if discardSplit {
				cache.UpdateSegments(metacache.UpdateCurrentSplit(nil), metacache.WithSegmentIDs(10))
			}
			continued, err := publish(cache, 20, strings.Repeat("x", 1024), !discardSplit)
			if discardSplit {
				require.ErrorContains(t, err, "Column group size mismatch: existing has 3 groups, but appended has 4 groups")
				require.ErrorIs(t, err, packed.ErrLoonPermanent)
				require.Nil(t, continued, "a rejected manifest must not publish segment metadata")
				unchanged, readErr := packed.RestoreColumnGroupsFromManifest(context.Background(), saved.GetManifestPath(), saved.GetStats(), config)
				require.NoError(t, readErr)
				require.Len(t, unchanged, 3)
				assertRows(saved.GetManifestPath(), []int64{10}, []string{"short"})
				t.Logf("reproduced native commit error: %v", err)
				return
			}
			require.NoError(t, err)
			assertRows(continued.GetManifestPath(), []int64{10, 20}, []string{"short", strings.Repeat("x", 1024)})
			require.EqualValues(t, 2, continued.GetNumOfRows())
			groups := continued.GetStats().GetLoadResource().GetColumnGroups()
			require.Len(t, groups, 3)
			for i, group := range groups {
				prior := saved.GetStats().GetLoadResource().GetColumnGroups()[i]
				require.Equal(t, prior.GetGroupId(), group.GetGroupId())
				require.Equal(t, prior.GetFieldIds(), group.GetFieldIds())
				require.Greater(t, group.GetMemorySize(), prior.GetMemorySize())
			}
			layout, err := packed.RestoreColumnGroupsFromManifest(context.Background(), continued.GetManifestPath(), continued.GetStats(), config)
			require.NoError(t, err)
			require.Equal(t, []storagecommon.ColumnGroup{
				{GroupID: 0, Fields: []int64{0, 1, 100}, Format: "parquet"},
				{GroupID: 1, Fields: []int64{101, 102}, Format: "parquet"},
				{GroupID: 103, Fields: []int64{103}, Format: "parquet"},
			}, layout)
		})
	}
}
