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
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestInitMetaCacheRestoresV3ColumnGroupsAfterMetadataReload(t *testing.T) {
	for _, aliases := range []bool{false, true} {
		t.Run(map[bool]string{false: "unique groups", true: "historical aliases"}[aliases], func(t *testing.T) {
			testInitMetaCacheRestoresV3ColumnGroups(t, aliases)
		})
	}
}

func testInitMetaCacheRestoresV3ColumnGroups(t *testing.T, aliases bool) {
	paramtable.Init()
	root := t.TempDir()
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(root)
	t.Cleanup(initcore.CleanArrowFileSystem)
	params := paramtable.Get()
	oldType := params.CommonCfg.StorageType.GetValue()
	oldFormat := params.DataNodeCfg.StorageFormat.GetValue()
	require.NoError(t, params.Save(params.CommonCfg.StorageType.Key, "local"))
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	t.Cleanup(func() {
		_ = params.Save(params.CommonCfg.StorageType.Key, oldType)
		_ = params.Save(params.DataNodeCfg.StorageFormat.Key, oldFormat)
	})
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	watch := &datapb.ChannelWatchInfo{Vchan: &datapb.VchannelInfo{
		CollectionID: 1, ChannelName: "v1", UnflushedSegments: []*datapb.SegmentInfo{{
			ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
			ManifestPath: packed.MarshalManifestPath(filepath.Join(root, "segment"), packed.ManifestEarliest),
		}},
	}, Schema: schema}
	initial := metacache.NewMetaCache(watch, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NewBM25StatsFactory)
	initial.UpdateSegments(metacache.UpdateCurrentSplit([]storagecommon.ColumnGroup{
		{GroupID: 7, Fields: []int64{0, 1, 100}, Columns: []int{0, 1, 2}, Format: "vortex"},
	}), metacache.WithSegmentIDs(10))

	// Capture the actual sync publication in the V3 catalog shape: keep Stats
	// and ManifestPath, omit the FieldBinlog arrays, then serialize/reload it.
	publish := func(cache metacache.MetaCache, timestamp int64) *datapb.SegmentInfo {
		var saved *datapb.SegmentInfo
		b := broker.NewMockBroker(t)
		attempts := 0
		expectedAttempts := 1
		if aliases && timestamp == 20 {
			expectedAttempts = 2
		}
		var firstAttemptStats, baselineStats *datapb.Statistics
		b.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) error {
			attempts++
			if expectedAttempts == 2 {
				segment, ok := cache.GetSegmentByID(10)
				require.True(t, ok)
				if attempts == 1 {
					firstAttemptStats = proto.Clone(req.GetStats()).(*datapb.Statistics)
					baselineStats = segment.Statistics().Publish()
					require.Len(t, baselineStats.GetLoadResource().GetColumnGroups(), 1)
					require.EqualValues(t, 7, baselineStats.GetLoadResource().GetColumnGroups()[0].GetGroupId())
					return merr.WrapErrServiceUnavailableMsg("injected metadata publication failure")
				}
				require.True(t, proto.Equal(firstAttemptStats, req.GetStats()), "retry must publish identical cumulative statistics")
				require.True(t, proto.Equal(baselineStats, segment.Statistics().Publish()), "failed publication must not advance the collector")
			}
			saved = &datapb.SegmentInfo{
				ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3,
				Level: datapb.SegmentLevel_L1, ManifestPath: req.GetManifestPath(), Stats: proto.Clone(req.GetStats()).(*datapb.Statistics),
				NumOfRows: req.GetCheckPoints()[0].GetNumOfRows(),
			}
			return nil
		}).Times(expectedAttempts)
		cache.UpdateSegments(metacache.UpdateBufferedRows(1), metacache.WithSegmentIDs(10))
		cache.UpdateSegments(metacache.StartSyncing(1), metacache.WithSegmentIDs(10))
		data := &storage.InsertData{Data: map[int64]storage.FieldData{
			0:   &storage.Int64FieldData{Data: []int64{timestamp}},
			1:   &storage.Int64FieldData{Data: []int64{timestamp}},
			100: &storage.Int64FieldData{Data: []int64{timestamp}},
		}}
		pack := new(syncmgr.SyncPack).WithCollectionID(1).WithPartitionID(2).WithSegmentID(10).WithChannelName("v1").
			WithInsertData([]*storage.InsertData{data}).WithBatchRows(1).WithLevel(datapb.SegmentLevel_L1).
			WithTimeRange(uint64(timestamp), uint64(timestamp)).WithCheckpoint(&msgpb.MsgPosition{Timestamp: uint64(timestamp)})
		task := syncmgr.NewSyncTask().WithSyncPack(pack).WithSchema(schema).WithMetaCache(cache).
			WithStorageConfig(config).WithChunkManager(cm).WithAllocator(allocator.NewLocalAllocator(timestamp*100, math.MaxInt64)).
			WithMetaWriter(syncmgr.BrokerMetaWriter(b, 1, retry.Attempts(2), retry.Sleep(time.Millisecond)))
		require.NoError(t, task.Run(context.Background()))
		require.NotNil(t, saved)
		return saved
	}
	saved := publish(initial, 10)
	encoded, err := proto.Marshal(saved)
	require.NoError(t, err)
	reloaded := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(encoded, reloaded))
	require.Empty(t, reloaded.GetBinlogs())
	if aliases {
		group := reloaded.Stats.LoadResource.ColumnGroups[0]
		alias := proto.Clone(group).(*datapb.ColumnGroupStatistics)
		alias.GroupId = 107
		alias.MemorySize = group.MemorySize / 2
		group.MemorySize -= alias.MemorySize
		reloaded.Stats.LoadResource.ColumnGroups = append([]*datapb.ColumnGroupStatistics{alias}, group)
	}
	inputStats := proto.Clone(reloaded.GetStats()).(*datapb.Statistics)
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	cache, err := initMetaCache(context.Background(), cm, watch, nil, []*datapb.SegmentInfo{reloaded}, nil, nil)
	require.NoError(t, err)
	segment, ok := cache.GetSegmentByID(10)
	require.True(t, ok)
	require.Equal(t, []storagecommon.ColumnGroup{{GroupID: 7, Fields: []int64{0, 1, 100}, Format: "vortex"}}, segment.GetCurrentSplit())
	require.True(t, proto.Equal(saved.GetStats(), segment.Statistics().Publish()))
	require.True(t, proto.Equal(inputStats, reloaded.GetStats()), "recovery must not mutate the input statistics")
	require.Empty(t, segment.Binlogs(), "layout hydration must not invent persisted binlogs")
	continued := publish(cache, 20)
	require.EqualValues(t, 2, continued.GetNumOfRows())
	groups := continued.GetStats().GetLoadResource().GetColumnGroups()
	require.Len(t, groups, 1)
	require.EqualValues(t, 7, groups[0].GetGroupId())
	require.Equal(t, []int64{0, 1, 100}, groups[0].GetFieldIds())
	require.Equal(t, []string{"vortex"}, continued.GetStats().GetFormats())
	require.Greater(t, groups[0].GetMemorySize(), saved.GetStats().GetLoadResource().GetColumnGroups()[0].GetMemorySize())
	require.Equal(t, saved.GetStats().GetInsertBinlogSize()*2, continued.GetStats().GetInsertBinlogSize())
	require.Equal(t, saved.GetStats().GetLoadResource().GetColumnGroups()[0].GetMemorySize()*2, groups[0].GetMemorySize())
	recoveredAgain, err := initMetaCache(context.Background(), cm, watch, nil, []*datapb.SegmentInfo{continued}, nil, nil)
	require.NoError(t, err)
	again, ok := recoveredAgain.GetSegmentByID(10)
	require.True(t, ok)
	require.Len(t, again.GetCurrentSplit(), len(segment.GetCurrentSplit()))
	for i, group := range segment.GetCurrentSplit() {
		require.Equal(t, group.GroupID, again.GetCurrentSplit()[i].GroupID)
		require.Equal(t, group.Fields, again.GetCurrentSplit()[i].Fields)
		require.Equal(t, group.Format, again.GetCurrentSplit()[i].Format)
	}
	require.True(t, proto.Equal(continued.GetStats(), again.Statistics().Publish()))
}

func TestInitMetaCacheDoesNotIgnoreColumnGroupReadFailure(t *testing.T) {
	for _, failure := range []error{packed.ErrLoonTransient, packed.ErrLoonPermanent} {
		mockey.PatchConvey("recovery read failure", t, func() {
			mockey.Mock(packed.RestoreColumnGroupsFromManifest).Return(nil, failure).Build()
			_, err := initMetaCache(context.Background(), nil, &datapb.ChannelWatchInfo{Vchan: &datapb.VchannelInfo{}}, nil,
				[]*datapb.SegmentInfo{{ID: 10, StorageVersion: storage.StorageV3, NumOfRows: 1}}, nil, nil)
			require.ErrorIs(t, err, failure)
			require.Equal(t, failure == packed.ErrLoonTransient, retry.IsRecoverable(err))
		})
	}
}

// Keep the constructor's Stats fallback covered independently of the V3 I/O
// path: V2 recovery still has no manifest and retains its previous behavior.
func TestLogicalSplitRestorationDoesNotRequireManifest(t *testing.T) {
	segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		StorageVersion: storage.StorageV2,
		Stats: &datapb.Statistics{LoadResource: &datapb.LoadResourceStatistics{
			ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 101, FieldIds: []int64{101}}},
		}},
	}, nil, nil, nil)
	require.EqualValues(t, 101, segment.GetCurrentSplit()[0].GroupID)
}
