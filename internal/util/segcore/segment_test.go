package segcore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestGrowingSegment(t *testing.T) {
	paramtable.Init()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	err := initcore.InitMmapManager(paramtable.Get(), 1)
	assert.NoError(t, err)
	initcore.InitTieredStorage(paramtable.Get())
	assert.NoError(t, err)

	collectionID := int64(100)
	segmentID := int64(100)

	schema := mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: collectionID,
		Schema:       schema,
		IndexMeta:    mock_segcore.GenTestIndexMeta(collectionID, schema),
	})
	assert.NoError(t, err)
	assert.NotNil(t, collection)
	defer collection.Release()

	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  collection,
		SegmentID:   segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
		IsSorted:    false,
	})

	assert.NoError(t, err)
	assert.NotNil(t, segment)
	defer segment.Release()

	assert.Equal(t, segmentID, segment.ID())
	assert.Equal(t, int64(0), segment.RowNum())
	assert.Zero(t, segment.MemSize())
	assert.True(t, segment.HasRawData(0))
	assertEqualCount(t, collection, segment, 0)

	insertMsg, err := mock_segcore.GenInsertMsg(collection, 1, segmentID, 100)
	assert.NoError(t, err)
	insertResult, err := segment.Insert(context.Background(), &segcore.InsertRequest{
		RowIDs:     insertMsg.RowIDs,
		Timestamps: insertMsg.Timestamps,
		Record: &segcorepb.InsertRecord{
			FieldsData: insertMsg.FieldsData,
			NumRows:    int64(len(insertMsg.RowIDs)),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, insertResult)
	assert.Equal(t, int64(100), insertResult.InsertedRows)

	assert.Equal(t, int64(100), segment.RowNum())
	assertEqualCount(t, collection, segment, 100)

	pk := storage.NewInt64PrimaryKeys(1)
	pk.Append(storage.NewInt64PrimaryKey(10))
	deleteResult, err := segment.Delete(context.Background(), &segcore.DeleteRequest{
		PrimaryKeys: pk,
		Timestamps: []typeutil.Timestamp{
			1000,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, deleteResult)

	assert.Equal(t, int64(99), segment.RowNum())
}

func assertEqualCount(
	t *testing.T,
	collection *segcore.CCollection,
	segment segcore.CSegment,
	count int64,
) {
	plan := planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				IsCount: true,
			},
		},
	}
	expr, err := proto.Marshal(&plan)
	assert.NoError(t, err)
	retrievePlan, err := segcore.NewRetrievePlan(
		collection,
		expr,
		typeutil.MaxTimestamp,
		100,
		0,
		0)
	defer retrievePlan.Delete()

	assert.True(t, retrievePlan.ShouldIgnoreNonPk())
	assert.False(t, retrievePlan.IsIgnoreNonPk())
	retrievePlan.SetIgnoreNonPk(true)
	assert.True(t, retrievePlan.IsIgnoreNonPk())
	assert.NotZero(t, retrievePlan.MsgID())

	assert.NotNil(t, retrievePlan)
	assert.NoError(t, err)

	retrieveResult, err := segment.Retrieve(context.Background(), retrievePlan)
	assert.NotNil(t, retrieveResult)
	assert.NoError(t, err)
	result, err := retrieveResult.GetResult()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, count, result.AllRetrieveCount)
	retrieveResult.Release()

	retrieveResult2, err := segment.RetrieveByOffsets(context.Background(), &segcore.RetrievePlanWithOffsets{
		RetrievePlan: retrievePlan,
		Offsets:      []int64{0, 1, 2, 3, 4},
	})
	assert.NoError(t, err)
	assert.NotNil(t, retrieveResult2)
	retrieveResult2.Release()
}

func TestConvertToSegcoreSegmentLoadInfo(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		result := segcore.ConvertToSegcoreSegmentLoadInfo(nil)
		assert.Nil(t, result)
	})

	t.Run("empty input", func(t *testing.T) {
		src := &querypb.SegmentLoadInfo{}
		result := segcore.ConvertToSegcoreSegmentLoadInfo(src)
		assert.NotNil(t, result)
		assert.Equal(t, int64(0), result.SegmentID)
		assert.Equal(t, int64(0), result.PartitionID)
		assert.Equal(t, int64(0), result.CollectionID)
	})

	t.Run("full conversion", func(t *testing.T) {
		// Create source querypb.SegmentLoadInfo with all fields populated
		src := &querypb.SegmentLoadInfo{
			SegmentID:    1001,
			PartitionID:  2001,
			CollectionID: 3001,
			DbID:         4001,
			FlushTime:    5001,
			BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum:    10,
							TimestampFrom: 1000,
							TimestampTo:   2000,
							LogPath:       "/path/to/binlog",
							LogSize:       1024,
							LogID:         9001,
							MemorySize:    2048,
						},
					},
					ChildFields: []int64{101, 102},
				},
			},
			NumOfRows: 1000,
			Statslogs: []*datapb.FieldBinlog{
				{
					FieldID: 200,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum:    5,
							TimestampFrom: 1500,
							TimestampTo:   2500,
							LogPath:       "/path/to/statslog",
							LogSize:       512,
							LogID:         9002,
							MemorySize:    1024,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					FieldID: 300,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum:    3,
							TimestampFrom: 2000,
							TimestampTo:   3000,
							LogPath:       "/path/to/deltalog",
							LogSize:       256,
							LogID:         9003,
							MemorySize:    512,
						},
					},
				},
			},
			CompactionFrom: []int64{8001, 8002},
			IndexInfos: []*querypb.FieldIndexInfo{
				{
					FieldID:             100,
					EnableIndex:         true,
					IndexName:           "test_index",
					IndexID:             7001,
					BuildID:             7002,
					IndexParams:         []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}},
					IndexFilePaths:      []string{"/path/to/index"},
					IndexSize:           4096,
					IndexVersion:        1,
					NumRows:             1000,
					CurrentIndexVersion: 2,
					IndexStoreVersion:   3,
				},
			},
			SegmentSize:     8192,
			InsertChannel:   "insert_channel_1",
			ReadableVersion: 6001,
			StorageVersion:  7001,
			IsSorted:        true,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				400: {
					FieldID:    400,
					Version:    1,
					Files:      []string{"/path/to/text/stats1", "/path/to/text/stats2"},
					LogSize:    2048,
					MemorySize: 4096,
					BuildID:    9101,
				},
			},
			Bm25Logs: []*datapb.FieldBinlog{
				{
					FieldID: 500,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum:    7,
							TimestampFrom: 3000,
							TimestampTo:   4000,
							LogPath:       "/path/to/bm25log",
							LogSize:       768,
							LogID:         9004,
							MemorySize:    1536,
						},
					},
				},
			},
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				600: {
					FieldID:                600,
					Version:                2,
					Files:                  []string{"/path/to/json/stats"},
					LogSize:                1024,
					MemorySize:             2048,
					BuildID:                9201,
					JsonKeyStatsDataFormat: 1,
				},
			},
			Priority: commonpb.LoadPriority_HIGH,
		}

		// Convert to segcorepb.SegmentLoadInfo
		result := segcore.ConvertToSegcoreSegmentLoadInfo(src)

		// Validate basic fields
		assert.NotNil(t, result)
		assert.Equal(t, src.SegmentID, result.SegmentID)
		assert.Equal(t, src.PartitionID, result.PartitionID)
		assert.Equal(t, src.CollectionID, result.CollectionID)
		assert.Equal(t, src.DbID, result.DbID)
		assert.Equal(t, src.FlushTime, result.FlushTime)
		assert.Equal(t, src.NumOfRows, result.NumOfRows)
		assert.Equal(t, src.SegmentSize, result.SegmentSize)
		assert.Equal(t, src.InsertChannel, result.InsertChannel)
		assert.Equal(t, src.ReadableVersion, result.ReadableVersion)
		assert.Equal(t, src.StorageVersion, result.StorageVersion)
		assert.Equal(t, src.IsSorted, result.IsSorted)
		assert.Equal(t, src.Priority, result.Priority)
		assert.Equal(t, src.CompactionFrom, result.CompactionFrom)

		// Validate BinlogPaths conversion
		assert.Equal(t, len(src.BinlogPaths), len(result.BinlogPaths))
		assert.Equal(t, src.BinlogPaths[0].FieldID, result.BinlogPaths[0].FieldID)
		assert.Equal(t, len(src.BinlogPaths[0].Binlogs), len(result.BinlogPaths[0].Binlogs))
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].EntriesNum, result.BinlogPaths[0].Binlogs[0].EntriesNum)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].TimestampFrom, result.BinlogPaths[0].Binlogs[0].TimestampFrom)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].TimestampTo, result.BinlogPaths[0].Binlogs[0].TimestampTo)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].LogPath, result.BinlogPaths[0].Binlogs[0].LogPath)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].LogSize, result.BinlogPaths[0].Binlogs[0].LogSize)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].LogID, result.BinlogPaths[0].Binlogs[0].LogID)
		assert.Equal(t, src.BinlogPaths[0].Binlogs[0].MemorySize, result.BinlogPaths[0].Binlogs[0].MemorySize)
		assert.Equal(t, src.BinlogPaths[0].ChildFields, result.BinlogPaths[0].ChildFields)

		// Validate Statslogs conversion
		assert.Equal(t, len(src.Statslogs), len(result.Statslogs))
		assert.Equal(t, src.Statslogs[0].FieldID, result.Statslogs[0].FieldID)

		// Validate Deltalogs conversion
		assert.Equal(t, len(src.Deltalogs), len(result.Deltalogs))
		assert.Equal(t, src.Deltalogs[0].FieldID, result.Deltalogs[0].FieldID)

		// Validate IndexInfos conversion
		assert.Equal(t, len(src.IndexInfos), len(result.IndexInfos))
		assert.Equal(t, src.IndexInfos[0].FieldID, result.IndexInfos[0].FieldID)
		assert.Equal(t, src.IndexInfos[0].EnableIndex, result.IndexInfos[0].EnableIndex)
		assert.Equal(t, src.IndexInfos[0].IndexName, result.IndexInfos[0].IndexName)
		assert.Equal(t, src.IndexInfos[0].IndexID, result.IndexInfos[0].IndexID)
		assert.Equal(t, src.IndexInfos[0].BuildID, result.IndexInfos[0].BuildID)
		assert.Equal(t, len(src.IndexInfos[0].IndexParams), len(result.IndexInfos[0].IndexParams))
		assert.Equal(t, src.IndexInfos[0].IndexFilePaths, result.IndexInfos[0].IndexFilePaths)
		assert.Equal(t, src.IndexInfos[0].IndexSize, result.IndexInfos[0].IndexSize)
		assert.Equal(t, src.IndexInfos[0].IndexVersion, result.IndexInfos[0].IndexVersion)
		assert.Equal(t, src.IndexInfos[0].NumRows, result.IndexInfos[0].NumRows)
		assert.Equal(t, src.IndexInfos[0].CurrentIndexVersion, result.IndexInfos[0].CurrentIndexVersion)
		assert.Equal(t, src.IndexInfos[0].IndexStoreVersion, result.IndexInfos[0].IndexStoreVersion)

		// Validate TextStatsLogs conversion
		assert.Equal(t, len(src.TextStatsLogs), len(result.TextStatsLogs))
		textStats := result.TextStatsLogs[400]
		assert.NotNil(t, textStats)
		assert.Equal(t, src.TextStatsLogs[400].FieldID, textStats.FieldID)
		assert.Equal(t, src.TextStatsLogs[400].Version, textStats.Version)
		assert.Equal(t, src.TextStatsLogs[400].Files, textStats.Files)
		assert.Equal(t, src.TextStatsLogs[400].LogSize, textStats.LogSize)
		assert.Equal(t, src.TextStatsLogs[400].MemorySize, textStats.MemorySize)
		assert.Equal(t, src.TextStatsLogs[400].BuildID, textStats.BuildID)

		// Validate Bm25Logs conversion
		assert.Equal(t, len(src.Bm25Logs), len(result.Bm25Logs))
		assert.Equal(t, src.Bm25Logs[0].FieldID, result.Bm25Logs[0].FieldID)

		// Validate JsonKeyStatsLogs conversion
		assert.Equal(t, len(src.JsonKeyStatsLogs), len(result.JsonKeyStatsLogs))
		jsonStats := result.JsonKeyStatsLogs[600]
		assert.NotNil(t, jsonStats)
		assert.Equal(t, src.JsonKeyStatsLogs[600].FieldID, jsonStats.FieldID)
		assert.Equal(t, src.JsonKeyStatsLogs[600].Version, jsonStats.Version)
		assert.Equal(t, src.JsonKeyStatsLogs[600].Files, jsonStats.Files)
		assert.Equal(t, src.JsonKeyStatsLogs[600].LogSize, jsonStats.LogSize)
		assert.Equal(t, src.JsonKeyStatsLogs[600].MemorySize, jsonStats.MemorySize)
		assert.Equal(t, src.JsonKeyStatsLogs[600].BuildID, jsonStats.BuildID)
		assert.Equal(t, src.JsonKeyStatsLogs[600].JsonKeyStatsDataFormat, jsonStats.JsonKeyStatsDataFormat)
	})

	t.Run("nil elements in arrays and maps", func(t *testing.T) {
		src := &querypb.SegmentLoadInfo{
			SegmentID: 1001,
			BinlogPaths: []*datapb.FieldBinlog{
				nil, // nil element should be skipped
				{FieldID: 100},
			},
			Statslogs: []*datapb.FieldBinlog{
				nil,
			},
			IndexInfos: []*querypb.FieldIndexInfo{
				nil,
				{FieldID: 200},
			},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				100: nil, // nil value should be skipped
				200: {FieldID: 200},
			},
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				300: nil,
				400: {FieldID: 400},
			},
		}

		result := segcore.ConvertToSegcoreSegmentLoadInfo(src)

		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result.BinlogPaths))
		assert.Equal(t, int64(100), result.BinlogPaths[0].FieldID)
		assert.Equal(t, 0, len(result.Statslogs))
		assert.Equal(t, 1, len(result.IndexInfos))
		assert.Equal(t, int64(200), result.IndexInfos[0].FieldID)
		assert.Equal(t, 1, len(result.TextStatsLogs))
		assert.NotNil(t, result.TextStatsLogs[200])
		assert.Equal(t, 1, len(result.JsonKeyStatsLogs))
		assert.NotNil(t, result.JsonKeyStatsLogs[400])
	})
}
