package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQVExternalMilvusTableUsesRealPKCandidate(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().CommonCfg.BloomFilterEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			schema := &schemapb.CollectionSchema{
				ExternalSpec: `{"format":"milvus-table"}`,
				Fields:       []*schemapb.FieldSchema{{FieldID: 100, Name: "id", IsPrimaryKey: true, ExternalField: "id", DataType: pkType}},
			}
			collection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
			local := &qvLocalSegment{segment: &segments.LocalSegment{}, collectionID: 1, collection: collection}
			bfs := pkoracle.NewBloomFilterSet(10, 100, segments.SegmentTypeSealed)
			// BloomFilterSet has no backing stats in this test. Patch its membership
			// lookup to distinguish selecting it from virtual-PK routing.
			pkPatch := mockey.Mock((*pkoracle.BloomFilterSet).PkCandidateExist).Return(true).Build()
			t.Cleanup(func() { pkPatch.UnPatch() })
			hitsPatch := mockey.Mock((*pkoracle.BloomFilterSet).BatchPkExist).Return([]bool{true}).Build()
			t.Cleanup(func() { hitsPatch.UnPatch() })
			loader := segments.NewLoader(ctx, nil, nil)
			patch := mockey.Mock(mockey.GetMethod(loader, "LoadBloomFilterSetWithSchema")).Return([]*pkoracle.BloomFilterSet{bfs}, nil).Build()
			t.Cleanup(func() { patch.UnPatch() })
			realLoader := realQVSegmentLoader{loader: loader}
			require.NoError(t, realLoader.LoadPKCandidate(ctx, local, &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10}))
			require.Equal(t, 1, patch.Times(), "real-PK stats are required even with global BF disabled")
			var pk storage.PrimaryKey = storage.NewInt64PrimaryKey(123)
			if pkType == schemapb.DataType_VarChar {
				pk = storage.NewVarCharPrimaryKey("user_123")
			}
			require.Equal(t, []bool{true}, local.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{pk})))
		})
	}
}

func TestQVExternalVirtualPKKeepsEncodedRouting(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: common.VirtualPKFieldName, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", ExternalField: "value", DataType: schemapb.DataType_Int64},
		},
	}
	local := &qvLocalSegment{segment: &segments.LocalSegment{}, collectionID: 1, collection: segments.NewCollectionWithoutSegcoreForTest(1, schema)}
	require.NoError(t, (realQVSegmentLoader{}).LoadPKCandidate(context.Background(), local, &querypb.SegmentLoadInfo{SegmentID: 10}))
	pks := []storage.PrimaryKey{storage.NewInt64PrimaryKey(10<<32 | 1), storage.NewInt64PrimaryKey(11<<32 | 1)}
	require.Equal(t, []bool{true, false}, local.BatchPkExist(storage.NewBatchLocationsCache(pks)))
}

func TestQVExternalDeltasReachSharedLoader(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loader := segments.NewLoader(ctx, nil, nil)
	patch := mockey.Mock(mockey.GetMethod(loader, "LoadDeltaLogsWithoutResource")).Return(assert.AnError).Build()
	defer patch.UnPatch()
	local := &qvLocalSegment{segment: &segments.LocalSegment{}, collectionID: 1, collection: segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields:       []*schemapb.FieldSchema{{Name: "id", ExternalField: "id", IsPrimaryKey: true}},
	})}
	err := (realQVSegmentLoader{loader: loader}).LoadDeltaLogs(ctx, local, &querypb.SegmentLoadInfo{CollectionID: 1})
	require.ErrorIs(t, err, assert.AnError, "historical delete loading cannot be silently skipped")
	require.Equal(t, 1, patch.Times())
}

func TestQVFinalizeRunsAfterAllMetadataConsumers(t *testing.T) {
	paramtable.Init()
	loader := &fakeQVLoader{segment: &fakeQVSegment{id: 10}}
	patch := mockey.Mock((*fakeQVLoader).FinalizeLoad).To(func(l *fakeQVLoader, _ qvLoadedSegment) error {
		require.True(t, l.loadCalled)
		require.True(t, l.deltaCalled)
		require.True(t, l.pkCalled)
		return nil
	}).Build()
	defer patch.UnPatch()
	_, err := newQueryViewPhysicalSegmentLoader(nil, loader).Load(context.Background(), &querypb.SegmentLoadInfo{SegmentID: 10}, fakeQVCollectionRuntime{})
	require.NoError(t, err)
	require.Equal(t, 1, patch.Times())
}

func TestQVFinalizeCompactsPhysicalSegmentMetadata(t *testing.T) {
	paramtable.Init()
	require.NoError(t, initcore.InitLocalChunkManager(t.TempDir()))
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	schema := &schemapb.CollectionSchema{Name: "finalize", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
	}}
	collection, err := segments.NewCollection(1, schema, nil, &querypb.LoadMetaInfo{})
	require.NoError(t, err)
	t.Cleanup(func() { segments.DeleteCollection(collection) })
	info := &querypb.SegmentLoadInfo{
		CollectionID: 1, SegmentID: 10, PartitionID: 100, Level: datapb.SegmentLevel_L1, InsertChannel: "by-dev-rootcoord-dml_0_1v0",
		BinlogPaths: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "binlog", MemorySize: 800, LogSize: 800}}}},
		Statslogs:   []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "stats"}}}},
	}
	ctx := context.Background()
	segment, err := segments.NewSegment(ctx, collection, segments.NewSegmentManager(), segments.SegmentTypeSealed, 0, info)
	require.NoError(t, err)
	t.Cleanup(func() { segment.Release(ctx) })
	before := segment.ResourceUsageEstimate()
	require.NotEmpty(t, segment.LoadInfo().GetBinlogPaths())
	physical := &qvLocalSegment{segment: segment, collection: collection}
	require.NoError(t, (realQVSegmentLoader{}).FinalizeLoad(physical))
	require.Nil(t, physical.Collection())
	require.Nil(t, segment.(*segments.LocalSegment).GetCollection())
	require.Empty(t, segment.LoadInfo().GetBinlogPaths())
	require.Empty(t, segment.LoadInfo().GetStatslogs())
	require.EqualValues(t, 10, segment.ID())
	require.Equal(t, before, segment.ResourceUsageEstimate())
	require.NotEmpty(t, info.GetBinlogPaths(), "finalization must not compact the original loading metadata in place")
}
