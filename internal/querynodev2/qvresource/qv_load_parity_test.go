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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQVExternalMilvusTableUsesRealPKCandidate(t *testing.T) {
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			patchNativeCollections(t)
			// Native accounting is outside this candidate-routing regression.
			patchCollectionLifetime(t, mockey.Mock((*pkoracle.BloomFilterSet).Charge).Return().Build())
			patchCollectionLifetime(t, mockey.Mock((*pkoracle.BloomFilterSet).Refund).Return().Build())
			cfg := paramtable.Get()
			key, previous := cfg.CommonCfg.BloomFilterEnabled.Key, cfg.CommonCfg.BloomFilterEnabled.GetValue()
			require.NoError(t, cfg.Save(key, "false"))
			t.Cleanup(func() { require.NoError(t, cfg.Save(key, previous)) })
			meta := collectionMetadata(1)
			meta.collection.Schema.ExternalSpec = `{"format":"milvus-table"}`
			meta.collection.Schema.ExternalSource = "s3://bucket/source"
			meta.collection.Schema.Fields[0].ExternalField = "pk"
			meta.collection.Schema.Fields[0].DataType = pkType
			guard := acquireRuntime(t, newQueryViewCollectionRuntimeManager(meta), 1)
			t.Cleanup(guard.Release)
			var pk storage.PrimaryKey = storage.NewInt64PrimaryKey(123)
			if pkType == schemapb.DataType_VarChar {
				pk = storage.NewVarCharPrimaryKey("user_123")
			}
			stats, err := storage.NewPrimaryKeyStats(100, int64(pkType), 1)
			require.NoError(t, err)
			stats.Update(pk)
			const path = "s3://bucket/source/stats/1001"
			read := mockey.Mock(packed.ReadFileWithExternalSpec).To(func(_ *indexpb.StorageConfig, actual string, ext packed.ExternalSpecContext) ([]byte, error) {
				assert.Equal(t, path, actual)
				assert.Equal(t, guard.Schema().GetExternalSource(), ext.Source)
				assert.EqualValues(t, 1, ext.CollectionID)
				return []byte("stats"), nil
			}).Build()
			patchCollectionLifetime(t, read)
			patchCollectionLifetime(t, mockey.Mock(storage.DeserializeBloomFilterStats).Return([]*storage.PrimaryKeyStats{stats}, nil).Build())
			local := &qvLocalSegment{collection: guard}
			info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, Statslogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: path}}}}}
			// Run the real shared helper so moving the BF-disable check there
			// cannot silently bypass the real-PK exception.
			require.NoError(t, (realQVSegmentLoader{}).LoadPKCandidate(context.Background(), local, info))
			t.Cleanup(local.candidate.Refund)
			require.Equal(t, 1, read.Times())
			require.True(t, local.PkCandidateExist())
			require.Equal(t, []bool{true}, local.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{pk})))
		})
	}
}

func TestQVExternalVirtualPKKeepsEncodedRouting(t *testing.T) {
	patchNativeCollections(t)
	meta := collectionMetadata(1)
	meta.collection.Schema.ExternalSpec = `{"format":"milvus-table"}`
	meta.collection.Schema.Fields[0].Name = common.VirtualPKFieldName
	meta.collection.Schema.Fields = append(meta.collection.Schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "value", ExternalField: "value", DataType: schemapb.DataType_Int64})
	guard := acquireRuntime(t, newQueryViewCollectionRuntimeManager(meta), 1)
	t.Cleanup(guard.Release)
	load := mockey.Mock(segments.LoadSegmentBloomFilters).Return(nil, assert.AnError).Build()
	patchCollectionLifetime(t, load)
	local := &qvLocalSegment{collection: guard}
	require.NoError(t, (realQVSegmentLoader{}).LoadPKCandidate(context.Background(), local, &querypb.SegmentLoadInfo{SegmentID: 10}))
	require.Zero(t, load.Times())
	pks := []storage.PrimaryKey{storage.NewInt64PrimaryKey(10<<32 | 1), storage.NewInt64PrimaryKey(11<<32 | 1)}
	require.Equal(t, []bool{true, false}, local.BatchPkExist(storage.NewBatchLocationsCache(pks)))
}

func TestQVExternalDeltasReachSharedLoader(t *testing.T) {
	patchNativeCollections(t)
	meta := collectionMetadata(1)
	meta.collection.Schema.ExternalSpec = `{"format":"milvus-table"}`
	meta.collection.Schema.Fields[0].ExternalField = "pk"
	guard := acquireRuntime(t, newQueryViewCollectionRuntimeManager(meta), 1)
	t.Cleanup(guard.Release)
	local := &qvLocalSegment{collection: guard}
	info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10}
	load := mockey.Mock(segments.LoadSegmentDeltaLogs).To(func(_ context.Context, schema *schemapb.CollectionSchema, id int64, _ storage.ChunkManager, target segments.DeltaLoadTarget, actual *querypb.SegmentLoadInfo) error {
		assert.Same(t, guard.Schema(), schema)
		assert.EqualValues(t, 1, id)
		assert.Same(t, local, target)
		assert.Same(t, info, actual)
		return assert.AnError
	}).Build()
	patchCollectionLifetime(t, load)
	err := (realQVSegmentLoader{}).LoadDeltaLogs(context.Background(), local, info)
	require.ErrorIs(t, err, assert.AnError, "historical deletes cannot be silently skipped")
	require.Equal(t, 1, load.Times())
}
