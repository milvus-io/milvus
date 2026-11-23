package qnview

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestResolveQueryViewManifestIndexesUsesSelectedRevisionAndDefinition(t *testing.T) {
	root := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := filepath.Join(root, "insert_log/1/2/3")
	pathVersion := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	path, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), config, &packed.ManifestUpdates{
		Indexes: []packed.ManifestIndexInfo{{
			ColumnName: "100", IndexName: "vector", IndexType: "HNSW", FieldID: 100, IndexID: 5,
			BuildID: 30, IndexVersion: 4, Path: "30/4", NumRows: 2000, MemSize: 1024,
			IndexFileKeys: []string{"index.bin"}, IndexStorePathVersion: pathVersion,
		}},
	})
	require.NoError(t, err)
	manifest, err := packed.OpenLoadManifest(t.Context(), path, config)
	require.NoError(t, err)
	t.Cleanup(manifest.Close)

	info := &querypb.SegmentLoadInfo{CollectionID: 1, PartitionID: 2, SegmentID: 3, ManifestPath: path, NumOfRows: 2100}
	definitions := map[int64]*indexpb.IndexInfo{
		5: {
			IndexID: 5, FieldID: 100,
			IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: "ef", Value: "60"}},
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		},
		6: {IndexID: 6, FieldID: 101},
		7: {IndexID: 7, FieldID: 102},
	}
	metadata := &querypb.QueryViewManifestIndexes{
		Summaries: []*datapb.SegmentIndexSummary{
			{IndexId: 5, FieldId: 100, BuildId: 30, IndexVersion: 4, HasArtifact: true},
			{IndexId: 6, FieldId: 101, BuildId: 31, IndexVersion: 4, HasArtifact: true},
			{IndexId: 7, FieldId: 102, BuildId: 32},
			{IndexId: 8, FieldId: 103, BuildId: 33, IndexVersion: 4, HasArtifact: true},
		},
		FinishedTasks: []*indexpb.IndexFilePathInfo{
			{SegmentID: 3, IndexID: 6, FieldID: 101, BuildID: 31, IndexVersion: 4, IndexFilePaths: []string{"task-index"}},
		},
	}
	result, err := resolveQueryViewManifestIndexes(info, metadata, definitions, manifest, root)
	require.NoError(t, err)
	require.Len(t, result, 3)
	indexes := make(map[int64]*querypb.FieldIndexInfo)
	for _, idx := range result {
		indexes[idx.GetIndexID()] = idx
	}
	require.Equal(t, []string{filepath.Join(base, "_index/30/4/index.bin")}, indexes[5].GetIndexFilePaths())
	require.EqualValues(t, 2000, indexes[5].GetNumRows())
	require.EqualValues(t, 1024, indexes[5].GetIndexSize())
	params := funcutil.KeyValuePair2Map(indexes[5].GetIndexParams())
	require.Equal(t, "HNSW", params[common.IndexTypeKey])
	require.Equal(t, "60", params["ef"])
	require.Equal(t, "128", params[common.DimKey])
	require.Equal(t, "FLAT", definitions[5].GetIndexParams()[0].GetValue())
	require.Equal(t, []string{"task-index"}, indexes[6].GetIndexFilePaths())
	require.Empty(t, indexes[7].GetIndexFilePaths())

	bad := proto.CloneOf(metadata)
	bad.Summaries[0].IndexVersion++
	_, err = resolveQueryViewManifestIndexes(info, bad, definitions, manifest, root)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}
