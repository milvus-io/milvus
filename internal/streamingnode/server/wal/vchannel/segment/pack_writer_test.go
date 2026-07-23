package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCurrentSplitForGrowingPackFillsNewSplitFormats(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() {
		_ = params.Reset(params.DataNodeCfg.StorageFormat.Key)
	})

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	meta := &streamingpb.SegmentAssignmentMeta{StorageVersion: storage.StorageV3}

	columnGroups := currentSplitForGrowingPack(schema, nil, meta)

	require.NotEmpty(t, columnGroups)
	for _, columnGroup := range columnGroups {
		assert.Equal(t, "parquet", columnGroup.Format)
	}
}

func TestCurrentSplitFromPersistedStorageRestoresFormat(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	persisted := &streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{{
			FieldBinlog: []*datapb.FieldBinlog{{
				FieldID:     101,
				ChildFields: []int64{101},
				Format:      "vortex",
			}},
		}},
	}

	columnGroups := currentSplitFromPersistedStorage(schema, persisted)

	require.Len(t, columnGroups, 1)
	assert.Equal(t, int64(101), columnGroups[0].GroupID)
	assert.Equal(t, []int64{101}, columnGroups[0].Fields)
	assert.Equal(t, []int{1}, columnGroups[0].Columns)
	assert.Equal(t, "vortex", columnGroups[0].Format)
}
