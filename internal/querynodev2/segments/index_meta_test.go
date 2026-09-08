package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
)

func TestComposeIndexMeta(t *testing.T) {
	ctx := context.Background()
	schema := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexInfos := mock_segcore.GenTestIndexInfoList(1, schema)

	meta := ComposeIndexMeta(ctx, indexInfos, schema)
	require.NotNil(t, meta)
	require.NotEmpty(t, meta.GetIndexMetas())
	require.Greater(t, meta.GetMaxIndexRowCount(), int64(0))
}

func TestComposeIndexMetaSanitizesExecutableParams(t *testing.T) {
	ctx := context.Background()
	schema := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexInfos := []*indexpb.IndexInfo{{
		FieldID: 100,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
			{Key: common.MmapEnabledKey, Value: "true"},
			{Key: common.WarmupKey, Value: common.WarmupSync},
			{Key: common.EvictableKey, Value: "false"},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.IndexOffsetCacheEnabledKey, Value: "true"},
			{Key: common.WarmupKey, Value: common.WarmupSync},
			{Key: common.EvictableKey, Value: "false"},
		},
		UserIndexParams: []*commonpb.KeyValuePair{{Key: common.EvictableKey, Value: "false"}},
	}}

	meta := ComposeIndexMeta(ctx, indexInfos, schema)

	require.Len(t, meta.GetIndexMetas(), 1)
	output := meta.GetIndexMetas()[0]
	for _, param := range output.GetTypeParams() {
		assert.False(t, indexparams.IsConfigableIndexParam(param.GetKey()))
	}
	for _, param := range output.GetIndexParams() {
		assert.False(t, indexparams.IsConfigableIndexParam(param.GetKey()))
	}
	value, hasUserValue := common.IsEvictableEnabled(output.GetUserIndexParams()...)
	require.True(t, hasUserValue)
	assert.False(t, value)

	_, inputHasTypeValue := common.IsEvictableEnabled(indexInfos[0].GetTypeParams()...)
	assert.True(t, inputHasTypeValue)
	_, inputHasIndexValue := common.IsEvictableEnabled(indexInfos[0].GetIndexParams()...)
	assert.True(t, inputHasIndexValue)
}
