package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
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
