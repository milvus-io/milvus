package segcore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestCollection(t *testing.T) {
	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexMeta := mock_segcore.GenTestIndexMeta(1, schema)
	ccollection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: 1,
		Schema:       schema,
		IndexMeta:    indexMeta,
	})
	assert.NoError(t, err)
	assert.NotNil(t, ccollection)
	assert.NotNil(t, ccollection.Schema())
	assert.NotNil(t, ccollection.IndexMeta())
	assert.Equal(t, int64(1), ccollection.ID())
	defer ccollection.Release()
}

func TestUpdateSchema(t *testing.T) {
	paramtable.Init()
	schema1 := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexMeta := mock_segcore.GenTestIndexMeta(1, schema1)
	ccollection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: 1,
		Schema:       schema1,
		IndexMeta:    indexMeta,
	})
	assert.NoError(t, err)

	schema2 := mock_segcore.GenTestBM25CollectionSchema("bm25")
	err = ccollection.UpdateSchema(schema2, 2)
	assert.NoError(t, err)
	assert.Equal(t, ccollection.Schema(), schema2)
}

func TestIndexMeta(t *testing.T) {
	paramtable.Init()
	schema1 := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexMeta1 := mock_segcore.GenTestIndexMeta(1, schema1)
	ccollection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: 1,
		Schema:       schema1,
		IndexMeta:    indexMeta1,
	})
	assert.NoError(t, err)

	indexMeta2 := mock_segcore.GenTestIndexMeta(1, schema1)
	indexMeta2.IndexMetas = indexMeta2.IndexMetas[:1]
	err = ccollection.UpdateIndexMeta(indexMeta2)
	assert.NoError(t, err)
	assert.Equal(t, ccollection.IndexMeta(), indexMeta2)
}
