package segcore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func TestQueryViewCollectionBindingsShareSchemaLifetime(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "shared-schema", Version: 7,
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}},
	}
	req := &CreateCCollectionRequest{CollectionID: 778899, Schema: schema, ShareSchema: true}
	a, err := CreateCCollection(req)
	require.NoError(t, err)
	key := sharedSchemaKey{req.CollectionID, schema.Version}
	readCollectionSchemas.Lock()
	owner := readCollectionSchemas.entries[key].collection
	readCollectionSchemas.Unlock()
	req.IndexMeta = &segcorepb.CollectionIndexMeta{MaxIndexRowCount: 100}
	b, err := CreateCCollection(req)
	require.NoError(t, err)
	require.NotEqual(t, a.ptr, b.ptr, "bindings own distinct native handles")
	require.Nil(t, a.IndexMeta())
	require.EqualValues(t, 100, b.IndexMeta().MaxIndexRowCount)
	readCollectionSchemas.Lock()
	require.Same(t, owner, readCollectionSchemas.entries[key].collection, "only one parsed schema owner")
	require.Equal(t, 2, readCollectionSchemas.entries[key].refs)
	readCollectionSchemas.Unlock()
	a.Release()
	require.EqualValues(t, 7, b.Schema().Version)
	readCollectionSchemas.Lock()
	require.Equal(t, 1, readCollectionSchemas.entries[key].refs)
	readCollectionSchemas.Unlock()
	require.Error(t, b.UpdateSchema(&schemapb.CollectionSchema{Version: 8}, 8), "published schema cannot change")
	b.Release()
	readCollectionSchemas.Lock()
	_, exists := readCollectionSchemas.entries[key]
	readCollectionSchemas.Unlock()
	require.False(t, exists, "last binding releases registry ownership")
}
