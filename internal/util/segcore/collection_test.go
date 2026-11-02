package segcore_test

import (
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// The WAL ordering token can differ from the logical SchemaVersion. The Go
// snapshot must follow successful native updates and reject stale completions.
func TestCollectionSchemaFollowsNativeUpdateOrder(t *testing.T) {
	paramtable.Init()
	original := mock_segcore.GenTestCollectionSchema("schema-order", schemapb.DataType_Int64, false)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{Schema: original})
	require.NoError(t, err)
	defer collection.Release()
	updated := proto.Clone(original).(*schemapb.CollectionSchema)
	updated.Version = 3
	updated.Fields = append(updated.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
	require.NoError(t, collection.UpdateSchema(updated, 100))
	require.Same(t, updated, collection.Schema())
	require.NoError(t, collection.UpdateSchema(original, 99))
	require.NoError(t, collection.UpdateSchema(original, 100))
	require.Same(t, updated, collection.Schema())
	require.Error(t, collection.UpdateSchema(nil, 101))
	require.Same(t, updated, collection.Schema())

	invalid := proto.Clone(updated).(*schemapb.CollectionSchema)
	invalid.Name = string([]byte{0xff}) // protobuf rejects invalid UTF-8 before native mutation
	require.Error(t, collection.UpdateSchema(invalid, 101))
	require.Same(t, updated, collection.Schema())

	next := proto.Clone(updated).(*schemapb.CollectionSchema)
	next.Version = 4
	nativeErr := merr.WrapErrServiceNotReadyMsg("native schema update failed")
	patch := mockey.Mock(segcore.ConsumeCStatusIntoError).Return(nativeErr).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, collection.UpdateSchema(next, 101), nativeErr)
	require.Same(t, updated, collection.Schema(), "a failed native update must not publish a Go snapshot")
	patch.UnPatch()
	require.NoError(t, collection.UpdateSchema(next, 101))
	require.Same(t, next, collection.Schema())

	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 100 {
				assert.NotNil(t, collection.Schema())
			}
		})
	}
	for tick := uint64(102); tick < 110; tick++ {
		require.NoError(t, collection.UpdateSchema(next, tick))
	}
	readers.Wait()
}
