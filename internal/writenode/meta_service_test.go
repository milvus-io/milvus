package writenode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaService_start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newReplica()

	metaService := newMetaService(ctx, replica)

	metaService.start()
}

func TestMetaService_getCollectionObjId(t *testing.T) {
	var key = "/collection/collection0"
	var collectionObjID1 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID1, "/collection/collection0")

	key = "fakeKey"
	var collectionObjID2 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID2, "fakeKey")
}

func TestMetaService_isCollectionObj(t *testing.T) {
	var key = Params.MetaRootPath + "/collection/collection0"
	var b1 = isCollectionObj(key)

	assert.Equal(t, b1, true)

	key = Params.MetaRootPath + "/segment/segment0"
	var b2 = isCollectionObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_processCollectionCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	replica := newReplica()
	metaService := newMetaService(ctx, replica)
	defer cancel()
	id := "0"
	value := `schema: <
				name: "test"
				fields: <
				fieldID:100
				name: "vec"
				data_type: VECTOR_FLOAT
				type_params: <
				  key: "dim"
				  value: "16"
				>
				index_params: <
				  key: "metric_type"
				  value: "L2"
				>
				>
				fields: <
				fieldID:101
				name: "age"
				data_type: INT32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				segmentIDs: 0
				partition_tags: "default"
				`

	metaService.processCollectionCreate(id, value)

	collectionNum := replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))
}

func TestMetaService_loadCollections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newReplica()

	metaService := newMetaService(ctx, replica)

	err2 := (*metaService).loadCollections()
	assert.Nil(t, err2)
}
