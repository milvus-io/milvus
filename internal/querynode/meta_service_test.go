package querynode

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

func TestMetaService_start(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	node.metaService.start()
	node.Close()
}

func TestMetaService_getCollectionObjId(t *testing.T) {
	var key = "/collection/collection0"
	var collectionObjID1 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID1, "/collection/collection0")

	key = "fakeKey"
	var collectionObjID2 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID2, "fakeKey")
}

func TestMetaService_getSegmentObjId(t *testing.T) {
	var key = "/segment/segment0"
	var segmentObjID1 = GetSegmentObjID(key)

	assert.Equal(t, segmentObjID1, "/segment/segment0")

	key = "fakeKey"
	var segmentObjID2 = GetSegmentObjID(key)

	assert.Equal(t, segmentObjID2, "fakeKey")
}

func TestMetaService_isCollectionObj(t *testing.T) {
	var key = Params.MetaRootPath + "/collection/collection0"
	var b1 = isCollectionObj(key)

	assert.Equal(t, b1, true)

	key = Params.MetaRootPath + "/segment/segment0"
	var b2 = isCollectionObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_isSegmentObj(t *testing.T) {
	var key = Params.MetaRootPath + "/segment/segment0"
	var b1 = isSegmentObj(key)

	assert.Equal(t, b1, true)

	key = Params.MetaRootPath + "/collection/collection0"
	var b2 = isSegmentObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_isSegmentChannelRangeInQueryNodeChannelRange(t *testing.T) {
	var s = etcdpb.SegmentMeta{
		SegmentID:    UniqueID(0),
		CollectionID: UniqueID(0),
		PartitionTag: "partition0",
		ChannelStart: 0,
		ChannelEnd:   1,
		OpenTime:     Timestamp(0),
		CloseTime:    Timestamp(math.MaxUint64),
		NumRows:      UniqueID(0),
	}

	var b = isSegmentChannelRangeInQueryNodeChannelRange(&s)
	assert.Equal(t, b, true)

	s = etcdpb.SegmentMeta{
		SegmentID:    UniqueID(0),
		CollectionID: UniqueID(0),
		PartitionTag: "partition0",
		ChannelStart: 128,
		ChannelEnd:   256,
		OpenTime:     Timestamp(0),
		CloseTime:    Timestamp(math.MaxUint64),
		NumRows:      UniqueID(0),
	}

	b = isSegmentChannelRangeInQueryNodeChannelRange(&s)
	assert.Equal(t, b, false)
}

func TestMetaService_printCollectionStruct(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)
	printCollectionStruct(collectionMeta)
}

func TestMetaService_printSegmentStruct(t *testing.T) {
	var s = etcdpb.SegmentMeta{
		SegmentID:    UniqueID(0),
		CollectionID: UniqueID(0),
		PartitionTag: "partition0",
		ChannelStart: 128,
		ChannelEnd:   256,
		OpenTime:     Timestamp(0),
		CloseTime:    Timestamp(math.MaxUint64),
		NumRows:      UniqueID(0),
	}

	printSegmentStruct(&s)
}

func TestMetaService_processCollectionCreate(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `schema: <
				name: "test"
				fields: <
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

	node.metaService.processCollectionCreate(id, value)

	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))
	node.Close()
}

func TestMetaService_processSegmentCreate(t *testing.T) {
	node := newQueryNode()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)

	s, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
	node.Close()
}

func TestMetaService_processCreate(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	key1 := Params.MetaRootPath + "/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
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

	(*node.metaService).processCreate(key1, msg1)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := Params.MetaRootPath + "/segment/0"
	msg2 := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	s, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
	node.Close()
}

func TestMetaService_processSegmentModify(t *testing.T) {
	node := newQueryNode()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	segmentID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, segmentID)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)
	s, err := node.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, segmentID)

	newValue := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	// TODO: modify segment for testing processCollectionModify
	(*node.metaService).processSegmentModify(id, newValue)
	seg, err := node.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, segmentID)
	node.Close()
}

func TestMetaService_processCollectionModify(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `schema: <
				name: "test"
				fields: <
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
				name: "age"
				data_type: INT32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				segmentIDs: 0
				partition_tags: "p0"
				partition_tags: "p1"
				partition_tags: "p2"
				`

	(*node.metaService).processCollectionCreate(id, value)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	partitionNum, err := node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 3)

	hasPartition := node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p3")
	assert.Equal(t, hasPartition, false)

	newValue := `schema: <
				name: "test"
				fields: <
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
				name: "age"
				data_type: INT32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				segmentIDs: 0
				partition_tags: "p1"
				partition_tags: "p2"
				partition_tags: "p3"
				`

	(*node.metaService).processCollectionModify(id, newValue)
	collection, err = node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	partitionNum, err = node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 3)

	hasPartition = node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, false)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p3")
	assert.Equal(t, hasPartition, true)
	node.Close()
}

func TestMetaService_processModify(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	key1 := Params.MetaRootPath + "/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
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
				name: "age"
				data_type: INT32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				segmentIDs: 0
				partition_tags: "p0"
				partition_tags: "p1"
				partition_tags: "p2"
				`

	(*node.metaService).processCreate(key1, msg1)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	partitionNum, err := node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 3)

	hasPartition := node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p3")
	assert.Equal(t, hasPartition, false)

	key2 := Params.MetaRootPath + "/segment/0"
	msg2 := `partition_tag: "p1"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	s, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))

	// modify
	// TODO: use different index for testing processCollectionModify
	msg3 := `schema: <
				name: "test"
				fields: <
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
				name: "age"
				data_type: INT32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				segmentIDs: 0
				partition_tags: "p1"
				partition_tags: "p2"
				partition_tags: "p3"
				`

	(*node.metaService).processModify(key1, msg3)
	collection, err = node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	partitionNum, err = node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 3)

	hasPartition = node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, false)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p3")
	assert.Equal(t, hasPartition, true)

	msg4 := `partition_tag: "p1"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processModify(key2, msg4)
	seg, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))
	node.Close()
}

func TestMetaService_processSegmentDelete(t *testing.T) {
	node := newQueryNode()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)
	seg, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))

	(*node.metaService).processSegmentDelete("0")
	mapSize := node.replica.getSegmentNum()
	assert.Equal(t, mapSize, 0)
	node.Close()
}

func TestMetaService_processCollectionDelete(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `schema: <
				name: "test"
				fields: <
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

	(*node.metaService).processCollectionCreate(id, value)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	(*node.metaService).processCollectionDelete(id)
	collectionNum = node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 0)
	node.Close()
}

func TestMetaService_processDelete(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	key1 := Params.MetaRootPath + "/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
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

	(*node.metaService).processCreate(key1, msg1)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := Params.MetaRootPath + "/segment/0"
	msg2 := `partition_tag: "default"
				channel_start: 0
				channel_end: 1
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	seg, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))

	(*node.metaService).processDelete(key1)
	collectionsSize := node.replica.getCollectionNum()
	assert.Equal(t, collectionsSize, 0)

	mapSize := node.replica.getSegmentNum()
	assert.Equal(t, mapSize, 0)
	node.Close()
}

func TestMetaService_processResp(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	metaChan := (*node.metaService).kvBase.WatchWithPrefix("")

	select {
	case <-node.queryNodeLoopCtx.Done():
		return
	case resp := <-metaChan:
		_ = (*node.metaService).processResp(resp)
	}
	node.Close()
}

func TestMetaService_loadCollections(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	err2 := (*node.metaService).loadCollections()
	assert.Nil(t, err2)
	node.Close()
}

func TestMetaService_loadSegments(t *testing.T) {
	node := newQueryNode()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	err2 := (*node.metaService).loadSegments()
	assert.Nil(t, err2)
	node.Close()
}
