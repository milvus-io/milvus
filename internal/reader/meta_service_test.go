package reader

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestMetaService_start(t *testing.T) {
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// init query node
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	(*node.metaService).start()
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
	var key = "by-dev/collection/collection0"
	var b1 = isCollectionObj(key)

	assert.Equal(t, b1, true)

	key = "by-dev/segment/segment0"
	var b2 = isCollectionObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_isSegmentObj(t *testing.T) {
	var key = "by-dev/segment/segment0"
	var b1 = isSegmentObj(key)

	assert.Equal(t, b1, true)

	key = "by-dev/collection/collection0"
	var b2 = isSegmentObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_isSegmentChannelRangeInQueryNodeChannelRange(t *testing.T) {
	var s = etcdpb.SegmentMeta{
		SegmentID:    UniqueID(0),
		CollectionID: UniqueID(0),
		PartitionTag: "partition0",
		ChannelStart: 0,
		ChannelEnd:   128,
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
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	printCollectionStruct(&collectionMeta)
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
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

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

	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))
}

func TestMetaService_processSegmentCreate(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	collectionName := "collection0"
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	colMetaBlob, err := proto.Marshal(&collectionMeta)
	assert.NoError(t, err)

	err = (*node.replica).addCollection(&collectionMeta, string(colMetaBlob))
	assert.NoError(t, err)

	err = (*node.replica).addPartition(UniqueID(0), "default")
	assert.NoError(t, err)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)

	s, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
}

func TestMetaService_processCreate(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	key1 := "by-dev/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
				name: "vec"
				data_type: VECTOR_FLOAT
				type_params: <
				  key: "dim"
				  value: "16"
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
	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := "by-dev/segment/0"
	msg2 := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	s, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
}

func TestMetaService_processSegmentModify(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	collectionName := "collection0"
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	colMetaBlob, err := proto.Marshal(&collectionMeta)
	assert.NoError(t, err)

	err = (*node.replica).addCollection(&collectionMeta, string(colMetaBlob))
	assert.NoError(t, err)

	err = (*node.replica).addPartition(UniqueID(0), "default")
	assert.NoError(t, err)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)
	s, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))

	newValue := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	// TODO: modify segment for testing processCollectionModify
	(*node.metaService).processSegmentModify(id, newValue)
	seg, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))
}

func TestMetaService_processCollectionModify(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

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
	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	// TODO: use different index for testing processCollectionModify
	newValue := `schema: <
				name: "test"
				fields: <
				name: "vec"
				data_type: VECTOR_FLOAT
				type_params: <
				  key: "dim"
				  value: "16"
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

	(*node.metaService).processCollectionModify(id, newValue)
	collection, err = (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))
}

func TestMetaService_processModify(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	key1 := "by-dev/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
				name: "vec"
				data_type: VECTOR_FLOAT
				type_params: <
				  key: "dim"
				  value: "16"
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
	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := "by-dev/segment/0"
	msg2 := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	s, err := (*node.replica).getSegmentByID(UniqueID(0))
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

	(*node.metaService).processModify(key1, msg3)
	collection, err = (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	msg4 := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	// TODO: modify segment for testing processCollectionModify
	(*node.metaService).processModify(key2, msg4)
	seg, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))
}

func TestMetaService_processSegmentDelete(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	collectionName := "collection0"
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	colMetaBlob, err := proto.Marshal(&collectionMeta)
	assert.NoError(t, err)

	err = (*node.replica).addCollection(&collectionMeta, string(colMetaBlob))
	assert.NoError(t, err)

	err = (*node.replica).addPartition(UniqueID(0), "default")
	assert.NoError(t, err)

	id := "0"
	value := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processSegmentCreate(id, value)
	seg, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))

	(*node.metaService).processSegmentDelete("0")
	mapSize := (*node.replica).getSegmentNum()
	assert.Equal(t, mapSize, 0)
}

func TestMetaService_processCollectionDelete(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

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
	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	(*node.metaService).processCollectionDelete(id)
	collectionNum = (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 0)
}

func TestMetaService_processDelete(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	key1 := "by-dev/collection/0"
	msg1 := `schema: <
				name: "test"
				fields: <
				name: "vec"
				data_type: VECTOR_FLOAT
				type_params: <
				  key: "dim"
				  value: "16"
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
	collectionNum := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := (*node.replica).getCollectionByName("test")
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := "by-dev/segment/0"
	msg2 := `partition_tag: "default"
				channel_start: 0
				channel_end: 128
				close_time: 18446744073709551615
				`

	(*node.metaService).processCreate(key2, msg2)
	seg, err := (*node.replica).getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, seg.segmentID, UniqueID(0))

	(*node.metaService).processDelete(key1)
	collectionsSize := (*node.replica).getCollectionNum()
	assert.Equal(t, collectionsSize, 0)

	mapSize := (*node.replica).getSegmentNum()
	assert.Equal(t, mapSize, 0)
}

func TestMetaService_processResp(t *testing.T) {
	var ctx context.Context
	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	metaChan := (*node.metaService).kvBase.WatchWithPrefix("")

	select {
	case <-node.ctx.Done():
		return
	case resp := <-metaChan:
		_ = (*node.metaService).processResp(resp)
	}
}

func TestMetaService_loadCollections(t *testing.T) {
	var ctx context.Context
	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	err2 := (*node.metaService).loadCollections()
	assert.Nil(t, err2)
}

func TestMetaService_loadSegments(t *testing.T) {
	var ctx context.Context
	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// init metaService
	node := NewQueryNode(ctx, 0)
	node.metaService = newMetaService(ctx, node.replica)

	err2 := (*node.metaService).loadSegments()
	assert.Nil(t, err2)
}
