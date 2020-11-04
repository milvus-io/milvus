package reader

import (
	"context"
	"log"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestMeta_GetCollectionObjId(t *testing.T) {
	conf.LoadConfig("config.yaml")

	var key = "/collection/collection0"
	var collectionObjId1 = GetCollectionObjId(key)

	assert.Equal(t, collectionObjId1, "/collection/collection0")

	key = "fakeKey"
	var collectionObjId2 = GetCollectionObjId(key)

	assert.Equal(t, collectionObjId2, "fakeKey")
}

func TestMeta_GetSegmentObjId(t *testing.T) {
	conf.LoadConfig("config.yaml")

	var key = "/segment/segment0"
	var segmentObjId1 = GetSegmentObjId(key)

	assert.Equal(t, segmentObjId1, "/segment/segment0")

	key = "fakeKey"
	var segmentObjId2 = GetSegmentObjId(key)

	assert.Equal(t, segmentObjId2, "fakeKey")
}

func TestMeta_isCollectionObj(t *testing.T) {
	conf.LoadConfig("config.yaml")

	var key = "by-dev/collection/collection0"
	var b1 = isCollectionObj(key)

	assert.Equal(t, b1, true)

	key = "by-dev/segment/segment0"
	var b2 = isCollectionObj(key)

	assert.Equal(t, b2, false)
}

func TestMeta_isSegmentObj(t *testing.T) {
	conf.LoadConfig("config.yaml")

	var key = "by-dev/segment/segment0"
	var b1 = isSegmentObj(key)

	assert.Equal(t, b1, true)

	key = "by-dev/collection/collection0"
	var b2 = isSegmentObj(key)

	assert.Equal(t, b2, false)
}

func TestMeta_isSegmentChannelRangeInQueryNodeChannelRange(t *testing.T) {
	conf.LoadConfig("config.yaml")

	var s = segment.Segment{
		SegmentID:      int64(0),
		CollectionID:   int64(0),
		PartitionTag:   "partition0",
		ChannelStart:   0,
		ChannelEnd:     128,
		OpenTimeStamp:  uint64(0),
		CloseTimeStamp: uint64(math.MaxUint64),
		CollectionName: "collection0",
		Rows:           int64(0),
	}

	var b = isSegmentChannelRangeInQueryNodeChannelRange(&s)
	assert.Equal(t, b, true)

	s = segment.Segment{
		SegmentID:      int64(0),
		CollectionID:   int64(0),
		PartitionTag:   "partition0",
		ChannelStart:   128,
		ChannelEnd:     256,
		OpenTimeStamp:  uint64(0),
		CloseTimeStamp: uint64(math.MaxUint64),
		CollectionName: "collection0",
		Rows:           int64(0),
	}

	b = isSegmentChannelRangeInQueryNodeChannelRange(&s)
	assert.Equal(t, b, false)
}

func TestMeta_PrintCollectionStruct(t *testing.T) {
	var age = collection.FieldMeta{
		FieldName: "age",
		Type:      schemapb.DataType_INT32,
		DIM:       int64(1),
	}

	var vec = collection.FieldMeta{
		FieldName: "vec",
		Type:      schemapb.DataType_VECTOR_FLOAT,
		DIM:       int64(16),
	}

	var fieldMetas = []collection.FieldMeta{age, vec}

	var c = collection.Collection{
		ID:         int64(0),
		Name:       "collection0",
		CreateTime: uint64(0),
		Schema:     fieldMetas,
		SegmentIDs: []int64{
			0, 1, 2,
		},
		PartitionTags: []string{
			"partition0",
		},
		GrpcMarshalString: "",
	}

	printCollectionStruct(&c)
}

func TestMeta_PrintSegmentStruct(t *testing.T) {
	var s = segment.Segment{
		SegmentID:      int64(0),
		CollectionID:   int64(0),
		PartitionTag:   "partition0",
		ChannelStart:   128,
		ChannelEnd:     256,
		OpenTimeStamp:  uint64(0),
		CloseTimeStamp: uint64(math.MaxUint64),
		CollectionName: "collection0",
		Rows:           int64(0),
	}

	printSegmentStruct(&s)
}

func TestMeta_ProcessCollectionCreate(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCollectionCreate(id, value)
	c := node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))
}

func TestMeta_ProcessSegmentCreate(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	c := node.NewCollection(int64(0), "test", "")
	c.NewPartition("default")

	node.processSegmentCreate(id, value)
	s := node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)
}

func TestMeta_ProcessCreate(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	key1 := "by-dev/collection/0"
	msg1 := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCreate(key1, msg1)
	c := node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	key2 := "by-dev/segment/0"
	msg2 := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	node.processCreate(key2, msg2)
	s := node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)
}

func TestMeta_ProcessSegmentModify(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	var c = node.NewCollection(int64(0), "test", "")
	c.NewPartition("default")

	node.processSegmentCreate(id, value)
	var s = node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)

	newValue := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177888," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	node.processSegmentModify(id, newValue)
	s = node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177888))
	assert.Equal(t, s.SegmentStatus, 0)
}

func TestMeta_ProcessCollectionModify(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCollectionCreate(id, value)
	var c = node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	// TODO: use different index for testing processCollectionModify
	newValue := "{\"id\":0,\"name\":\"test_new\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCollectionModify(id, newValue)
	c = node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))
}

func TestMeta_ProcessModify(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	key1 := "by-dev/collection/0"
	msg1 := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCreate(key1, msg1)
	c := node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	key2 := "by-dev/segment/0"
	msg2 := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	node.processCreate(key2, msg2)
	s := node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)

	// modify
	// TODO: use different index for testing processCollectionModify
	msg3 := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processModify(key1, msg3)
	c = node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	msg4 := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177888," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	node.processModify(key2, msg4)
	s = node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177888))
	assert.Equal(t, s.SegmentStatus, 0)
}

func TestMeta_ProcessSegmentDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	c := node.NewCollection(int64(0), "test", "")
	c.NewPartition("default")

	node.processSegmentCreate(id, value)
	s := node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)

	node.processSegmentDelete("0")
	mapSize := len(node.SegmentsMap)

	assert.Equal(t, mapSize, 0)
}

func TestMeta_ProcessCollectionDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	id := "0"
	value := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCollectionCreate(id, value)
	c := node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	node.processCollectionDelete(id)
	collectionsSize := len(node.Collections)

	assert.Equal(t, collectionsSize, 0)
}

func TestMeta_ProcessDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	key1 := "by-dev/collection/0"
	msg1 := "{\"id\":0,\"name\":\"test\",\"creat_time\":1603359905,\"schema\":" +
		"[{\"field_name\":\"age\",\"type\":4,\"dimension\":1}," +
		"{\"field_name\":\"field_vec\",\"type\":101,\"dimension\":512}]," +
		"\"segment_ids\":[6886378356295345384],\"partition_tags\":[\"default\"]," +
		"\"grpc_marshal_string\":\"id: 6886378356295345384\\nname: \\\"test\\\"\\nschema: \\u003c\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"age\\\"\\n    type: INT32\\n    dim: 1\\n  \\u003e\\n  " +
		"field_metas: \\u003c\\n    field_name: \\\"field_vec\\\"\\n    type: VECTOR_FLOAT\\n    " +
		"dim: 512\\n  \\u003e\\n\\u003e\\ncreate_time: 1603359905\\nsegment_ids: " +
		"6886378356295345384\\npartition_tags: \\\"default\\\"\\n\",\"index_param\":null}"

	node.processCreate(key1, msg1)
	c := node.Collections[0]

	assert.Equal(t, c.CollectionName, "test")
	assert.Equal(t, c.CollectionID, uint64(0))

	key2 := "by-dev/segment/0"
	msg2 := "{\"segment_id\":0,\"collection_id\":0," +
		"\"partition_tag\":\"default\",\"channel_start\":0,\"channel_end\":128," +
		"\"open_timestamp\":1603360439,\"close_timestamp\":70368744177663," +
		"\"collection_name\":\"test\",\"segment_status\":0,\"rows\":0}"

	node.processCreate(key2, msg2)
	s := node.SegmentsMap[int64(0)]

	assert.Equal(t, s.SegmentId, int64(0))
	assert.Equal(t, s.SegmentCloseTime, uint64(70368744177663))
	assert.Equal(t, s.SegmentStatus, 0)

	node.processDelete(key1)
	collectionsSize := len(node.Collections)

	assert.Equal(t, collectionsSize, 0)

	mapSize := len(node.SegmentsMap)

	assert.Equal(t, mapSize, 0)
}

func TestMeta_ProcessResp(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	err := node.InitFromMeta()
	assert.Nil(t, err)

	metaChan := node.kvBase.WatchWithPrefix("")

	select {
	case <-node.ctx.Done():
		return
	case resp := <-metaChan:
		_ = node.processResp(resp)
	}
}

func TestMeta_LoadCollections(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	err := node.InitFromMeta()
	assert.Nil(t, err)

	err2 := node.loadCollections()
	assert.Nil(t, err2)
}

func TestMeta_LoadSegments(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	err := node.InitFromMeta()
	assert.Nil(t, err)

	err2 := node.loadSegments()
	assert.Nil(t, err2)
}

func TestMeta_InitFromMeta(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	err := node.InitFromMeta()
	assert.Nil(t, err)
}

func TestMeta_RunMetaService(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	node := CreateQueryNode(ctx, 0, 0, nil)

	wg := sync.WaitGroup{}
	err := node.InitFromMeta()

	if err != nil {
		log.Printf("Init query node from meta failed")
		return
	}

	wg.Add(1)
	go node.RunMetaService(&wg)
	wg.Wait()

	node.Close()
}
