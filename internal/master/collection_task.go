package master

import (
	"encoding/json"
	"errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"log"
	"strconv"
)

const collectionMetaPrefix = "collection/"

type createCollectionTask struct {
	baseTask
	req    *internalpb.CreateCollectionRequest
}

type dropCollectionTask struct {
	baseTask
	req    *internalpb.DropCollectionRequest
}

func (t *createCollectionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *createCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *createCollectionTask) Execute() commonpb.Status {
	var schema schemapb.CollectionSchema
	err0 := json.Unmarshal(t.req.Schema.Value, &schema)
	if err0 != nil {
		t.Notify()
		return commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Unmarshal CollectionSchema failed",
		}
	}

	// TODO: allocate collection id
	var collectionId uint64 = 0
	// TODO: allocate timestamp
	var collectionCreateTime uint64 = 0

	collection := etcdpb.CollectionMeta{
		Id:         collectionId,
		Schema:     &schema,
		CreateTime: collectionCreateTime,
		// TODO: initial segment?
		SegmentIds: make([]uint64, 0),
		// TODO: initial partition?
		PartitionTags: make([]string, 0),
	}

	collectionJson, err1 := json.Marshal(&collection)
	if err1 != nil {
		t.Notify()
		return commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Marshal collection failed",
		}
	}

	err2 := (*t.kvBase).Save(collectionMetaPrefix+strconv.FormatUint(collectionId, 10), string(collectionJson))
	if err2 != nil {
		t.Notify()
		return commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "Save collection failed",
		}
	}

	t.Notify()
	return commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
}

func (t *dropCollectionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *dropCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *dropCollectionTask) Execute() commonpb.Status {
	return commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
}
