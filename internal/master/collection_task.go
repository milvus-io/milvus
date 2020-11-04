package master

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

const collectionMetaPrefix = "collection/"

type createCollectionTask struct {
	baseTask
	req *internalpb.CreateCollectionRequest
}

type dropCollectionTask struct {
	baseTask
	req *internalpb.DropCollectionRequest
}

type hasCollectionTask struct {
	baseTask
	hasCollection bool
	req           *internalpb.HasCollectionRequest
}

type describeCollectionTask struct {
	baseTask
	description *servicepb.CollectionDescription
	req         *internalpb.DescribeCollectionRequest
}

type showCollectionsTask struct {
	baseTask
	stringListResponse *servicepb.StringListResponse
	req                *internalpb.ShowCollectionRequest
}

//////////////////////////////////////////////////////////////////////////
func (t *createCollectionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *createCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *createCollectionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	var schema schemapb.CollectionSchema
	err := json.Unmarshal(t.req.Schema.Value, &schema)
	if err != nil {
		_ = t.Notify()
		return errors.New("unmarshal CollectionSchema failed")
	}

	// TODO: allocate collection id
	var collectionId int64 = 0
	// TODO: allocate timestamp
	var collectionCreateTime uint64 = 0

	collection := etcdpb.CollectionMeta{
		Id:         collectionId,
		Schema:     &schema,
		CreateTime: collectionCreateTime,
		// TODO: initial segment?
		SegmentIds: make([]int64, 0),
		// TODO: initial partition?
		PartitionTags: make([]string, 0),
	}

	collectionJson, err := json.Marshal(&collection)
	if err != nil {
		_ = t.Notify()
		return errors.New("marshal collection failed")
	}

	err = (*t.kvBase).Save(collectionMetaPrefix+strconv.FormatInt(collectionId, 10), string(collectionJson))
	if err != nil {
		_ = t.Notify()
		return errors.New("save collection failed")
	}

	t.mt.collId2Meta[collectionId] = collection

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *dropCollectionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *dropCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *dropCollectionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		_ = t.Notify()
		return err
	}

	collectionId := collectionMeta.Id

	err = (*t.kvBase).Remove(collectionMetaPrefix + strconv.FormatInt(collectionId, 10))
	if err != nil {
		_ = t.Notify()
		return errors.New("save collection failed")
	}

	delete(t.mt.collId2Meta, collectionId)

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *hasCollectionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *hasCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *hasCollectionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName.CollectionName
	_, err := t.mt.GetCollectionByName(collectionName)
	if err == nil {
		t.hasCollection = true
	}

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *describeCollectionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *describeCollectionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *describeCollectionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName
	collection, err := t.mt.GetCollectionByName(collectionName.CollectionName)
	if err != nil {
		_ = t.Notify()
		return err
	}

	description := servicepb.CollectionDescription{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Schema: collection.Schema,
	}

	t.description = &description

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *showCollectionsTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *showCollectionsTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *showCollectionsTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	collections := make([]string, 0)
	for _, collection := range t.mt.collId2Meta {
		collections = append(collections, collection.Schema.Name)
	}

	stringListResponse := servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Values: collections,
	}

	t.stringListResponse = &stringListResponse

	_ = t.Notify()
	return nil
}
