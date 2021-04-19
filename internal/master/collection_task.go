package master

import (
	"errors"
	"log"

	"github.com/golang/protobuf/proto"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

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
	return t.req.Timestamp, nil
}

func (t *createCollectionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	var schema schemapb.CollectionSchema
	err := proto.UnmarshalMerge(t.req.Schema.Value, &schema)
	if err != nil {
		return err
	}

	collectionID, err := t.sch.globalIDAllocator()
	if err != nil {
		return err
	}

	ts, err := t.Ts()
	if err != nil {
		return err
	}

	collection := etcdpb.CollectionMeta{
		ID:         collectionID,
		Schema:     &schema,
		CreateTime: ts,
		// TODO: initial segment?
		SegmentIDs: make([]UniqueID, 0),
		// TODO: initial partition?
		PartitionTags: make([]string, 0),
	}
	err = t.mt.AddCollection(&collection)
	if err != nil {
		return err
	}

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		BeginTimestamp: t.req.Timestamp,
		EndTimestamp:   t.req.Timestamp,
		HashValues:     []uint32{0},
	}
	timeTickMsg := &ms.CreateCollectionMsg{
		BaseMsg:                 baseMsg,
		CreateCollectionRequest: *t.req,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	return t.sch.ddMsgStream.Broadcast(&msgPack)
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
	return t.req.Timestamp, nil
}

func (t *dropCollectionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	collectionID := collectionMeta.ID

	err = t.mt.DeleteCollection(collectionID)
	if err != nil {
		return err
	}

	ts, err := t.Ts()
	if err != nil {
		return err
	}

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	timeTickMsg := &ms.DropCollectionMsg{
		BaseMsg:               baseMsg,
		DropCollectionRequest: *t.req,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	return t.sch.ddMsgStream.Broadcast(&msgPack)

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
	return t.req.Timestamp, nil
}

func (t *hasCollectionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName.CollectionName
	_, err := t.mt.GetCollectionByName(collectionName)
	if err == nil {
		t.hasCollection = true
	}
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
	return t.req.Timestamp, nil
}

func (t *describeCollectionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	collectionName := t.req.CollectionName
	collection, err := t.mt.GetCollectionByName(collectionName.CollectionName)
	if err != nil {
		return err
	}

	t.description.Schema = collection.Schema

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
	return t.req.Timestamp, nil
}

func (t *showCollectionsTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	colls, err := t.mt.ListCollections()
	if err != nil {
		return err
	}

	t.stringListResponse.Values = colls

	return nil
}
