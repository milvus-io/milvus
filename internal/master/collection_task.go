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

	for index, singleFiled := range schema.Fields {
		singleFiled.FieldID = int64(index + 100)
	}

	zeroField := &schemapb.FieldSchema{
		FieldID:      int64(0),
		Name:         "RowID",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT64,
	}

	oneField := &schemapb.FieldSchema{
		FieldID:      int64(1),
		Name:         "Timestamp",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT64,
	}

	schema.Fields = append(schema.Fields, zeroField, oneField)

	collectionID, err := t.sch.globalIDAllocator()
	if err != nil {
		return err
	}

	ts, err := t.Ts()
	if err != nil {
		return err
	}

	collection := etcdpb.CollectionMeta{
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    ts,
		SegmentIDs:    make([]UniqueID, 0),
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

	t.req.CollectionID = collectionID
	t.req.Schema.Value, err = proto.Marshal(&schema)
	if err != nil {
		return err
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

	t.req.CollectionID = collectionID
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

func (t *describeCollectionTask) filterSchema() error {
	// remove system field
	var newFields []*schemapb.FieldSchema
	for _, fieldMeta := range t.description.Schema.Fields {
		fieldID := fieldMeta.FieldID
		// todo not hardcode
		if fieldID < 100 {
			continue
		}
		newFields = append(newFields, fieldMeta)
	}
	t.description.Schema.Fields = newFields
	return nil
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
	cloneSchema := proto.Clone(collection.Schema)
	t.description.Schema = cloneSchema.(*schemapb.CollectionSchema)
	return t.filterSchema()

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
