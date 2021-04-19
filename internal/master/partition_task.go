package master

import (
	"errors"

	"log"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

const partitionMetaPrefix = "partition/"

type createPartitionTask struct {
	baseTask
	req *milvuspb.CreatePartitionRequest
}

type dropPartitionTask struct {
	baseTask
	req *milvuspb.DropPartitionRequest
}

type hasPartitionTask struct {
	baseTask
	hasPartition bool
	req          *milvuspb.HasPartitionRequest
}

//type describePartitionTask struct {
//	baseTask
//	description *servicepb.PartitionDescription
//	req         *internalpb.DescribePartitionRequest
//}

type showPartitionTask struct {
	baseTask
	resp *milvuspb.ShowPartitionResponse
	req  *milvuspb.ShowPartitionRequest
}

//////////////////////////////////////////////////////////////////////////
func (t *createPartitionTask) Type() commonpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.Base.MsgType
}

func (t *createPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Base.Timestamp), nil
}

func (t *createPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := t.req.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	ts, err := t.Ts()
	if err != nil {
		return err
	}

	err = t.mt.AddPartition(collectionMeta.ID, partitionName)
	if err != nil {
		return err
	}

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}

	partitionMsg := internalpb2.CreatePartitionRequest{
		Base:           t.req.Base,
		DbName:         "",
		CollectionName: t.req.CollectionName,
		PartitionName:  t.req.PartitionName,
		DbID:           0, // todo add DbID
		CollectionID:   collectionMeta.ID,
		PartitionID:    0, // todo add partitionID
	}
	timeTickMsg := &ms.CreatePartitionMsg{
		BaseMsg:                baseMsg,
		CreatePartitionRequest: partitionMsg,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	return t.sch.ddMsgStream.Broadcast(&msgPack)

}

//////////////////////////////////////////////////////////////////////////
func (t *dropPartitionTask) Type() commonpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.Base.MsgType
}

func (t *dropPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return t.req.Base.Timestamp, nil
}

func (t *dropPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := t.req.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)

	if err != nil {
		return err
	}

	err = t.mt.DeletePartition(collectionMeta.ID, partitionName)
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

	dropMsg := internalpb2.DropPartitionRequest{
		Base:           t.req.Base,
		DbName:         "", // tod add DbName
		CollectionName: t.req.CollectionName,
		PartitionName:  t.req.PartitionName,
		DbID:           0, // todo add DbID
		CollectionID:   collectionMeta.ID,
		PartitionID:    0, // todo addd PartitionID
	}
	timeTickMsg := &ms.DropPartitionMsg{
		BaseMsg:              baseMsg,
		DropPartitionRequest: dropMsg,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	return t.sch.ddMsgStream.Broadcast(&msgPack)

}

//////////////////////////////////////////////////////////////////////////
func (t *hasPartitionTask) Type() commonpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.Base.MsgType
}

func (t *hasPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return t.req.Base.Timestamp, nil
}

func (t *hasPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := t.req.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	t.hasPartition = t.mt.HasPartition(collectionMeta.ID, partitionName)

	return nil

}

//////////////////////////////////////////////////////////////////////////
//func (t *describePartitionTask) Type() commonpb.MsgType {
//	if t.req == nil {
//		log.Printf("null request")
//		return 0
//	}
//	return t.req.MsgType
//}
//
//func (t *describePartitionTask) Ts() (Timestamp, error) {
//	if t.req == nil {
//		return 0, errors.New("null request")
//	}
//	return t.req.Timestamp, nil
//}
//
//func (t *describePartitionTask) Execute() error {
//	if t.req == nil {
//		return errors.New("null request")
//	}
//
//	partitionName := t.req.PartitionName
//
//	description := servicepb.PartitionDescription{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_SUCCESS,
//		},
//		Name:       partitionName,
//		Statistics: nil,
//	}
//
//	t.description = &description
//
//	return nil
//
//}

//////////////////////////////////////////////////////////////////////////
func (t *showPartitionTask) Type() commonpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.Base.MsgType
}

func (t *showPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return t.req.Base.Timestamp, nil
}

func (t *showPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	collMeta, err := t.mt.GetCollectionByName(t.req.CollectionName)
	if err != nil {
		return err
	}
	partitions := make([]string, 0)
	partitions = append(partitions, collMeta.PartitionTags...)

	stringListResponse := milvuspb.ShowPartitionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		PartitionNames: partitions,
	}

	t.resp = &stringListResponse

	return nil

}
