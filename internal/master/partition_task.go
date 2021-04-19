package master

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

const partitionMetaPrefix = "partition/"

type createPartitionTask struct {
	baseTask
	req *internalpb.CreatePartitionRequest
}

type dropPartitionTask struct {
	baseTask
	req *internalpb.DropPartitionRequest
}

type hasPartitionTask struct {
	baseTask
	hasPartition bool
	req          *internalpb.HasPartitionRequest
}

type describePartitionTask struct {
	baseTask
	description *servicepb.PartitionDescription
	req         *internalpb.DescribePartitionRequest
}

type showPartitionTask struct {
	baseTask
	stringListResponse *servicepb.StringListResponse
	req                *internalpb.ShowPartitionRequest
}

//////////////////////////////////////////////////////////////////////////
func (t *createPartitionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *createPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *createPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	collectionMeta.PartitionTags = append(collectionMeta.PartitionTags, partitionName.Tag)

	collectionJSON, err := json.Marshal(&collectionMeta)
	if err != nil {
		return err
	}

	collectionID := collectionMeta.Id
	err = (*t.kvBase).Save(partitionMetaPrefix+strconv.FormatInt(collectionID, 10), string(collectionJSON))
	if err != nil {
		return err
	}

	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *dropPartitionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *dropPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *dropPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	err = t.mt.DeletePartition(collectionMeta.Id, partitionName.Tag)
	if err != nil {
		return err
	}

	collectionJSON, err := json.Marshal(&collectionMeta)
	if err != nil {
		return err
	}

	collectionID := collectionMeta.Id
	err = (*t.kvBase).Save(partitionMetaPrefix+strconv.FormatInt(collectionID, 10), string(collectionJSON))
	if err != nil {
		return err
	}

	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *hasPartitionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *hasPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *hasPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		return err
	}

	t.hasPartition = t.mt.HasPartition(collectionMeta.Id, partitionName.Tag)

	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *describePartitionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *describePartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *describePartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName

	description := servicepb.PartitionDescription{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Name: partitionName,
	}

	t.description = &description

	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *showPartitionTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *showPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *showPartitionTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	partitions := make([]string, 0)
	for _, collection := range t.mt.collID2Meta {
		partitions = append(partitions, collection.PartitionTags...)
	}

	stringListResponse := servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Values: partitions,
	}

	t.stringListResponse = &stringListResponse

	return nil
}
