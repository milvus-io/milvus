package master

import (
	"encoding/json"
	"errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"log"
	"strconv"
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
func (t *createPartitionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *createPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *createPartitionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		_ = t.Notify()
		return err
	}

	collectionMeta.PartitionTags = append(collectionMeta.PartitionTags, partitionName.Tag)

	collectionJson, err := json.Marshal(&collectionMeta)
	if err != nil {
		_ = t.Notify()
		return errors.New("marshal collection failed")
	}

	collectionId := collectionMeta.Id
	err = (*t.kvBase).Save(partitionMetaPrefix+strconv.FormatInt(collectionId, 10), string(collectionJson))
	if err != nil {
		_ = t.Notify()
		return errors.New("save collection failed")
	}

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *dropPartitionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *dropPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *dropPartitionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	collectionMeta, err := t.mt.GetCollectionByName(collectionName)
	if err != nil {
		_ = t.Notify()
		return err
	}

	err = t.mt.DeletePartition(partitionName.Tag, collectionName)
	if err != nil {
		return err
	}

	collectionJson, err := json.Marshal(&collectionMeta)
	if err != nil {
		_ = t.Notify()
		return errors.New("marshal collection failed")
	}

	collectionId := collectionMeta.Id
	err = (*t.kvBase).Save(partitionMetaPrefix+strconv.FormatInt(collectionId, 10), string(collectionJson))
	if err != nil {
		_ = t.Notify()
		return errors.New("save collection failed")
	}

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *hasPartitionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *hasPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *hasPartitionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName
	collectionName := partitionName.CollectionName
	t.hasPartition = t.mt.HasPartition(partitionName.Tag, collectionName)

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *describePartitionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *describePartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *describePartitionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	partitionName := t.req.PartitionName

	description := servicepb.PartitionDescription {
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Name: partitionName,
	}

	t.description = &description

	_ = t.Notify()
	return nil
}

//////////////////////////////////////////////////////////////////////////
func (t *showPartitionTask) Type() internalpb.ReqType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.ReqType
}

func (t *showPartitionTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return Timestamp(t.req.Timestamp), nil
}

func (t *showPartitionTask) Execute() error {
	if t.req == nil {
		_ = t.Notify()
		return errors.New("null request")
	}

	partitions := make([]string, 0)
	for _, collection := range t.mt.collId2Meta {
		for _, partition := range collection.PartitionTags {
			partitions = append(partitions, partition)
		}
	}

	stringListResponse := servicepb.StringListResponse {
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Values: partitions,
	}

	t.stringListResponse = &stringListResponse

	_ = t.Notify()
	return nil
}
