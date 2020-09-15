package mock

import (
	"fmt"
	"time"

	pb "github.com/czs007/suvlim/pkg/master/grpc/master"
	messagepb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Collection struct {
	ID         uint64      `json:"id"`
	Name       string      `json:"name"`
	CreateTime uint64      `json:"creat_time"`
	Schema     []FieldMeta `json:"schema"`
	//	ExtraSchema       []FieldMeta `json:"extra_schema"`
	SegmentIDs        []uint64 `json:"segment_ids"`
	PartitionTags     []string `json:"partition_tags"`
	GrpcMarshalString string   `json:"grpc_marshal_string"`
}

type FieldMeta struct {
	FieldName string `json:"field_name"`
	Type      string `json:"type"`
	DIM       int64  `json:"dimension"`
}

func GrpcMarshal(c *Collection) *Collection {
	if c.GrpcMarshalString != "" {
		c.GrpcMarshalString = ""
	}
	pbSchema := &messagepb.Schema{
		FieldMetas: []*messagepb.FieldMeta{},
	}
	grpcCollection := &pb.Collection{
		Id:            c.ID,
		Name:          c.Name,
		Schema:        pbSchema,
		CreateTime:    c.CreateTime,
		SegmentIds:    c.SegmentIDs,
		PartitionTags: c.PartitionTags,
	}
	out, err := proto.Marshal(grpcCollection)
	if err != nil {
		fmt.Println(err)
	}
	c.GrpcMarshalString = string(out)
	return c
}

func NewCollection(id uuid.UUID, name string, createTime time.Time,
	schema []*messagepb.FieldMeta, sIds []uuid.UUID, ptags []string) Collection {

	segementIDs := []uint64{}
	newSchema := []FieldMeta{}
	for _, v := range schema {
		newSchema = append(newSchema, FieldMeta{FieldName: v.FieldName, Type: v.Type.String(), DIM: v.Dim})
	}
	for _, sid := range sIds {
		segementIDs = append(segementIDs, uint64(sid.ID()))
	}
	return Collection{
		ID:            uint64(id.ID()),
		Name:          name,
		CreateTime:    uint64(createTime.Unix()),
		Schema:        newSchema,
		SegmentIDs:    segementIDs,
		PartitionTags: ptags,
	}
}

func Collection2JSON(c Collection) (string, error) {
	b, err := json.Marshal(&c)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func JSON2Collection(s string) (*Collection, error) {
	var c Collection
	err := json.Unmarshal([]byte(s), &c)
	if err != nil {
		return &Collection{}, err
	}
	return &c, nil
}
