package collection

import (
	"time"

	masterpb "github.com/czs007/suvlim/internal/proto/master"
	messagepb "github.com/czs007/suvlim/internal/proto/message"
	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Collection struct {
	ID         uint64      `json:"id"`
	Name       string      `json:"name"`
	CreateTime uint64      `json:"creat_time"`
	Schema     []FieldMeta `json:"schema"`
	//	ExtraSchema       []FieldMeta `json:"extra_schema"`
	SegmentIDs        []uint64                `json:"segment_ids"`
	PartitionTags     []string                `json:"partition_tags"`
	GrpcMarshalString string                  `json:"grpc_marshal_string"`
	IndexParam        []*messagepb.IndexParam `json:"index_param"`
}

type FieldMeta struct {
	FieldName string             `json:"field_name"`
	Type      messagepb.DataType `json:"type"`
	DIM       int64              `json:"dimension"`
}

func GrpcMarshal(c *Collection) *Collection {
	if c.GrpcMarshalString != "" {
		c.GrpcMarshalString = ""
	}
	pbSchema := &messagepb.Schema{
		FieldMetas: []*messagepb.FieldMeta{},
	}
	schemaSlice := []*messagepb.FieldMeta{}
	for _, v := range c.Schema {
		newpbMeta := &messagepb.FieldMeta{
			FieldName: v.FieldName,
			Type:      v.Type,
			Dim:       v.DIM,
		}
		schemaSlice = append(schemaSlice, newpbMeta)
	}
	pbSchema.FieldMetas = schemaSlice
	grpcCollection := &masterpb.Collection{
		Id:            c.ID,
		Name:          c.Name,
		Schema:        pbSchema,
		CreateTime:    c.CreateTime,
		SegmentIds:    c.SegmentIDs,
		PartitionTags: c.PartitionTags,
		Indexes:       c.IndexParam,
	}
	out := proto.MarshalTextString(grpcCollection)
	c.GrpcMarshalString = out
	return c
}

func NewCollection(id uint64, name string, createTime time.Time,
	schema []*messagepb.FieldMeta, sIds []uint64, ptags []string) Collection {

	segementIDs := []uint64{}
	newSchema := []FieldMeta{}
	for _, v := range schema {
		newSchema = append(newSchema, FieldMeta{FieldName: v.FieldName, Type: v.Type, DIM: v.Dim})
	}
	for _, sid := range sIds {
		segementIDs = append(segementIDs, sid)
	}
	return Collection{
		ID:            id,
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
