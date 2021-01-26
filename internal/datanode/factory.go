package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type (
	Factory interface {
	}

	MetaFactory struct {
	}

	AllocatorFactory struct {
	}

	MasterServiceFactory struct {
		ID             UniqueID
		collectionName string
		collectionID   UniqueID
	}
)

func (mf *MetaFactory) CollectionMetaFactory(collectionID UniqueID, collectionName string) *etcdpb.CollectionMeta {
	sch := schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "test collection by meta factory",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     0,
				Name:        "RowID",
				Description: "RowID field",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f0_tk1",
						Value: "f0_tv1",
					},
				},
			},
			{
				FieldID:     1,
				Name:        "Timestamp",
				Description: "Timestamp field",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f1_tk1",
						Value: "f1_tv1",
					},
				},
			},
			{
				FieldID:     100,
				Name:        "float_vector_field",
				Description: "field 100",
				DataType:    schemapb.DataType_VECTOR_FLOAT,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     101,
				Name:        "binary_vector_field",
				Description: "field 101",
				DataType:    schemapb.DataType_VECTOR_BINARY,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "32",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     102,
				Name:        "bool_field",
				Description: "field 102",
				DataType:    schemapb.DataType_BOOL,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     103,
				Name:        "int8_field",
				Description: "field 103",
				DataType:    schemapb.DataType_INT8,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     104,
				Name:        "int16_field",
				Description: "field 104",
				DataType:    schemapb.DataType_INT16,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     105,
				Name:        "int32_field",
				Description: "field 105",
				DataType:    schemapb.DataType_INT32,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     106,
				Name:        "int64_field",
				Description: "field 106",
				DataType:    schemapb.DataType_INT64,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     107,
				Name:        "float32_field",
				Description: "field 107",
				DataType:    schemapb.DataType_FLOAT,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     108,
				Name:        "float64_field",
				Description: "field 108",
				DataType:    schemapb.DataType_DOUBLE,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
		},
	}

	collection := etcdpb.CollectionMeta{
		ID:            collectionID,
		Schema:        &sch,
		CreateTime:    Timestamp(1),
		SegmentIDs:    make([]UniqueID, 0),
		PartitionTags: make([]string, 0),
	}
	return &collection
}

func (alloc AllocatorFactory) allocID() (UniqueID, error) {
	// GOOSE TODO: random ID generate
	return UniqueID(0), nil
}

func (m *MasterServiceFactory) setID(id UniqueID) {
	m.ID = id // GOOSE TODO: random ID generator
}

func (m *MasterServiceFactory) setCollectionID(id UniqueID) {
	m.collectionID = id
}

func (m *MasterServiceFactory) setCollectionName(name string) {
	m.collectionName = name
}

func (m *MasterServiceFactory) AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	resp := &masterpb.IDResponse{
		Status: &commonpb.Status{},
		ID:     m.ID,
	}
	return resp, nil
}

func (m *MasterServiceFactory) ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	resp := &milvuspb.ShowCollectionResponse{
		Status:          &commonpb.Status{},
		CollectionNames: []string{m.collectionName},
	}
	return resp, nil

}
func (m *MasterServiceFactory) DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	f := MetaFactory{}
	meta := f.CollectionMetaFactory(m.collectionID, m.collectionName)
	resp := &milvuspb.DescribeCollectionResponse{
		Status:       &commonpb.Status{},
		CollectionID: m.collectionID,
		Schema:       meta.Schema,
	}
	return resp, nil
}

func (m *MasterServiceFactory) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return &internalpb2.ComponentStates{
		State:              &internalpb2.ComponentInfo{},
		SubcomponentStates: make([]*internalpb2.ComponentInfo, 0),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}, nil
}
