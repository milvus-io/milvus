package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

var (
	colID      = typeutil.UniqueID(1)
	colName    = "c"
	fieldID    = typeutil.UniqueID(101)
	fieldName  = "field110"
	partID     = typeutil.UniqueID(20)
	partName   = "testPart"
	tenantID   = "tenant-1"
	typeParams = []*commonpb.KeyValuePair{
		{
			Key:   "field110-k1",
			Value: "field110-v1",
		},
	}
	startPositions = []*commonpb.KeyDataPair{
		{
			Key:  "k1",
			Data: []byte{byte(1)},
		},
	}

	colModel = &Collection{
		TenantID:     tenantID,
		CollectionID: colID,
		Name:         colName,
		AutoID:       false,
		Description:  "none",
		Fields:       []*Field{fieldModel},
		FieldIDToIndexID: []common.Int64Tuple{
			{
				Key:   fieldID,
				Value: indexID,
			},
		},
		VirtualChannelNames:  []string{"vch"},
		PhysicalChannelNames: []string{"pch"},
		ShardsNum:            1,
		CreateTime:           1,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
		Partitions: []*Partition{
			{
				PartitionID:               partID,
				PartitionName:             partName,
				PartitionCreatedTimestamp: 1,
			},
		},
	}

	deprecatedColPb = &pb.CollectionInfo{
		ID: colID,
		Schema: &schemapb.CollectionSchema{
			Name:        colName,
			Description: "none",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{filedSchemaPb},
		},
		CreateTime:                 1,
		PartitionIDs:               []int64{partID},
		PartitionNames:             []string{partName},
		PartitionCreatedTimestamps: []uint64{1},
		FieldIndexes: []*pb.FieldIndexInfo{
			{
				FiledID: fieldID,
				IndexID: indexID,
			},
		},
		VirtualChannelNames:  []string{"vch"},
		PhysicalChannelNames: []string{"pch"},
		ShardsNum:            1,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
	}

	newColPb = &pb.CollectionInfo{
		ID: colID,
		Schema: &schemapb.CollectionSchema{
			Name:        colName,
			Description: "none",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{filedSchemaPb},
		},
		CreateTime: 1,
		Partitions: []*pb.PartitionInfo{
			{
				PartitionID:               partID,
				PartitionName:             partName,
				PartitionCreatedTimestamp: 1,
			},
		},
		FieldIndexes: []*pb.FieldIndexInfo{
			{
				FiledID: fieldID,
				IndexID: indexID,
			},
		},
		VirtualChannelNames:  []string{"vch"},
		PhysicalChannelNames: []string{"pch"},
		ShardsNum:            1,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
	}
)

func TestUnmarshalCollectionModel(t *testing.T) {
	ret := UnmarshalCollectionModel(deprecatedColPb)
	ret.TenantID = tenantID
	assert.Equal(t, ret, colModel)

	ret = UnmarshalCollectionModel(newColPb)
	ret.TenantID = tenantID
	assert.Equal(t, ret, colModel)

	assert.Nil(t, UnmarshalCollectionModel(nil))
}

func TestMarshalCollectionModel(t *testing.T) {
	ret := MarshalCollectionModel(colModel)
	assert.Equal(t, ret, newColPb)

	assert.Nil(t, MarshalCollectionModel(nil))
}
