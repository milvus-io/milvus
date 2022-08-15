package model

import (
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type Collection struct {
	TenantID             string
	CollectionID         int64
	Partitions           []*Partition
	Name                 string
	Description          string
	AutoID               bool
	Fields               []*Field
	FieldIDToIndexID     []common.Int64Tuple
	VirtualChannelNames  []string
	PhysicalChannelNames []string
	ShardsNum            int32
	StartPositions       []*commonpb.KeyDataPair
	CreateTime           uint64
	ConsistencyLevel     commonpb.ConsistencyLevel
	Aliases              []string          // TODO: deprecate this.
	Extra                map[string]string // extra kvs
}

func (c Collection) Clone() *Collection {
	clone := &Collection{
		TenantID:         c.TenantID,
		CollectionID:     c.CollectionID,
		Name:             c.Name,
		Description:      c.Description,
		AutoID:           c.AutoID,
		ShardsNum:        c.ShardsNum,
		ConsistencyLevel: c.ConsistencyLevel,
		CreateTime:       c.CreateTime,
		Extra:            c.Extra,
	}

	clone.Partitions = make([]*Partition, 0, len(c.Partitions))
	for _, partition := range c.Partitions {
		clone.Partitions = append(clone.Partitions, partition.Clone())
	}

	clone.Fields = make([]*Field, 0, len(c.Fields))
	for _, field := range c.Fields {
		clone.Fields = append(clone.Fields, field.Clone())
	}

	clone.FieldIDToIndexID = common.CloneInt64Tuples(c.FieldIDToIndexID)
	clone.VirtualChannelNames = common.CloneStringList(c.VirtualChannelNames)
	clone.PhysicalChannelNames = common.CloneStringList(c.PhysicalChannelNames)
	clone.StartPositions = common.CloneKeyDataPairs(c.StartPositions)
	clone.Aliases = common.CloneStringList(c.Aliases)
	clone.Extra = common.CloneS2S(c.Extra)

	return clone
}

func UnmarshalCollectionModel(coll *pb.CollectionInfo) *Collection {
	if coll == nil {
		return nil
	}

	// backward compatible for deprecated fields
	partitions := make([]*Partition, len(coll.PartitionIDs))
	for idx := range coll.PartitionIDs {
		partitions[idx] = &Partition{
			PartitionID:               coll.PartitionIDs[idx],
			PartitionName:             coll.PartitionNames[idx],
			PartitionCreatedTimestamp: coll.PartitionCreatedTimestamps[idx],
		}
	}

	filedIDToIndexIDs := make([]common.Int64Tuple, len(coll.FieldIndexes))
	for idx, fieldIndexInfo := range coll.FieldIndexes {
		filedIDToIndexIDs[idx] = common.Int64Tuple{
			Key:   fieldIndexInfo.FiledID,
			Value: fieldIndexInfo.IndexID,
		}
	}

	return &Collection{
		CollectionID:         coll.ID,
		Name:                 coll.Schema.Name,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               UnmarshalFieldModels(coll.GetSchema().GetFields()),
		Partitions:           partitions,
		FieldIDToIndexID:     filedIDToIndexIDs,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		StartPositions:       coll.StartPositions,
	}
}

// MarshalCollectionModel marshal only collection-related information.
// partitions, aliases and fields won't be marshaled. They should be written to newly path.
func MarshalCollectionModel(coll *Collection) *pb.CollectionInfo {
	if coll == nil {
		return nil
	}

	collSchema := &schemapb.CollectionSchema{
		Name:        coll.Name,
		Description: coll.Description,
		AutoID:      coll.AutoID,
	}

	partitions := make([]*pb.PartitionInfo, len(coll.Partitions))
	for idx, partition := range coll.Partitions {
		partitions[idx] = &pb.PartitionInfo{
			PartitionID:               partition.PartitionID,
			PartitionName:             partition.PartitionName,
			PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		}
	}

	fieldIndexes := make([]*pb.FieldIndexInfo, len(coll.FieldIDToIndexID))
	for idx, tuple := range coll.FieldIDToIndexID {
		fieldIndexes[idx] = &pb.FieldIndexInfo{
			FiledID: tuple.Key,
			IndexID: tuple.Value,
		}
	}

	return &pb.CollectionInfo{
		ID:                   coll.CollectionID,
		Schema:               collSchema,
		FieldIndexes:         fieldIndexes,
		CreateTime:           coll.CreateTime,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		StartPositions:       coll.StartPositions,
	}
}
