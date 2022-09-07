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
	VirtualChannelNames  []string
	PhysicalChannelNames []string
	ShardsNum            int32
	StartPositions       []*commonpb.KeyDataPair
	CreateTime           uint64
	ConsistencyLevel     commonpb.ConsistencyLevel
	Aliases              []string          // TODO: deprecate this.
	Extra                map[string]string // deprecated.
	State                pb.CollectionState
}

func (c Collection) Available() bool {
	return c.State == pb.CollectionState_CollectionCreated
}

func (c Collection) Clone() *Collection {
	return &Collection{
		TenantID:             c.TenantID,
		CollectionID:         c.CollectionID,
		Name:                 c.Name,
		Description:          c.Description,
		AutoID:               c.AutoID,
		Fields:               CloneFields(c.Fields),
		Partitions:           ClonePartitions(c.Partitions),
		VirtualChannelNames:  common.CloneStringList(c.VirtualChannelNames),
		PhysicalChannelNames: common.CloneStringList(c.PhysicalChannelNames),
		ShardsNum:            c.ShardsNum,
		ConsistencyLevel:     c.ConsistencyLevel,
		CreateTime:           c.CreateTime,
		StartPositions:       common.CloneKeyDataPairs(c.StartPositions),
		Aliases:              common.CloneStringList(c.Aliases),
		Extra:                common.CloneStr2Str(c.Extra),
		State:                c.State,
	}
}

func (c Collection) Equal(other Collection) bool {
	return c.TenantID == other.TenantID &&
		CheckPartitionsEqual(c.Partitions, other.Partitions) &&
		c.Name == other.Name &&
		c.Description == other.Description &&
		c.AutoID == other.AutoID &&
		CheckFieldsEqual(c.Fields, other.Fields) &&
		c.ShardsNum == other.ShardsNum &&
		c.ConsistencyLevel == other.ConsistencyLevel
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

	return &Collection{
		CollectionID:         coll.ID,
		Name:                 coll.Schema.Name,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               UnmarshalFieldModels(coll.GetSchema().GetFields()),
		Partitions:           partitions,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		StartPositions:       coll.StartPositions,
		State:                coll.State,
	}
}

// MarshalCollectionModel marshal only collection-related information.
// partitions, aliases and fields won't be marshaled. They should be written to newly path.
func MarshalCollectionModel(coll *Collection) *pb.CollectionInfo {
	return marshalCollectionModelWithConfig(coll, newDefaultConfig())
}

type config struct {
	withFields     bool
	withPartitions bool
}

type Option func(c *config)

func newDefaultConfig() *config {
	return &config{withFields: false, withPartitions: false}
}

func WithFields() Option {
	return func(c *config) {
		c.withFields = true
	}
}

func WithPartitions() Option {
	return func(c *config) {
		c.withPartitions = true
	}
}

func marshalCollectionModelWithConfig(coll *Collection, c *config) *pb.CollectionInfo {
	if coll == nil {
		return nil
	}

	collSchema := &schemapb.CollectionSchema{
		Name:        coll.Name,
		Description: coll.Description,
		AutoID:      coll.AutoID,
	}

	if c.withFields {
		fields := MarshalFieldModels(coll.Fields)
		collSchema.Fields = fields
	}

	collectionPb := &pb.CollectionInfo{
		ID:                   coll.CollectionID,
		Schema:               collSchema,
		CreateTime:           coll.CreateTime,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		StartPositions:       coll.StartPositions,
		State:                coll.State,
	}

	if c.withPartitions {
		for _, partition := range coll.Partitions {
			collectionPb.PartitionNames = append(collectionPb.PartitionNames, partition.PartitionName)
			collectionPb.PartitionIDs = append(collectionPb.PartitionIDs, partition.PartitionID)
			collectionPb.PartitionCreatedTimestamps = append(collectionPb.PartitionCreatedTimestamps, partition.PartitionCreatedTimestamp)
		}
	}

	return collectionPb
}

func MarshalCollectionModelWithOption(coll *Collection, opts ...Option) *pb.CollectionInfo {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return marshalCollectionModelWithConfig(coll, c)
}
