package model

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type Partition struct {
	PartitionID               int64
	PartitionName             string
	PartitionCreatedTimestamp uint64
	Extra                     map[string]string // deprecated.
	CollectionID              int64
	State                     pb.PartitionState
}

func (p *Partition) Available() bool {
	return p.State == pb.PartitionState_PartitionCreated
}

func (p *Partition) Clone() *Partition {
	return &Partition{
		PartitionID:               p.PartitionID,
		PartitionName:             p.PartitionName,
		PartitionCreatedTimestamp: p.PartitionCreatedTimestamp,
		Extra:                     common.CloneStr2Str(p.Extra),
		CollectionID:              p.CollectionID,
		State:                     p.State,
	}
}

func ClonePartitions(partitions []*Partition) []*Partition {
	clone := make([]*Partition, 0, len(partitions))
	for _, partition := range partitions {
		clone = append(clone, partition.Clone())
	}
	return clone
}

func (p *Partition) Equal(other Partition) bool {
	return p.PartitionName == other.PartitionName
}

func CheckPartitionsEqual(partitionsA, partitionsB []*Partition) bool {
	if len(partitionsA) != len(partitionsB) {
		return false
	}
	mapA := make(map[string]*Partition)
	for _, p := range partitionsA {
		mapA[p.PartitionName] = p
	}

	for _, p := range partitionsB {
		if other, exists := mapA[p.PartitionName]; !exists || !p.Equal(*other) {
			return false
		}
	}
	return true
}

func MarshalPartitionModel(partition *Partition) *pb.PartitionInfo {
	return &pb.PartitionInfo{
		PartitionID:               partition.PartitionID,
		PartitionName:             partition.PartitionName,
		PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		CollectionId:              partition.CollectionID,
		State:                     partition.State,
	}
}

func UnmarshalPartitionModel(info *pb.PartitionInfo) *Partition {
	return &Partition{
		PartitionID:               info.GetPartitionID(),
		PartitionName:             info.GetPartitionName(),
		PartitionCreatedTimestamp: info.GetPartitionCreatedTimestamp(),
		CollectionID:              info.GetCollectionId(),
		State:                     info.GetState(),
	}
}
