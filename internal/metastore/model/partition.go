package model

import (
	"github.com/milvus-io/milvus/internal/common"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
)

type Partition struct {
	PartitionID               int64
	PartitionName             string
	PartitionCreatedTimestamp uint64
	Extra                     map[string]string
	CollectionID              int64
}

func (p Partition) Clone() *Partition {
	clone := &Partition{
		PartitionID:               p.PartitionID,
		PartitionName:             p.PartitionName,
		PartitionCreatedTimestamp: p.PartitionCreatedTimestamp,
		Extra:                     common.CloneS2S(p.Extra),
		CollectionID:              p.CollectionID,
	}
	return clone
}

func MarshalPartitionModel(partition *Partition) *pb.PartitionInfo {
	return &pb.PartitionInfo{
		PartitionID:               partition.PartitionID,
		PartitionName:             partition.PartitionName,
		PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		CollectionId:              partition.CollectionID,
	}
}

func UnmarshalPartitionModel(info *pb.PartitionInfo) *Partition {
	return &Partition{
		PartitionID:               info.GetPartitionID(),
		PartitionName:             info.GetPartitionName(),
		PartitionCreatedTimestamp: info.GetPartitionCreatedTimestamp(),
		CollectionID:              info.GetCollectionId(),
	}
}
