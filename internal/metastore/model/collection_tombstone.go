package model

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type CollectionTombstone struct {
	Tombstone *datapb.CollectionTombstone
}

func (ct *CollectionTombstone) Key() string {
	switch tombstone := ct.Tombstone.Tombstone.(type) {
	case *datapb.CollectionTombstone_Collection:
		return fmt.Sprintf("%d", tombstone.Collection.GetCollectionId())
	case *datapb.CollectionTombstone_Partition:
		return fmt.Sprintf("%d/%d", tombstone.Partition.GetCollectionId(), tombstone.Partition.GetPartitionId())
	}
	panic("unexpected tombstone type")
}
