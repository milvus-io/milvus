package model

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

// NewCollectionTombstone creates a new CollectionTombstone instance with the given CollectionTombstoneImpl.
func NewCollectionTombstone(dc *datapb.CollectionTombstoneImpl) *CollectionTombstone {
	return &CollectionTombstone{
		Tombstone: &datapb.CollectionTombstone{
			Tombstone: &datapb.CollectionTombstone_Collection{
				Collection: dc,
			},
		},
	}
}

// NewPartitionTombstone creates a new CollectionTombstone instance with the given PartitionTombstoneImpl.
func NewParititionTombstone(dp *datapb.PartitionTombstoneImpl) *CollectionTombstone {
	return &CollectionTombstone{
		Tombstone: &datapb.CollectionTombstone{
			Tombstone: &datapb.CollectionTombstone_Partition{
				Partition: dp,
			},
		},
	}
}

// NewCollectionTombstoneFromValue creates a new CollectionTombstone instance from the given value.
func NewCollectionTombstoneFromValue(value string) (*CollectionTombstone, error) {
	tombstone := &datapb.CollectionTombstone{}
	err := proto.Unmarshal([]byte(value), tombstone)
	if err != nil {
		return nil, err
	}
	return &CollectionTombstone{Tombstone: tombstone}, nil
}

// CollectionTombstone is a wrapper for datapb.CollectionTombstone.
type CollectionTombstone struct {
	Tombstone *datapb.CollectionTombstone
}

// Key returns the key of the CollectionTombstone.
func (ct *CollectionTombstone) Key() string {
	switch tombstone := ct.Tombstone.Tombstone.(type) {
	case *datapb.CollectionTombstone_Collection:
		return fmt.Sprintf("%d", tombstone.Collection.GetCollectionId())
	case *datapb.CollectionTombstone_Partition:
		return fmt.Sprintf("%d/%d", tombstone.Partition.GetCollectionId(), tombstone.Partition.GetPartitionId())
	}
	panic("unexpected tombstone type")
}

func (ct *CollectionTombstone) Value() string {
	val, err := proto.Marshal(ct.Tombstone)
	if err != nil {
		panic(err)
	}
	return string(val)
}
