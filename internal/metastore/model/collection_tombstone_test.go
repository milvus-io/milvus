package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestCollectionTombstone(t *testing.T) {
	tombstone := &CollectionTombstone{
		Tombstone: &datapb.CollectionTombstone{
			Tombstone: &datapb.CollectionTombstone_Collection{
				Collection: &datapb.CollectionTombstoneImpl{
					CollectionId: 1,
					Vchannels:    []string{"1", "2"},
				},
			},
		},
	}
	assert.Equal(t, "1", tombstone.Key())

	tombstone = &CollectionTombstone{
		Tombstone: &datapb.CollectionTombstone{
			Tombstone: &datapb.CollectionTombstone_Partition{
				Partition: &datapb.PartitionTombstoneImpl{
					CollectionId: 1,
					PartitionId:  1,
				},
			},
		},
	}
	assert.Equal(t, "1/1", tombstone.Key())
}
