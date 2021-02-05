package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartition_newPartition(t *testing.T) {
	partitionID := defaultPartitionID
	partition := newPartition(UniqueID(0), partitionID)
	assert.Equal(t, partition.ID(), defaultPartitionID)
}
