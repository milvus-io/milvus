package queryservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplica_Release(t *testing.T) {
	replica := newMetaReplica()
	err := replica.addCollection(0, 1, nil)
	require.NoError(t, err)

	colls, err := replica.getCollections(0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(colls))

	err = replica.addPartition(0, 1, 100)
	assert.NoError(t, err)
	partitions, err := replica.getPartitions(0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))

	err = replica.releasePartition(0, 1, 100)
	assert.NoError(t, err)
	partitions, err = replica.getPartitions(0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(partitions))
	err = replica.releasePartition(0, 1, 100)
	assert.Error(t, err)

	err = replica.releaseCollection(0, 1)
	assert.NoError(t, err)
	colls, err = replica.getCollections(0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(colls))
	err = replica.releaseCollection(0, 1)
	assert.Error(t, err)
}
