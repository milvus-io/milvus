package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaCacheCollectionCache(t *testing.T) {
	coll1 := &collectionInfo{
		collID:         1,
		databaseName:   "db1",
		collectionName: "coll1",
		aliases:        []string{"ca1", "ca2"},
	}
	coll2 := &collectionInfo{
		collID:         2,
		databaseName:   "db2",
		collectionName: "coll",
		aliases:        []string{"ca1", "ca2"},
	}
	coll3 := &collectionInfo{
		collID:         3,
		databaseName:   "db1",
		collectionName: "coll",
	}
	c := newMetaCacheCollectionCache()
	c.UpdateCollectionCache(coll1)
	c.UpdateCollectionCache(coll2)
	c.UpdateCollectionCache(coll3)

	// Test get by id.
	result, ok := c.GetCollectionByID(1)
	assert.True(t, ok)
	assert.Equal(t, coll1.collID, result.collID)
	result, ok = c.GetCollectionByID(2)
	assert.True(t, ok)
	assert.Equal(t, coll2.collID, result.collID)
	result, ok = c.GetCollectionByID(4)
	assert.False(t, ok)
	assert.Nil(t, result)

	// Test get by name.
	result, ok = c.GetCollectionByName(coll1.GetDatabaseName(), coll1.GetCollectionName())
	assert.True(t, ok)
	assert.Equal(t, coll1.collID, result.collID)
	result, ok = c.GetCollectionByName(coll2.GetDatabaseName(), coll2.GetCollectionName())
	assert.True(t, ok)
	assert.Equal(t, coll2.collID, result.collID)

	// Test can not get by name.
	result, ok = c.GetCollectionByName("db4", "coll1")
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByName("db1", "coll100")
	assert.False(t, ok)
	assert.Nil(t, result)

	// Test get by alias.
	result, ok = c.GetCollectionByName(coll1.GetDatabaseName(), coll1.aliases[0])
	assert.True(t, ok)
	assert.Equal(t, coll1.collID, result.collID)
	result, ok = c.GetCollectionByName(coll1.GetDatabaseName(), coll1.aliases[1])
	assert.True(t, ok)
	assert.Equal(t, coll1.collID, result.collID)
	result, ok = c.GetCollectionByName(coll2.GetDatabaseName(), coll2.aliases[1])
	assert.True(t, ok)
	assert.Equal(t, coll2.collID, result.collID)

	// Test has database.
	ok = c.HasDatabase(coll1.GetDatabaseName())
	assert.True(t, ok)
	ok = c.HasDatabase(coll2.GetDatabaseName())
	assert.True(t, ok)
	ok = c.HasDatabase("db100")
	assert.False(t, ok)

	relatedNames := c.RemoveCollectionByID(1)
	assert.ElementsMatch(t, relatedNames, []string{"coll1", "ca1", "ca2"})
	result, ok = c.GetCollectionByID(1)
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByName(coll1.GetDatabaseName(), coll1.GetCollectionName())
	assert.False(t, ok)
	assert.Nil(t, result)

	c.RemoveCollectionByName("db2", "ca1")
	result, ok = c.GetCollectionByName("db2", "ca1")
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByName(coll2.GetDatabaseName(), coll2.GetCollectionName())
	assert.False(t, ok)
	assert.Nil(t, result)

	// Test Remove database
	c = newMetaCacheCollectionCache()
	c.UpdateCollectionCache(coll1)
	c.UpdateCollectionCache(coll2)
	c.UpdateCollectionCache(coll3)

	c.RemoveDatabase("db1")
	result, ok = c.GetCollectionByID(1)
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByName(coll1.GetDatabaseName(), coll1.GetCollectionName())
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByID(3)
	assert.False(t, ok)
	assert.Nil(t, result)
	result, ok = c.GetCollectionByName(coll3.GetDatabaseName(), coll3.GetCollectionName())
	assert.False(t, ok)
	assert.Nil(t, result)
	ok = c.HasDatabase("db1")
	assert.False(t, ok)

	result, ok = c.GetCollectionByName(coll2.GetDatabaseName(), coll2.GetCollectionName())
	assert.True(t, ok)
	assert.Equal(t, coll2.collID, result.collID)

	// Test Remove partition
	c = newMetaCacheCollectionCache()
	coll4 := &collectionInfo{
		collID:         4,
		databaseName:   "db2",
		collectionName: "coll",
		schema: &schemaInfo{
			hasPartitionKeyField: true,
		},
		partInfo: &partitionInfos{
			partitionInfos: []*partitionInfo{
				{
					partitionID: 1,
					name:        "1",
				},
				{
					partitionID: 2,
					name:        "2",
				},
			},
		},
	}
	c.UpdateCollectionCache(coll4)
	result, ok = c.GetCollectionByID(4)
	assert.True(t, ok)
	assert.Equal(t, 2, len(result.partInfo.partitionInfos))

	// RemovePartition do not affect the previous collection cache.
	c.RemovePartitionByName("db2", "coll", "1")
	assert.True(t, ok)
	assert.Equal(t, 2, len(result.partInfo.partitionInfos))

	// RemovePartitionByName affect collection cache after remove.
	result, ok = c.GetCollectionByID(4)
	assert.True(t, ok)
	assert.Equal(t, 1, len(result.partInfo.partitionInfos))

	// removing a non-exist collection is ok.
	c.RemovePartitionByName("db3", "coll", "2")
}
