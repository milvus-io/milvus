package primaryindex

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestCheckPrimaryKeyExists(t *testing.T) {
	manager := NewPKStatsManager()
	segmentID := int64(1)

	stats := manager.createSegmentStats("test", segmentID, 100, int64(schemapb.DataType_Int64))
	assert.NotNil(t, stats)

	t.Run("check_int64_pk_exists", func(t *testing.T) {
		pk := storage.NewInt64PrimaryKey(12345)
		manager.UpdateBloomFilterFromPrimaryKeys("test", []storage.PrimaryKey{pk}, segmentID)

		manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
	})

	t.Run("check_int64_pk_not_exists", func(t *testing.T) {
		pk := storage.NewInt64PrimaryKey(99999)
		manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
	})

	t.Run("check_varchar_pk_exists", func(t *testing.T) {
		stats := manager.createSegmentStats("test", 2, 100, int64(schemapb.DataType_VarChar))
		assert.NotNil(t, stats)
		pk := storage.NewVarCharPrimaryKey("test_string")
		manager.UpdateBloomFilterFromPrimaryKeys("test", []storage.PrimaryKey{pk}, 2)

		manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
	})

	t.Run("check_varchar_pk_not_exists", func(t *testing.T) {
		pk := storage.NewVarCharPrimaryKey("not_exists")
		manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
	})
}

func TestMemoryUsageWith10kKeys(t *testing.T) {
	manager := NewPKStatsManager()
	segmentID := int64(1)

	stats := manager.createSegmentStats("test", segmentID, 100, int64(schemapb.DataType_Int64))
	assert.NotNil(t, stats)

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		manager.UpdateBloomFilterFromPrimaryKeys("test", []storage.PrimaryKey{pk}, segmentID)
	}

	for i := 0; i < numKeys; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		foundSegmentID, _ := manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
		assert.Equal(t, segmentID, foundSegmentID, "Key %d should exist", i)
	}

	for i := numKeys; i < numKeys+100; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		foundSegmentID, _ := manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
		assert.Equal(t, int64(0), foundSegmentID, "Key %d should not exist", i)
	}
}

func TestMemoryUsageWith10kVarcharKeys(t *testing.T) {
	manager := NewPKStatsManager()
	segmentID := int64(2)

	stats := manager.createSegmentStats("test", segmentID, 100, int64(schemapb.DataType_VarChar))
	assert.NotNil(t, stats)

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		manager.UpdateBloomFilterFromPrimaryKeys("test", []storage.PrimaryKey{pk}, segmentID)
	}

	for i := 0; i < numKeys; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		foundSegmentID, _ := manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
		assert.Equal(t, segmentID, foundSegmentID, "Key %d should exist", i)
	}

	for i := numKeys; i < numKeys+100; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		foundSegmentID, _ := manager.CheckDuplicatePrimaryKeys("test", []storage.PrimaryKey{pk})
		assert.Equal(t, int64(0), foundSegmentID, "Key %d should not exist", i)
	}
}
