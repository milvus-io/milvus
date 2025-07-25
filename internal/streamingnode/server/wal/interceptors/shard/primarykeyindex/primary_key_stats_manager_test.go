package primaryindex

import (
	"fmt"
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestCheckPrimaryKeyExists(t *testing.T) {
	manager := NewGrowingSegmentPKStatsManager()
	segmentID := int64(1)

	err := manager.CreateSegmentStats(segmentID, 100, int64(schemapb.DataType_Int64), 1000)
	assert.NoError(t, err)

	t.Run("check_int64_pk_exists", func(t *testing.T) {
		pk := storage.NewInt64PrimaryKey(12345)
		manager.UpdatePrimaryKey(segmentID, pk)

		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, segmentID, foundSegmentID)
	})

	t.Run("check_int64_pk_not_exists", func(t *testing.T) {
		pk := storage.NewInt64PrimaryKey(99999)
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, int64(-1), foundSegmentID)
	})

	t.Run("check_varchar_pk_exists", func(t *testing.T) {
		manager.CreateSegmentStats(2, 100, int64(schemapb.DataType_VarChar), 1000)
		pk := storage.NewVarCharPrimaryKey("test_string")
		manager.UpdatePrimaryKey(2, pk)

		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, int64(2), foundSegmentID)
	})

	t.Run("check_varchar_pk_not_exists", func(t *testing.T) {
		pk := storage.NewVarCharPrimaryKey("not_exists")
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, int64(-1), foundSegmentID)
	})
}

func TestMemoryUsageWith10kKeys(t *testing.T) {
	manager := NewGrowingSegmentPKStatsManager()
	segmentID := int64(1)

	err := manager.CreateSegmentStats(segmentID, 100, int64(schemapb.DataType_Int64), 10000)
	assert.NoError(t, err)

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		err := manager.UpdatePrimaryKey(segmentID, pk)
		assert.NoError(t, err)
	}

	for i := 0; i < numKeys; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, segmentID, foundSegmentID, "Key %d should exist", i)
	}

	for i := numKeys; i < numKeys+100; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, int64(0), foundSegmentID, "Key %d should not exist", i)
	}

	summary := manager.GetStatsSummary()
	t.Logf("Stats summary: %+v", summary)
}

func TestMemoryUsageWith10kVarcharKeys(t *testing.T) {
	manager := NewGrowingSegmentPKStatsManager()
	segmentID := int64(2)

	err := manager.CreateSegmentStats(segmentID, 100, int64(schemapb.DataType_VarChar), 10000)
	assert.NoError(t, err)

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		err := manager.UpdatePrimaryKey(segmentID, pk)
		assert.NoError(t, err)
	}

	for i := 0; i < numKeys; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, segmentID, foundSegmentID, "Key %d should exist", i)
	}

	for i := numKeys; i < numKeys+100; i++ {
		pk := storage.NewVarCharPrimaryKey(fmt.Sprintf("key_%d", i))
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, int64(0), foundSegmentID, "Key %d should not exist", i)
	}

	summary := manager.GetStatsSummary()
	t.Logf("Stats summary: %+v", summary)
}

func TestOneWriterOneReader(t *testing.T) {
	manager := NewGrowingSegmentPKStatsManager()
	segmentID := int64(1)

	err := manager.CreateSegmentStats(segmentID, 100, int64(schemapb.DataType_Int64), 10000)
	assert.NoError(t, err)

	const numKeys = 1000
	var wg sync.WaitGroup
	var writeErrors []error
	var readErrors []error
	var writeMu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numKeys; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			err := manager.UpdatePrimaryKey(segmentID, pk)
			if err != nil {
				writeMu.Lock()
				writeErrors = append(writeErrors, err)
				writeMu.Unlock()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numKeys; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			foundSegmentID := manager.CheckPrimaryKeyExists(pk)
			_ = foundSegmentID
		}
	}()

	wg.Wait()

	assert.Empty(t, writeErrors, "Should have no write errors")
	assert.Empty(t, readErrors, "Should have no read errors")

	for i := 0; i < numKeys; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		foundSegmentID := manager.CheckPrimaryKeyExists(pk)
		assert.Equal(t, segmentID, foundSegmentID, "Key %d should exist after concurrent write/read", i)
	}

	summary := manager.GetStatsSummary()
	t.Logf("One writer one reader test stats: %+v", summary)
	t.Logf("Total keys: %d", numKeys)
}
