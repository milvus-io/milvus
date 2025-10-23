package broadcaster

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestResourceKeyLocker(t *testing.T) {
	t.Run("concurrent lock/unlock", func(t *testing.T) {
		locker := newResourceKeyLocker(newBroadcasterMetrics())
		const numGoroutines = 10
		const numKeys = 5
		const numIterations = 100

		// Create a set of test keys
		keys := make([]message.ResourceKey, numKeys*2)
		for i := 0; i < numKeys; i++ {
			keys[i] = message.NewExclusiveCollectionNameResourceKey("test", fmt.Sprintf("test_collection_%d", i))
			keys[i+numKeys] = message.NewSharedDBNameResourceKey("test")
		}
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		// Start multiple goroutines trying to lock/unlock the same keys
		done := make(chan bool)
		for i := 0; i < numGoroutines; i++ {
			go func(id uint64) {
				for j := 0; j < numIterations; j++ {
					// Try to lock random subset of keys
					right := rand.Intn(numKeys)
					left := 0
					if right > 0 {
						left = rand.Intn(right)
					}
					keysToLock := make([]message.ResourceKey, right-left)
					for i := left; i < right; i++ {
						keysToLock[i-left] = keys[i]
					}
					rand.Shuffle(len(keysToLock), func(i, j int) {
						keysToLock[i], keysToLock[j] = keysToLock[j], keysToLock[i]
					})
					n := rand.Intn(10)
					if n < 3 {
						// Lock the keys
						guards := locker.Lock(keysToLock...)
						// Hold lock briefly
						time.Sleep(time.Millisecond)
						// Unlock the keys
						guards.Unlock()
					} else {
						guards := locker.Lock(keysToLock...)
						guards.Unlock()
					}
				}
				done <- true
			}(uint64(i))
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})

	t.Run("deadlock prevention", func(t *testing.T) {
		locker := newResourceKeyLocker(newBroadcasterMetrics())
		key1 := message.NewCollectionNameResourceKey("test_collection_1")
		key2 := message.NewCollectionNameResourceKey("test_collection_2")

		// Create two goroutines that try to lock resources in different orders
		done := make(chan bool)
		go func() {
			for i := 0; i < 100; i++ {
				// Lock key1 then key2
				guards := locker.Lock(key1, key2)
				time.Sleep(time.Millisecond)
				guards.Unlock()
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 100; i++ {
				// Lock key2 then key1
				guards := locker.Lock(key2, key1)
				time.Sleep(time.Millisecond)
				guards.Unlock()
			}
			done <- true
		}()

		// Wait for both goroutines with timeout
		for i := 0; i < 2; i++ {
			select {
			case <-done:
				// Goroutine completed successfully
			case <-time.After(5 * time.Second):
				t.Fatal("Deadlock detected - goroutines did not complete in time")
			}
		}
	})

	t.Run("fast lock", func(t *testing.T) {
		locker := newResourceKeyLocker(newBroadcasterMetrics())
		key := message.NewCollectionNameResourceKey("test_collection")

		// First fast lock should succeed
		guards1, err := locker.FastLock(key, key)
		if err != nil {
			t.Fatalf("First FastLock failed: %v", err)
		}

		// Second fast lock should fail
		_, err = locker.FastLock(key)
		if err == nil {
			t.Fatal("Second FastLock should have failed")
		}

		// After unlock, fast lock should succeed again
		guards1.Unlock()
		guards2, err := locker.FastLock(key)
		if err != nil {
			t.Fatalf("FastLock after unlock failed: %v", err)
		}
		guards2.Unlock()
	})
}

func TestUniqueSortResourceKeys(t *testing.T) {
	keys := []message.ResourceKey{
		message.NewSharedDBNameResourceKey("test_db_1"),
		message.NewSharedDBNameResourceKey("test_db_1"),
		message.NewSharedDBNameResourceKey("test_db_2"),
		message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_11"),
		message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_11"),
		message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_12"),
		message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_13"),
		message.NewExclusiveCollectionNameResourceKey("test_db_2", "test_collection_21"),
		message.NewExclusiveCollectionNameResourceKey("test_db_2", "test_collection_21"),
		message.NewExclusiveCollectionNameResourceKey("test_db_2", "test_collection_22"),
		message.NewSharedClusterResourceKey(),
	}
	for i := 0; i < 10; i++ {
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
		keys2 := uniqueSortResourceKeys(keys)
		assert.Equal(t, keys2, []message.ResourceKey{
			message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_11"),
			message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_12"),
			message.NewExclusiveCollectionNameResourceKey("test_db_1", "test_collection_13"),
			message.NewExclusiveCollectionNameResourceKey("test_db_2", "test_collection_21"),
			message.NewExclusiveCollectionNameResourceKey("test_db_2", "test_collection_22"),
			message.NewSharedDBNameResourceKey("test_db_1"),
			message.NewSharedDBNameResourceKey("test_db_2"),
			message.NewSharedClusterResourceKey(),
		})
	}
}
