package cache

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
)

func TestScannerRoundsAndSlotBudget(t *testing.T) {
	c := New(nil)
	t.Cleanup(c.Close)
	s := c.NewScanner()
	require.ErrorIs(t, s.Scan(1, func(*CollectionEntry) bool { t.Fatal("empty cache"); return true }), io.EOF)
	for id := int64(1); id <= 4; id++ {
		c.PublishLoadConfig(id, cacheConfig(id, id*10), 1)
	}
	var ids []int64
	visit := func(c *CollectionEntry) bool { ids = append(ids, c.ID()); return true }
	require.NoError(t, s.Scan(1, visit))
	c.PublishLoadConfig(2, nil, 2)
	require.NoError(t, s.Scan(1, visit), "a tombstone consumes the slot budget")
	require.Equal(t, []int64{1}, ids)
	require.NoError(t, s.Scan(10, func(c *CollectionEntry) bool { visit(c); return false }))
	require.Equal(t, []int64{1, 3}, ids)
	c.PublishLoadConfig(5, cacheConfig(5, 50), 1)
	require.ErrorIs(t, s.Scan(10, visit), io.EOF)
	require.Equal(t, []int64{1, 3, 4}, ids, "append does not extend an active round")
	ids = nil
	require.ErrorIs(t, s.Scan(10, visit), io.EOF)
	require.Equal(t, []int64{1, 3, 4, 5}, ids)
}

func TestScannerRetainsDirectoryAcrossCompactionAndReload(t *testing.T) {
	c := New(nil)
	t.Cleanup(c.Close)
	for id := int64(1); id <= 4; id++ {
		c.PublishLoadConfig(id, cacheConfig(id, id*10), 1)
	}
	s := c.NewScanner().(*collectionScanner)
	var ids []int64
	visit := func(c *CollectionEntry) bool { ids = append(ids, c.ID()); return true }
	require.NoError(t, s.Scan(1, visit))
	oldDirectory := s.slots
	c.PublishLoadConfig(1, nil, 2)
	c.PublishLoadConfig(2, nil, 2)
	c.compactCollections()
	require.Len(t, c.scanIndex.slots, 2)
	require.Same(t, oldDirectory[2], c.scanIndex.slots[0], "compaction shares live slots")
	c.PublishLoadConfig(2, cacheConfig(2, 20), 3)
	c.PublishLoadConfig(3, cacheConfig(3, 30), 7)
	require.ErrorIs(t, s.Scan(10, func(entry *CollectionEntry) bool {
		if entry.ID() == 3 {
			require.Equal(t, uint64(7), entry.ConfigVersion(), "contents remain live")
		}
		return visit(entry)
	}), io.EOF)
	require.Equal(t, []int64{1, 3, 4}, ids, "neither restart nor skip after compaction; retired slot stays dead")
	require.Nil(t, s.slots, "EOF releases the old backing array")
	ids = nil
	require.ErrorIs(t, s.Scan(10, visit), io.EOF)
	require.Equal(t, []int64{3, 4, 2}, ids)
}

func TestScannerCallbackRunsOutsideCacheLock(t *testing.T) {
	c := New(nil)
	t.Cleanup(c.Close)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	require.ErrorIs(t, c.NewScanner().Scan(10, func(*CollectionEntry) bool {
		c.PublishLoadConfig(1, nil, 2)
		c.PublishLoadConfig(2, cacheConfig(2, 20), 1)
		return true
	}), io.EOF)
}

func TestCompactionRejectsConcurrentAppendAndSharesRetirement(t *testing.T) {
	for _, appendCollection := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "append"}[appendCollection], func(t *testing.T) {
			c := New(nil)
			t.Cleanup(c.Close)
			c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
			c.PublishLoadConfig(2, cacheConfig(2, 20), 1)
			c.PublishLoadConfig(1, nil, 2)
			var original func([]*collectionScanSlot) ([]*collectionScanSlot, int)
			patch := mockey.Mock(compactCollectionSlots).Origin(&original).To(func(slots []*collectionScanSlot) ([]*collectionScanSlot, int) {
				next, removed := original(slots)
				// Runs during the O(C) phase: publication must acquire the cache
				// lock successfully, and publishing this result must not lose it.
				if appendCollection {
					c.PublishLoadConfig(3, cacheConfig(3, 30), 1)
				} else {
					c.PublishLoadConfig(2, nil, 2)
				}
				return next, removed
			}).Build()
			defer patch.UnPatch()
			c.compactCollections()
			var ids []int64
			require.ErrorIs(t, c.NewScanner().Scan(10, func(c *CollectionEntry) bool { ids = append(ids, c.ID()); return true }), io.EOF)
			if appendCollection {
				require.Equal(t, []int64{2, 3}, ids)
				require.Len(t, c.scanIndex.slots, 3, "append invalidates replacement")
			} else {
				require.Empty(t, ids, "a copied live slot must not resurrect a deletion")
				require.Equal(t, 1, c.scanIndex.deleted)
			}
		})
	}
}

func TestScannerConcurrentPublicationsAndCompaction(t *testing.T) {
	c := New(nil)
	t.Cleanup(c.Close)
	var workers sync.WaitGroup
	for worker := int64(0); worker < 4; worker++ {
		workers.Go(func() {
			for i := int64(1); i <= 200; i++ {
				id := worker*1000 + i
				c.PublishLoadConfig(id, cacheConfig(id, id*10), 1)
				c.PublishLoadConfig(id, nil, 2)
			}
		})
	}
	workers.Go(func() {
		s := c.NewScanner()
		for i := 0; i < 200; i++ {
			_ = s.Scan(7, func(c *CollectionEntry) bool { _ = c.LoadConfig(); return true })
			c.compactCollections()
		}
	})
	workers.Wait()
	c.compactCollections()
	require.Empty(t, c.scanIndex.slots)
	require.Zero(t, c.scanIndex.deleted)
}

func TestLazyCompactionAndClose(t *testing.T) {
	c := New(nil)
	for id := int64(1); id <= minScanTombstones; id++ {
		c.PublishLoadConfig(id, cacheConfig(id, id*10), 1)
	}
	s := c.NewScanner()
	require.NoError(t, s.Scan(1, func(*CollectionEntry) bool { return true }))
	for id := int64(1); id <= minScanTombstones; id++ {
		c.PublishLoadConfig(id, nil, 2)
	}
	c.Close() // Joins the single in-flight maintenance job.
	require.Empty(t, c.scanIndex.slots)
	require.Zero(t, c.scanIndex.deleted)
	require.ErrorIs(t, s.Scan(minScanTombstones, func(*CollectionEntry) bool { t.Fatal("retired slot"); return true }), io.EOF)
}

func BenchmarkCollectionScanner(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("collections_%d", size), func(b *testing.B) {
			c := New(nil)
			b.Cleanup(c.Close)
			for id := 1; id <= size; id++ {
				c.collection(int64(id))
			}
			s := c.NewScanner()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = s.Scan(100, func(*CollectionEntry) bool { return true })
			}
		})
	}
}
