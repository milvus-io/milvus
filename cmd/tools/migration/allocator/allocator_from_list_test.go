package allocator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocatorFromList(t *testing.T) {
	t.Run("de asc", func(t *testing.T) {
		s := []int64{100000, 10000, 1000}
		alloc := NewAllocatorFromList(s, true, true)
		n := 100
		wg := &sync.WaitGroup{}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, err := alloc.AllocID()
				assert.NoError(t, err)
				assert.True(t, id < 1000)
			}()
		}
		wg.Wait()
		assert.Equal(t, int64(-1), alloc.delta)
		assert.Equal(t, int64(1000-n), alloc.now.Load())
	})
}
