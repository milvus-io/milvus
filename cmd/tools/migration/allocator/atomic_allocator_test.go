package allocator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicAllocator_AllocID(t *testing.T) {
	n := 100
	alloc := NewAllocator()
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := alloc.AllocID()
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(defaultDelta), alloc.delta)
	assert.Equal(t, int64(defaultInitializedValue+n*defaultDelta), alloc.now.Load())
}
