package concurrency

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	pool, err := NewPool(runtime.NumCPU())
	assert.NoError(t, err)

	taskNum := pool.Cap() * 2
	futures := make([]*Future, 0, taskNum)
	for i := 0; i < taskNum; i++ {
		res := i
		future := pool.Submit(func() (interface{}, error) {
			time.Sleep(500 * time.Millisecond)
			return res, nil
		})
		futures = append(futures, future)
	}

	assert.Greater(t, pool.Running(), 0)
	AwaitAll(futures)
	for i, future := range futures {
		res, err := future.Await()
		assert.NoError(t, err)
		assert.Equal(t, err, future.Err())
		assert.True(t, future.OK())
		assert.Equal(t, res, future.Value())
		assert.Equal(t, i, res.(int))

		// Await() should be idempotent
		<-future.Inner()
		resDup, errDup := future.Await()
		assert.Equal(t, res, resDup)
		assert.Equal(t, err, errDup)
	}
}
