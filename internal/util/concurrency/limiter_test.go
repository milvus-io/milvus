package concurrency

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	maxTaskNum := runtime.GOMAXPROCS(0)
	if maxTaskNum%2 != 0 {
		maxTaskNum *= 2
	}
	allTaskNum := maxTaskNum * 2

	limiter := NewLimiter(maxTaskNum)
	assert.Equal(t, maxTaskNum, limiter.Cap())

	task := func() {
		limiter.Acquire()
	}

	for i := 0; i < allTaskNum; i++ {
		go task()
	}

	time.Sleep(time.Second)

	assert.Equal(t, maxTaskNum, limiter.Cap())
	assert.Equal(t, maxTaskNum, limiter.Len())

	for i := 0; i < allTaskNum-maxTaskNum; i++ {
		limiter.Release()
	}

	assert.Equal(t, maxTaskNum, limiter.Cap())
	assert.Equal(t, maxTaskNum, limiter.Len())

	for i := 0; i < maxTaskNum/2; i++ {
		limiter.Release()
	}

	assert.Equal(t, maxTaskNum, limiter.Cap())
	assert.Equal(t, maxTaskNum/2, limiter.Len())
}
