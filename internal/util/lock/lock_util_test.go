package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTryLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source, mutex, interval, retryCount, toPrint := "test", sync.RWMutex{}, 1*time.Second, uint(2), true

	//no lock before
	assert.True(t, TryLock(ctx, source, &mutex, interval, retryCount, toPrint))
	mutex.Unlock()

	//lock before
	mutex.Lock()
	assert.False(t, TryLock(ctx, source, &mutex, interval, retryCount, toPrint))
	mutex.Unlock()

	//ctx cancel
	var wg sync.WaitGroup
	mutex.Lock()
	wg.Add(1)
	go func() {
		assert.False(t, TryLock(ctx, source, &mutex, interval, retryCount, toPrint))
		wg.Done()
	}()
	cancel()
	wg.Wait()
	mutex.Unlock()
}
