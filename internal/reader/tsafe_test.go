package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSafe_GetAndSet(t *testing.T) {
	tSafe := newTSafe()
	watcher := newTSafeWatcher()
	(*tSafe).registerTSafeWatcher(watcher)

	go func() {
		watcher.hasUpdate()
		timestamp := (*tSafe).get()
		assert.Equal(t, timestamp, Timestamp(1000))
	}()

	(*tSafe).set(Timestamp(1000))
}
