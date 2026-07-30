package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
)

func TestNotifySealSegmentKeepsPendingSealWhenNotifierIsFull(t *testing.T) {
	worker := newSealWorker(nil)
	worker.sealNotifier <- struct{}{}
	expectedPolicy := policy.PolicyCapacity()

	done := make(chan struct{})
	go func() {
		worker.NotifySealSegment(2, expectedPolicy)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("seal notification blocked when notifier was full")
	}

	pending := worker.takePendingSeals()
	assert.Len(t, pending, 1)
	assert.Equal(t, expectedPolicy, pending[2])

	select {
	case <-worker.sealNotifier:
	default:
		t.Fatal("missing existing seal worker wakeup")
	}
	select {
	case <-worker.sealNotifier:
		t.Fatal("full notifier should coalesce additional wakeups")
	default:
	}
}
