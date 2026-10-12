package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
)

func TestNotifySealSegmentDropsWhenQueueIsFull(t *testing.T) {
	worker := &sealWorker{
		sealNotifier: make(chan sealSegmentIDWithPolicy, 1),
	}
	worker.sealNotifier <- sealSegmentIDWithPolicy{
		segmentID:  1,
		sealPolicy: policy.PolicyCapacity(),
	}

	worker.NotifySealSegment(2, policy.PolicyCapacity())
	first := <-worker.sealNotifier
	assert.Equal(t, int64(1), first.segmentID)

	select {
	case notification := <-worker.sealNotifier:
		t.Fatalf("received notification for segment %d after the full queue should have dropped it", notification.segmentID)
	case <-time.After(200 * time.Millisecond):
	}
}
