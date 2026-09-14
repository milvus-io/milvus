package segment

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSegmentTaskBaseDone(t *testing.T) {
	task := &segmentTaskBase{}
	assert.False(t, task.Done())
	task.done.Store(true)
	assert.True(t, task.Done())
}

func TestRuntimeConfigFromViewConfig(t *testing.T) {
	owner := &recordingSegmentViewOwner{}
	cfg := runtimeConfigFromViewConfig(ViewConfig{
		Lifecycle: &failingSegmentLifecycle{},
		Runtime:   moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}},
		Owner:     owner,
	})
	assert.NotNil(t, cfg.lifecycle)
	assert.NotNil(t, cfg.runtime.Scheduler)
	assert.NotNil(t, cfg.owner)
}

func TestPublicConstructors(t *testing.T) {
	lifecycle := NewSegmentLifecycleWriter(nil, 1)
	require.NotNil(t, lifecycle)

	writer := NewBulkPackWriter(nil, nil, nil)
	require.NotNil(t, writer)
}

func TestWriteOnlyFlushPolicy(t *testing.T) {
	policy := newWriteOnlyFlushPolicy(10, 100, time.Second)
	assert.False(t, policy.ShouldFlush(writeOnlyInsertBuffer{}, 20), "empty buffer never flushes")

	start := tsoutil.ComposeTSByTime(time.Now())
	raw := message.CreateTestInsertMessage(t, 1, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	buffer := writeOnlyInsertBuffer{}
	buffer.appendMessage(retained, 10, 10)
	defer func() {
		retained.Release()
		owner.Release()
	}()
	assert.True(t, policy.ShouldFlush(buffer, start), "row threshold reached")
	assert.True(t, newWriteOnlyFlushPolicy(0, 10, 0).ShouldFlush(buffer, start), "byte threshold reached")
	assert.False(t, newWriteOnlyFlushPolicy(0, 0, 0).ShouldFlush(buffer, start), "no thresholds configured")

	old := writeOnlyInsertBuffer{}
	rawOld := message.CreateTestInsertMessage(t, 1, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	ownerOld := message.NewOwnedImmutableMessage(rawOld, nil)
	retainedOld := ownerOld.Clone()
	defer func() {
		retainedOld.Release()
		ownerOld.Release()
	}()
	old.appendMessage(retainedOld, 10, 10)
	old.fromTimeTick = tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	assert.True(t, newWriteOnlyFlushPolicy(0, 0, time.Millisecond).ShouldFlush(old, start), "age threshold reached")
}
