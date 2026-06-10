package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestReadIgnoresDelayedPublishForRetainedEntries(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"}).(*transformLog)
	entry := testTransformLogEntry(50)
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 0, Entries: []*streamingpb.TransformLogEntry{entry}},
	}

	scanner := transformLog.Read(context.Background(), transformlogapi.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()

	transformLog.publish([]*streamingpb.TransformLogEntry{entry})

	entryEvent := <-scanner.Chan()
	require.NotNil(t, entryEvent.Entry)
	assert.Equal(t, uint64(50), entryEvent.Entry.GetTimeTick())
	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)

	select {
	case event := <-scanner.Chan():
		t.Fatalf("unexpected duplicate event: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReadBuffersLiveEntriesUntilCaughtUp(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"}).(*transformLog)
	entries := make([]*streamingpb.TransformLogEntry, 0, 20)
	for timeTick := uint64(1); timeTick <= 20; timeTick++ {
		entries = append(entries, testTransformLogEntry(timeTick))
	}
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 0, Entries: entries},
	}

	scanner := transformLog.Read(context.Background(), transformlogapi.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 0,
	})
	defer scanner.Close()

	require.Eventually(t, func() bool {
		return len(scanner.Chan()) == 16
	}, time.Second, 10*time.Millisecond)
	transformLog.publish([]*streamingpb.TransformLogEntry{testTransformLogEntry(21)})

	for expected := uint64(1); expected <= 20; expected++ {
		event := <-scanner.Chan()
		require.NotNil(t, event.Entry)
		assert.Equal(t, expected, event.Entry.GetTimeTick())
	}
	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)
	liveEvent := <-scanner.Chan()
	require.NotNil(t, liveEvent.Entry)
	assert.Equal(t, uint64(21), liveEvent.Entry.GetTimeTick())
}

func TestSnapshotChunksCopiesSliceOnly(t *testing.T) {
	first := &streamingpb.TransformLogChunk{ChunkId: 1}
	second := &streamingpb.TransformLogChunk{ChunkId: 2}
	chunks := []*streamingpb.TransformLogChunk{first}

	snapshot := snapshotChunks(chunks)
	require.Len(t, snapshot, 1)
	assert.Same(t, first, snapshot[0])

	chunks[0] = second
	assert.Same(t, first, snapshot[0])
}

func testTransformLogEntry(timeTick uint64) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{TimeTick: timeTick}
}
