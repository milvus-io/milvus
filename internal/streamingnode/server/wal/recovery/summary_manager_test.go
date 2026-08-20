package recovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestEvictPersistedEntriesInNormalMode(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, summaryEvictionConfig{})
	manager.setNormalMode()
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300, 400})
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})

	// In normal mode the staging buffer is released once its contents are in a
	// chunk: the interceptor window, not this summary, answers live dedup.
	manager.evictPersistedEntries()

	require.Empty(t, state.entries)
}

func TestEvictPersistedEntriesNoOpInRecoveryMode(t *testing.T) {
	manager := newSummaryManager("p1", 0, &config{idempotencyEnabled: true}, nil, summaryEvictionConfig{})
	state := newEmptyVChannelSummary("p1", "v1", &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	populateSummaryEntries(state, []uint64{100, 200, 300})
	manager.setSummaries(map[string]*vchannelSummary{"v1": state})

	// During recovery the entries ARE the set being rebuilt for the consumer, so
	// nothing may be released until the handover is done.
	manager.evictPersistedEntries()

	require.Len(t, state.entries, 3)
}

// --- helpers ---

func populateSummaryEntries(state *vchannelSummary, timeticks []uint64) {
	for i, tt := range timeticks {
		key := fmt.Sprintf("key-%d", i)
		record := (&streamingpb.SummaryEntry{SourceMessageId: rmq.NewRmqID(int64(tt)).IntoProto(), SourceTimetick: tt, Idempotency: &streamingpb.IdempotencyContent{Key: key}})
		state.applySummaryEntry(record, false)
	}
}

func populateSummaryEntriesWithBaseTT(state *vchannelSummary, baseTT uint64, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		tt := baseTT + uint64(i)
		record := (&streamingpb.SummaryEntry{SourceMessageId: rmq.NewRmqID(int64(tt)).IntoProto(), SourceTimetick: tt, Idempotency: &streamingpb.IdempotencyContent{Key: key}})
		state.applySummaryEntry(record, false)
	}
}

func makeRecord(key string, timetick uint64) *streamingpb.SummaryEntry {
	return &streamingpb.SummaryEntry{SourceTimetick: timetick, Idempotency: &streamingpb.IdempotencyContent{Key: key}}
}

func buildTimeTickMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	msg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				Timestamp: timetick,
			},
		}).
		WithAllVChannel().
		BuildMutable()
	require.NoError(t, err)
	return msg.
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick) - 1)).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}
