package recovery

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// TestSummaryManagerConcurrentObserveAndPersist exercises the summaryManager lock
// model under real concurrency. The existing recovery tests are single-threaded,
// so neither -race nor Go's map-access checker can catch a missing summaryManager.mu
// acquisition; this test runs the three production paths against each other:
//
//   - observe: rs.ObserveMessage takes rs.mu then summaryManager.mu and creates
//     summaries (writes m.vchannelSummaries);
//   - persist: the summary persistence helpers take summaryManager.mu alone and
//     read/mutate the same summary state;
//   - dirty-check: rs.consumeDirtySnapshot takes rs.mu then summaryManager.mu via
//     canPersistConsumeCheckpoint.
//
// If observe failed to hold summaryManager.mu, the concurrent map write/read on
// m.vchannelSummaries would panic outright ("concurrent map read and map write"); -race
// additionally flags any unsynchronized field access. Because every path takes
// rs.mu before summaryManager.mu (and summaryManager never takes rs.mu), the lock
// order cannot invert, so the test also serves as a deadlock check.
func TestSummaryManagerConcurrentObserveAndPersist(t *testing.T) {
	enableRecoveryIdempotency(t)
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.summaryManager.resetSummaries()
	rs.summaryManager.markActiveViewsInitialized()
	rs.summaryManager.setNormalMode()

	// Pre-build the observe messages on the test goroutine: require.* (and the
	// builders' Must* helpers) must not run from a child goroutine.
	const collections = 300
	msgs := make([]message.ImmutableMessage, 0, collections)
	for i := 0; i < collections; i++ {
		tt := int64(10 + i)
		msgs = append(msgs, message.NewCreateCollectionMessageBuilderV1().
			WithVChannel(fmt.Sprintf("v%d", i)).
			WithHeader(&message.CreateCollectionMessageHeader{
				CollectionId: int64(100 + i),
				PartitionIds: []int64{int64(1000 + i)},
			}).
			WithBody(&msgpb.CreateCollectionRequest{}).
			MustBuildMutable().
			WithTimeTick(uint64(tt)).
			WithLastConfirmed(rmq.NewRmqID(tt-1)).
			IntoImmutableMessage(rmq.NewRmqID(tt)))
	}

	const persistRounds = 3000
	var wg sync.WaitGroup
	wg.Add(3)

	// observe path — rs.mu -> summaryManager.mu, writes m.vchannelSummaries.
	go func() {
		defer wg.Done()
		for _, msg := range msgs {
			_ = rs.ObserveMessage(context.Background(), msg)
		}
	}()

	// summary persistence path — summaryManager.mu alone.
	go func() {
		defer wg.Done()
		for i := 0; i < persistRounds; i++ {
			cp := &WALCheckpoint{MessageID: rmq.NewRmqID(int64(i + 1)), TimeTick: uint64(i + 1)}
			rs.summaryManager.ensurePendingIdempotencyPersistSnapshot()
			rs.summaryManager.clearPendingIdempotencyPersistSnapshot()
			rs.summaryManager.consumeIdempotencySnapshot()
			rs.summaryManager.markVChannelSummariesPersisted(nil, nil, uint64(i), cp)
			rs.summaryManager.markConsumeCheckpointPersisted(cp)
		}
	}()

	// dirty-check path — rs.mu -> summaryManager.mu via canPersistConsumeCheckpoint.
	go func() {
		defer wg.Done()
		for i := 0; i < persistRounds; i++ {
			rs.consumeDirtySnapshot()
		}
	}()

	wg.Wait()
	require.Len(t, rs.summaryManager.summaries(), collections)
}

func TestSummaryManagerConcurrentIdleAdvanceAndTruncateClamp(t *testing.T) {
	enableRecoveryIdempotency(t)
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	rs.summaryManager.SetLogger(resource.Resource().Logger())
	rs.summaryManager.markActiveViewsInitialized()
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(&WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	catalogState.storeMeta = &streamingpb.PChannelSummaryMeta{
		Pchannel:                  "p1",
		SourceCheckpointMessageId: rmq.NewRmqID(1).IntoProto(),
		SourceCheckpointTimetick:  1,
		Term:                      rs.summaryManager.term,
	}

	const rounds = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 2; i < rounds; i++ {
			rs.summaryManager.mu.Lock()
			rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(&WALCheckpoint{
				MessageID: rmq.NewRmqID(int64(i)),
				TimeTick:  uint64(i),
			})
			rs.summaryManager.mu.Unlock()
			rs.summaryManager.advanceIdleSourceCheckpoint(ctx)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 2; i < rounds; i++ {
			_ = rs.summaryManager.truncateClampCheckpoint()
		}
	}()
	wg.Wait()
}
