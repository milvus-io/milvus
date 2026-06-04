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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// TestWindowManagerConcurrentObserveAndPersist exercises the windowManager lock
// model under real concurrency. The existing recovery tests are single-threaded,
// so neither -race nor Go's map-access checker can catch a missing windowManager.mu
// acquisition; this test runs the three production paths against each other:
//
//   - observe: rs.ObserveMessage takes rs.mu then windowManager.mu and creates
//     windows (writes m.windows);
//   - persist: the window persistence helpers take windowManager.mu alone and
//     read/mutate the same window state;
//   - dirty-check: rs.consumeDirtySnapshot takes rs.mu then windowManager.mu via
//     canPersistConsumeCheckpoint.
//
// If observe failed to hold windowManager.mu, the concurrent map write/read on
// m.windows would panic outright ("concurrent map read and map write"); -race
// additionally flags any unsynchronized field access. Because every path takes
// rs.mu before windowManager.mu (and windowManager never takes rs.mu), the lock
// order cannot invert, so the test also serves as a deadlock check.
func TestWindowManagerConcurrentObserveAndPersist(t *testing.T) {
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.windowManager.resetIdempotencyWindows()
	rs.windowManager.markActiveViewsInitialized()
	rs.windowManager.setNormalMode()

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

	// observe path — rs.mu -> windowManager.mu, writes m.windows.
	go func() {
		defer wg.Done()
		for _, msg := range msgs {
			_ = rs.ObserveMessage(context.Background(), msg)
		}
	}()

	// window persistence path — windowManager.mu alone.
	go func() {
		defer wg.Done()
		for i := 0; i < persistRounds; i++ {
			cp := &WALCheckpoint{MessageID: rmq.NewRmqID(int64(i + 1)), TimeTick: uint64(i + 1)}
			rs.windowManager.ensurePendingIdempotencyPersistSnapshot()
			rs.windowManager.clearPendingIdempotencyPersistSnapshot()
			rs.windowManager.consumeIdempotencySnapshot()
			rs.windowManager.markVChannelWindowsPersisted(nil, nil, uint64(i), cp)
			rs.windowManager.markConsumeCheckpointPersisted(cp)
		}
	}()

	// dirty-check path — rs.mu -> windowManager.mu via canPersistConsumeCheckpoint.
	go func() {
		defer wg.Done()
		for i := 0; i < persistRounds; i++ {
			rs.consumeDirtySnapshot()
		}
	}()

	wg.Wait()
	require.Len(t, rs.windowManager.idempotencyWindows(), collections)
}
