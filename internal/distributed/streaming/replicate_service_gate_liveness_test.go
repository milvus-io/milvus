package streaming

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// TestReplicateAppendGateLivenessWithInterleavedStreams drives the append gate
// with the shape a secondary actually sees: one SplitShard broadcast whose
// replicas travel on two independent per-pchannel replicate streams, delivered
// in either order.
//
//	stream q0 (primary p0): source p0_1v0 @100, target p0_1v1 @101
//	stream q1 (primary p1): unrelated @99, target p1_1v2 @102, control p1_vcchan @103
//
// Each stream is in tick order, and every non-append-first replica's tick is
// above the source's, which is what the primary's two-phase broadcast
// guarantees. Under those facts the gate must never wedge: every replica lands,
// and the append-first source lands before every other replica of its
// broadcast, whichever stream arrives first -- including when the target on the
// source's own stream has to wait behind it, and when the other stream parks at
// the gate before the source's stream has delivered anything.
func TestReplicateAppendGateLivenessWithInterleavedStreams(t *testing.T) {
	for _, first := range []string{"q0", "q1"} {
		t.Run("stream "+first+" delivered first", func(t *testing.T) {
			f := newFakeSecondary()
			rs := newGateReplicateService(t, f)

			split := splitShardBroadcastWith(800)
			unrelated := message.NewDropCollectionMessageBuilderV1().
				WithHeader(&message.DropCollectionMessageHeader{CollectionId: 2}).
				WithBody(&msgpb.DropCollectionRequest{}).
				WithBroadcast([]string{"p1_2v0"}).
				MustBuildBroadcast().
				WithBroadcastID(799)
			streams := map[string][]message.ReplicateMutableMessage{
				"q0": {
					replicaAt(split, "p0_1v0", 100),
					replicaAt(split, "p0_1v1", 101),
				},
				"q1": {
					replicaAt(unrelated, "p1_2v0", 99),
					replicaAt(split, "p1_1v2", 102),
					replicaAt(split, "p1_vcchan", 103),
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			results := make(chan error, len(streams))
			// A replicate stream appends synchronously from its recv loop: the
			// next message is not handled until the previous append returns.
			runStream := func(msgs []message.ReplicateMutableMessage) {
				go func() {
					for _, msg := range msgs {
						if _, err := rs.Append(ctx, msg); err != nil {
							results <- err
							return
						}
					}
					results <- nil
				}()
			}

			runStream(streams[first])
			if first == "q1" {
				// The other stream parks at the gate before the source's stream
				// has delivered anything at all.
				require.Eventually(t, func() bool { return f.waitCalls(800) == 1 }, 5*time.Second, 10*time.Millisecond)
				assert.Equal(t, float64(1), gatedAppendsOn("q1"))
				assert.Equal(t, []string{"q1_2v0"}, f.appendOrder(), "the gated target must not land before its source")
			} else {
				require.Eventually(t, func() bool { return len(f.appendOrder()) == 2 }, 5*time.Second, 10*time.Millisecond)
			}
			if first == "q1" {
				runStream(streams["q0"])
			} else {
				runStream(streams["q1"])
			}

			for range streams {
				select {
				case err := <-results:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatalf("replication wedged at the append gate, appended so far: %v", f.appendOrder())
				}
			}

			order := f.appendOrder()
			assert.ElementsMatch(t, []string{"q1_2v0", "q0_1v0", "q0_1v1", "q1_1v2", "q1_vcchan"}, order)
			source := slices.Index(order, "q0_1v0")
			for _, rest := range []string{"q0_1v1", "q1_1v2", "q1_vcchan"} {
				assert.Less(t, source, slices.Index(order, rest),
					"the append-first source must land before %s, got %v", rest, order)
			}
			assert.Zero(t, gatedAppendsOn("q0"))
			assert.Zero(t, gatedAppendsOn("q1"))
		})
	}
}
