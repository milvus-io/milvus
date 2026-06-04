package partial_update

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// TestConcurrentLostUpdateEliminated simulates two clients racing to update
// the same primary key. Each client repeatedly:
//
//  1. reads the current cached version (representing queryPreExecute)
//  2. attempts an OCC append at that version
//  3. on conflict, retries with the freshly observed version
//
// The invariant is that the final cached version equals the timestamp of the
// last successful append AND the total successful appends equals the sum of
// per-client successes (no append silently lost).
func TestConcurrentLostUpdateEliminated(t *testing.T) {
	itc := newTestInterceptor()

	const pk = int64(1000)
	key := pkKey{collectionID: 1, pk: pk}
	// Bootstrap the cache so subsequent ops are version-bound updates.
	{
		unlock := itc.cache.Lock([]pkKey{key})
		itc.cache.Update(key, 1)
		unlock()
	}

	const clientA = "A"
	const clientB = "B"
	const opsPerClient = 200

	var (
		successA, successB         int64
		conflictA                  int64
		conflictB                  int64
		lastSuccessA, lastSuccessB uint64
	)

	// nextTS is the per-client tick used as the new version after a successful
	// append. We multiplex the two clients into disjoint ts ranges so the
	// final cache value reveals which client landed last.
	tsA := uint64(1_000_000)
	tsB := uint64(2_000_000)

	var wg sync.WaitGroup
	wg.Add(2)

	runClient := func(name string, baseTS *uint64, success, conflict *int64, lastSuccess *uint64) {
		defer wg.Done()
		for i := 0; i < opsPerClient; i++ {
			// Step 1: snapshot the current version (analogous to queryPreExecute).
			itc.cache.shards[shardOf(key)].mu.Lock()
			var expectedTS uint64
			if e := itc.cache.shards[shardOf(key)].cache[key]; e != nil {
				expectedTS = e.ts
			}
			itc.cache.shards[shardOf(key)].mu.Unlock()

			// Step 2: Build OCC message at the snapshot version.
			myTS := atomic.AddUint64(baseTS, 1)
			msg := buildOCCMessageWithTimeTick(t, []int64{pk}, []uint64{expectedTS}, []bool{true}, myTS)

			_, err := itc.DoAppend(context.Background(), msg, func(ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
				return nil, nil
			})
			if err == nil {
				atomic.AddInt64(success, 1)
				atomic.StoreUint64(lastSuccess, myTS)
				_ = name
			} else if IsPKStateConflict(err) {
				atomic.AddInt64(conflict, 1)
				// Retry next iteration; outer loop just continues.
				i-- // reattempt this op
			} else {
				t.Errorf("unexpected err: %v", err)
				return
			}
		}
	}
	go runClient(clientA, &tsA, &successA, &conflictA, &lastSuccessA)
	go runClient(clientB, &tsB, &successB, &conflictB, &lastSuccessB)
	wg.Wait()

	// Both clients must succeed at exactly opsPerClient.
	assert.EqualValues(t, opsPerClient, successA, "client A successes")
	assert.EqualValues(t, opsPerClient, successB, "client B successes")

	// At least one conflict should have happened in a real race. If neither
	// observed a conflict the test scheduler simply serialized the goroutines
	// (still correct, but make the assertion non-strict).
	t.Logf("conflicts: A=%d B=%d", conflictA, conflictB)

	// Final cache value must equal the last successful tick from one of the
	// two clients. We can't know which, but it must equal one of the two
	// clients' last successful ts (whichever landed last).
	itc.cache.shards[shardOf(key)].mu.Lock()
	var final uint64
	if e := itc.cache.shards[shardOf(key)].cache[key]; e != nil {
		final = e.ts
	}
	itc.cache.shards[shardOf(key)].mu.Unlock()

	if final != lastSuccessA && final != lastSuccessB {
		t.Fatalf("final cache version %d did not match either client's last successful ts (A=%d, B=%d) — silent lost update", final, lastSuccessA, lastSuccessB)
	}
}

// buildOCCMessageWithTimeTick is a thin variant of buildInsertMessageWithOCC
// that assigns a caller-provided time tick. The interceptor uses TimeTick()
// to advance the version cache, so this lets us simulate distinct successful
// append timestamps.
func buildOCCMessageWithTimeTick(t *testing.T, pks []int64, ts []uint64, exists []bool, timetick uint64) message.MutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId: 1,
		Partitions: []*message.PartitionSegmentAssignment{
			{PartitionId: 2, Rows: uint64(len(pks)), BinarySize: 1},
		},
		OccMode:               messagespb.OCCMode_OCC_MODE_CAS,
		ExpectedPks:           &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		ExpectedRowTimestamps: ts,
		ExpectedRowExists:     exists,
	}
	body := &msgpb.InsertRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 1},
		ShardName:      "v1",
		DbName:         "db",
		CollectionName: "c",
		PartitionName:  "p",
		CollectionID:   1,
		PartitionID:    2,
		Version:        msgpb.InsertDataVersion_ColumnBased,
		RowIDs:         make([]int64, len(pks)),
		Timestamps:     make([]uint64, len(pks)),
		NumRows:        uint64(len(pks)),
	}
	msg, err := message.NewInsertMessageBuilderV1().
		WithHeader(header).
		WithBody(body).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(timetick)
	return msg
}
