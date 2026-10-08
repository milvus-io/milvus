package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A secondary cluster is force-promoted while a replicated SplitShard broadcast
// is incomplete there. fixIncompleteBroadcastsForForcePromote must re-drive the
// missing replicas through the normal broadcast path and so reproduce the
// primary's two-phase order: the append-first source lands and is persisted
// (AckPartial) before any other replica is appended, the ack callback runs
// exactly once, and it sees the source's SplitShardExtraResponse -- from the
// replicated ack when the source had already landed, from the re-driven append
// when it had not.

const (
	fpSplitSource      = "p0_1v0"
	fpSplitTargetA     = "p1_1v1"
	fpSplitTargetB     = "p2_1v2"
	fpSplitControl     = "p0_vcchan"
	fpSplitBroadcastID = uint64(50)
	fpPromoteID        = uint64(100)
	// The switch tick the secondary's own source StreamingNode reported when the
	// replicated fence landed there, before the promotion.
	fpReplicatedSwitchTick = uint64(90)
	// Ticks of replicas that landed on the secondary before the promotion. The
	// fake WAL of the promoted cluster allocates from fpPromotedTickBase, above
	// every one of them, as one TSO would.
	fpReplicatedTickBase = uint64(200)
	fpPromotedTickBase   = uint64(1000)
)

var fpSplitVChannels = []string{fpSplitSource, fpSplitTargetA, fpSplitTargetB, fpSplitControl}

// fpSplitBroadcast is the primary's SplitShard broadcast as the secondary holds
// it: source append-first, the collection and cluster keys a split takes.
func fpSplitBroadcast() message.BroadcastMutableMessage {
	return createNewSplitShardBroadcastMsg(fpSplitVChannels, fpSplitSource).
		OverwriteBroadcastHeader(fpSplitBroadcastID,
			message.NewExclusiveCollectionNameResourceKey("db", "c1"),
			message.NewSharedClusterResourceKey())
}

func fpSwitchExtra(t *testing.T, switchTimeTick uint64) *anypb.Any {
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: switchTimeTick})
	require.NoError(t, err)
	return extra
}

// fpSecondaryRecordOf is the WAL record the secondary's StreamingNode consumed
// for one replicated replica and acked to its coord: it carries the replicate
// header, and the source's record carries the extra append response its node
// stamped.
func fpSecondaryRecordOf(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(timetick))
	for _, replica := range fpSplitBroadcast().SplitIntoMutableMessage() {
		if replica.VChannel() != vchannel {
			continue
		}
		if vchannel == fpSplitSource {
			message.SetAppendExtra(replica, fpSwitchExtra(t, fpReplicatedSwitchTick))
		}
		return replica.WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "primary",
			MessageID:              id,
			LastConfirmedMessageID: id,
			TimeTick:               timetick,
			VChannel:               vchannel,
		}).WithTimeTick(timetick).
			WithLastConfirmed(id).
			IntoImmutableMessage(id)
	}
	panic("vchannel is not one of the split broadcast's own")
}

// fpReplicatedSplitTask builds, through the secondary's real ack path, the
// persisted REPLICATED task of the split broadcast after the given replicas
// landed there (in the given order; the first creates the task).
func fpReplicatedSplitTask(t *testing.T, landed []string) *streamingpb.BroadcastTask {
	require.NotEmpty(t, landed, "a replicated task exists only once a replica has been acked")
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptMixCoordClient(f))

	records := make([]message.ImmutableMessage, 0, len(landed))
	for i, vchannel := range landed {
		records = append(records, fpSecondaryRecordOf(t, vchannel, fpReplicatedTickBase+uint64(i)))
	}
	task := newBroadcastTaskFromImmutableMessage(records[0], newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())
	for _, record := range records {
		require.NoError(t, task.Ack(context.Background(), record))
	}
	require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, task.State())
	return proto.Clone(task.task).(*streamingpb.BroadcastTask)
}

// fpAppended is one replica the promoted cluster's WAL was asked to append.
type fpAppended struct {
	call              int
	broadcastID       uint64
	vchannel          string
	timetick          uint64
	replicated        bool // still carries a replicate header
	staleAppendExtra  bool // carries an extra append response nobody produced
	freshTimeTick     bool
	sourcePersistedAt bool // the catalog already held the source's checkpoint and extra
	failed            bool
}

// fpCatalog records the last persisted image of every broadcast task.
type fpCatalog struct {
	mu         sync.Mutex
	latest     map[uint64]*streamingpb.BroadcastTask
	tombstoned *typeutil.ConcurrentSet[uint64]
}

func (c *fpCatalog) save(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.mu.Lock()
	c.latest[broadcastID] = proto.Clone(task).(*streamingpb.BroadcastTask)
	c.mu.Unlock()
	if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
		c.tombstoned.Insert(broadcastID)
	}
	return nil
}

// persistedCheckpoint returns the persisted checkpoint of vchannel of the split
// broadcast, or nil.
func (c *fpCatalog) persistedCheckpoint(vchannel string) *streamingpb.AckedCheckpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.latest[fpSplitBroadcastID]
	if !ok {
		return nil
	}
	msg := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties)
	idx := findIdxOfVChannel(vchannel, msg.BroadcastHeader().VChannels)
	if idx < 0 || idx >= len(task.AckedCheckpoints) {
		return nil
	}
	cp := task.AckedCheckpoints[idx]
	if cp == nil || cp.TimeTick == 0 {
		return nil
	}
	return cp
}

// snapshot returns the persisted images to recover a restarted coord from,
// falling back to the given originals for tasks never re-saved.
func (c *fpCatalog) snapshot(originals []*streamingpb.BroadcastTask) []*streamingpb.BroadcastTask {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*streamingpb.BroadcastTask, 0, len(originals))
	for _, original := range originals {
		id := message.NewBroadcastMutableMessageBeforeAppend(original.Message.Payload, original.Message.Properties).BroadcastHeader().BroadcastID
		if saved, ok := c.latest[id]; ok {
			out = append(out, proto.Clone(saved).(*streamingpb.BroadcastTask))
			continue
		}
		out = append(out, proto.Clone(original).(*streamingpb.BroadcastTask))
	}
	return out
}

// fpWAL is the promoted cluster's WAL. It hands out strictly increasing ticks
// in append order, stamps the source replica's response with the first-fence
// switch tick, and can refuse every non-source replica of the split to stop a
// promotion half-way.
type fpWAL struct {
	streaming.WALAccesser
	t       *testing.T
	catalog *fpCatalog

	mu       sync.Mutex
	nextTick uint64
	calls    int
	appended []fpAppended
	failRest bool
}

func (w *fpWAL) setFailRest(fail bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failRest = fail
}

func (w *fpWAL) records() []fpAppended {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]fpAppended(nil), w.appended...)
}

func (w *fpWAL) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
	for i, msg := range msgs {
		rec := fpAppended{
			call:             w.calls,
			broadcastID:      msg.BroadcastHeader().BroadcastID,
			vchannel:         msg.VChannel(),
			replicated:       msg.ReplicateHeader() != nil,
			staleAppendExtra: message.AppendExtraOf(msg) != nil,
			freshTimeTick:    msg.MessageType().IsFreshTimeTick(),
		}
		cp := w.catalog.persistedCheckpoint(fpSplitSource)
		rec.sourcePersistedAt = cp != nil && cp.Extra != nil
		if rec.broadcastID == fpSplitBroadcastID && rec.vchannel != fpSplitSource && w.failRest {
			rec.failed = true
			w.appended = append(w.appended, rec)
			resps.Responses[i] = types.AppendResponse{Error: errors.New("wal unavailable")}
			continue
		}
		w.nextTick++
		rec.timetick = w.nextTick
		id := walimplstest.NewTestMessageID(int64(w.nextTick))
		result := &types.AppendResult{MessageID: id, LastConfirmedMessageID: id, TimeTick: w.nextTick}
		if rec.broadcastID == fpSplitBroadcastID && rec.vchannel == fpSplitSource {
			// This is the first fence of the source on this cluster.
			result.Extra = fpSwitchExtra(w.t, w.nextTick)
		}
		w.appended = append(w.appended, rec)
		resps.Responses[i] = types.AppendResponse{AppendResult: result}
	}
	return resps
}

// fpCallbackRecorder records every run of the SplitShard ack callback.
type fpCallbackRecorder struct {
	mu      sync.Mutex
	results []map[string]*message.AppendResult
}

func (r *fpCallbackRecorder) runs() []map[string]*message.AppendResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]*message.AppendResult(nil), r.results...)
}

// initForcePromoteSplitShardGlobals is initForcePromoteTestGlobals plus a
// recording SplitShard ack callback.
func initForcePromoteSplitShardGlobals(t *testing.T) *fpCallbackRecorder {
	initForcePromoteTestGlobals(t)
	recorder := &fpCallbackRecorder{}
	registry.RegisterSplitShardV2AckCallback(func(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		recorder.results = append(recorder.results, result.Results)
		return nil
	})
	return recorder
}

// startPromotedCoord recovers a broadcaster from the given persisted tasks, the
// way a coord restarts on the cluster that is being force-promoted.
func startPromotedCoord(t *testing.T, tasks []*streamingpb.BroadcastTask, catalog *fpCatalog, wal *fpWAL) Broadcaster {
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).Return(tasks, nil).Times(1)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(catalog.save).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))
	streaming.SetWALForTest(wal)
	// What the coord recovers from is, by definition, what is persisted.
	catalog.mu.Lock()
	for _, task := range tasks {
		id := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties).BroadcastHeader().BroadcastID
		catalog.latest[id] = proto.Clone(task).(*streamingpb.BroadcastTask)
	}
	catalog.mu.Unlock()

	bc, err := RecoverBroadcaster(context.Background())
	require.NoError(t, err)
	return bc
}

func newFPCatalog() *fpCatalog {
	return &fpCatalog{latest: make(map[uint64]*streamingpb.BroadcastTask), tombstoned: typeutil.NewConcurrentSet[uint64]()}
}

func splitRecords(records []fpAppended) []fpAppended {
	out := make([]fpAppended, 0, len(records))
	for _, r := range records {
		if r.broadcastID == fpSplitBroadcastID {
			out = append(out, r)
		}
	}
	return out
}

// assertRedrivenAsPrimary asserts every re-driven replica left as a message of
// this cluster: no replicate header (the now-primary's replicate interceptor
// would refuse one), no extra append response inherited from the replicated
// record, and a FreshTimeTick type, so the StreamingNode takes its tick from a
// fresh TSO call.
func assertRedrivenAsPrimary(t *testing.T, records []fpAppended) {
	t.Helper()
	for _, r := range records {
		assert.False(t, r.replicated, "re-driven replica %s still carries the replicate header", r.vchannel)
		assert.False(t, r.staleAppendExtra, "re-driven replica %s carries a stale extra append response", r.vchannel)
		assert.True(t, r.freshTimeTick, "a SplitShard replica must take a fresh time tick")
	}
}

// assertSwitchTick asserts the callback's source result carries the given
// first-fence switch tick.
func assertSwitchTick(t *testing.T, results map[string]*message.AppendResult, want uint64) {
	t.Helper()
	source := results[fpSplitSource]
	require.NotNil(t, source)
	require.NotNil(t, source.Extra, "the callback must see the source's extra append response")
	resp := &message.SplitShardExtraResponse{}
	require.NoError(t, source.Extra.UnmarshalTo(resp))
	assert.Equal(t, want, resp.GetSplitTimeTick())
}

// assertSourceTickBelowTheRest asserts premise (a) of the append gate on the
// callback's results: the source's tick is strictly below every other replica's.
func assertSourceTickBelowTheRest(t *testing.T, results map[string]*message.AppendResult) {
	t.Helper()
	require.Len(t, results, len(fpSplitVChannels))
	for vchannel, result := range results {
		if vchannel == fpSplitSource {
			continue
		}
		assert.Less(t, results[fpSplitSource].TimeTick, result.TimeTick, "source tick must be below %s", vchannel)
	}
}

// TestForcePromoteSplitShardSourceNotLanded: the source replica never landed on
// the secondary. After the promotion the source is appended alone and persisted
// before any other replica is appended; the callback sees the switch tick of
// that re-driven append.
func TestForcePromoteSplitShardSourceNotLanded(t *testing.T) {
	recorder := initForcePromoteSplitShardGlobals(t)

	// Only the control channel landed. (A secondary running the append gate
	// never lands a non-source replica before the source is acked, so this state
	// exists only for replicas appended before the gate; the re-drive must still
	// order it.)
	split := fpReplicatedSplitTask(t, []string{fpSplitControl})
	promote := createReplicatedForcePromoteTask(fpPromoteID, []string{"cc_vcchan", "v1", "v2"}, []string{"cc_vcchan", "v1", "v2"})

	catalog := newFPCatalog()
	wal := &fpWAL{t: t, catalog: catalog, nextTick: fpPromotedTickBase}
	bc := startPromotedCoord(t, []*streamingpb.BroadcastTask{split, promote}, catalog, wal)
	defer bc.Close()

	require.Eventually(t, func() bool {
		return catalog.tombstoned.Contain(fpSplitBroadcastID) && catalog.tombstoned.Contain(fpPromoteID)
	}, 30*time.Second, 10*time.Millisecond)

	records := splitRecords(wal.records())
	require.Len(t, records, 3, "source, both targets; the control channel had landed")
	assertRedrivenAsPrimary(t, records)

	// Phase one: the source, alone in its call, before the persisted checkpoint.
	assert.Equal(t, fpSplitSource, records[0].vchannel)
	assert.False(t, records[0].sourcePersistedAt)
	for _, r := range records[1:] {
		assert.NotEqual(t, fpSplitSource, r.vchannel, "the source is appended once")
		assert.Greater(t, r.call, records[0].call, "no other replica shares the source's append call")
		assert.True(t, r.sourcePersistedAt, "%s was appended before the source's AckPartial was persisted", r.vchannel)
	}
	assert.ElementsMatch(t, []string{fpSplitTargetA, fpSplitTargetB}, []string{records[1].vchannel, records[2].vchannel})

	runs := recorder.runs()
	require.Len(t, runs, 1, "the ack callback runs once")
	assertSwitchTick(t, runs[0], records[0].timetick)
	// The control channel's tick is the one replicated before the promotion; the
	// other two came after the source here.
	assert.Less(t, runs[0][fpSplitSource].TimeTick, runs[0][fpSplitTargetA].TimeTick)
	assert.Less(t, runs[0][fpSplitSource].TimeTick, runs[0][fpSplitTargetB].TimeTick)
}

// TestForcePromoteSplitShardSourceLanded: the source replica landed and was
// acked on the secondary, with its extra append response persisted, but not
// every target did. The promotion re-drives exactly the missing replicas, never
// the source, and the callback sees the switch tick persisted from the
// replicated ack.
func TestForcePromoteSplitShardSourceLanded(t *testing.T) {
	cases := []struct {
		name    string
		landed  []string
		missing []string
	}{
		{
			name:    "only_source_landed",
			landed:  []string{fpSplitSource},
			missing: []string{fpSplitTargetA, fpSplitTargetB, fpSplitControl},
		},
		{
			// The control channel landed too, so the callback is already
			// scheduled and waiting, holding the split's keys, when the promotion
			// starts.
			name:    "one_target_missing",
			landed:  []string{fpSplitSource, fpSplitControl, fpSplitTargetA},
			missing: []string{fpSplitTargetB},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := initForcePromoteSplitShardGlobals(t)

			split := fpReplicatedSplitTask(t, tc.landed)
			promote := createReplicatedForcePromoteTask(fpPromoteID, []string{"cc_vcchan", "v1", "v2"}, []string{"cc_vcchan", "v1", "v2"})

			catalog := newFPCatalog()
			wal := &fpWAL{t: t, catalog: catalog, nextTick: fpPromotedTickBase}
			bc := startPromotedCoord(t, []*streamingpb.BroadcastTask{split, promote}, catalog, wal)
			defer bc.Close()

			require.Eventually(t, func() bool {
				return catalog.tombstoned.Contain(fpSplitBroadcastID) && catalog.tombstoned.Contain(fpPromoteID)
			}, 30*time.Second, 10*time.Millisecond)

			records := splitRecords(wal.records())
			assertRedrivenAsPrimary(t, records)
			redriven := make([]string, 0, len(records))
			for _, r := range records {
				redriven = append(redriven, r.vchannel)
				assert.True(t, r.sourcePersistedAt)
			}
			assert.ElementsMatch(t, tc.missing, redriven, "exactly the missing replicas are re-driven")

			runs := recorder.runs()
			require.Len(t, runs, 1, "the ack callback runs once")
			assertSwitchTick(t, runs[0], fpReplicatedSwitchTick)
			assert.Equal(t, fpReplicatedTickBase, runs[0][fpSplitSource].TimeTick, "the source result is the replicated ack's")
			assertSourceTickBelowTheRest(t, runs[0])
		})
	}
}

// TestForcePromoteSplitShardRestartMidway: the promoted coord lands and
// persists the source, then stops before any other replica lands. The restarted
// coord re-runs the promotion's fix, does not append the source again, appends
// the rest, and the callback -- which runs once across both lives -- sees the
// switch tick persisted by the first life's AckPartial.
func TestForcePromoteSplitShardRestartMidway(t *testing.T) {
	recorder := initForcePromoteSplitShardGlobals(t)

	split := fpReplicatedSplitTask(t, []string{fpSplitControl})
	promote := createReplicatedForcePromoteTask(fpPromoteID, []string{"cc_vcchan", "v1", "v2"}, []string{"cc_vcchan", "v1", "v2"})
	originals := []*streamingpb.BroadcastTask{split, promote}

	// First life: every non-source replica of the split is refused.
	catalog := newFPCatalog()
	wal := &fpWAL{t: t, catalog: catalog, nextTick: fpPromotedTickBase, failRest: true}
	bc := startPromotedCoord(t, originals, catalog, wal)
	require.Eventually(t, func() bool {
		failedRest := false
		for _, r := range splitRecords(wal.records()) {
			failedRest = failedRest || r.failed
		}
		return failedRest && catalog.persistedCheckpoint(fpSplitSource) != nil
	}, 30*time.Second, 10*time.Millisecond)
	bc.Close()

	firstLife := splitRecords(wal.records())
	require.NotEmpty(t, firstLife)
	assert.Equal(t, fpSplitSource, firstLife[0].vchannel)
	sourceTick := firstLife[0].timetick
	for _, r := range firstLife[1:] {
		assert.NotEqual(t, fpSplitSource, r.vchannel, "the source is appended once in the first life")
		assert.True(t, r.failed)
		assert.True(t, r.sourcePersistedAt, "%s was attempted before the source's AckPartial was persisted", r.vchannel)
	}
	require.False(t, catalog.tombstoned.Contain(fpSplitBroadcastID))
	require.False(t, catalog.tombstoned.Contain(fpPromoteID))
	require.Empty(t, recorder.runs(), "the callback cannot run before every replica landed")

	// Second life: recovered from what the first one persisted.
	persisted := catalog.snapshot(originals)
	catalog2 := newFPCatalog()
	wal2 := &fpWAL{t: t, catalog: catalog2, nextTick: sourceTick + fpPromotedTickBase}
	bc2 := startPromotedCoord(t, persisted, catalog2, wal2)
	defer bc2.Close()

	require.Eventually(t, func() bool {
		return catalog2.tombstoned.Contain(fpSplitBroadcastID) && catalog2.tombstoned.Contain(fpPromoteID)
	}, 30*time.Second, 10*time.Millisecond)

	secondLife := splitRecords(wal2.records())
	assertRedrivenAsPrimary(t, secondLife)
	redriven := make([]string, 0, len(secondLife))
	for _, r := range secondLife {
		redriven = append(redriven, r.vchannel)
	}
	assert.ElementsMatch(t, []string{fpSplitTargetA, fpSplitTargetB}, redriven, "the restarted coord does not append the source again")

	runs := recorder.runs()
	require.Len(t, runs, 1, "the ack callback runs once across both lives")
	assertSwitchTick(t, runs[0], sourceTick)
	assert.Equal(t, sourceTick, runs[0][fpSplitSource].TimeTick)
	assert.Less(t, runs[0][fpSplitSource].TimeTick, runs[0][fpSplitTargetA].TimeTick)
	assert.Less(t, runs[0][fpSplitSource].TimeTick, runs[0][fpSplitTargetB].TimeTick)
}
