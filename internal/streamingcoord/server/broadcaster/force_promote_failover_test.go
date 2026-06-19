package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// appendBehavior defines how the mock WAL should respond to an AppendMessages call.
type appendBehavior struct {
	handler func(ctx context.Context, msgs []message.MutableMessage) types.AppendResponses
}

// appendRecord records metadata about a single supplemented message.
type appendRecord struct {
	BroadcastID uint64
	VChannel    string
	MessageType message.MessageType
}

// walController provides a programmable mock WAL for testing.
// It queues appendBehavior functions that are consumed in order per call.
// When the queue is exhausted, it falls back to default (all success + auto ack).
type walController struct {
	t           *testing.T
	mu          sync.Mutex
	idCounter   atomic.Int64
	appendCount atomic.Int64 // counts successful appends
	behaviors   []appendBehavior
	broadcaster *syncutil.Future[Broadcaster]

	// appendRecords tracks metadata of all successfully appended messages.
	appendRecordsMu sync.Mutex
	appendRecords   []appendRecord
}

func newWALController(t *testing.T, broadcaster *syncutil.Future[Broadcaster]) *walController {
	return &walController{
		t:           t,
		idCounter:   *atomic.NewInt64(1000),
		broadcaster: broadcaster,
	}
}

func (w *walController) setBehaviors(behaviors []appendBehavior) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.behaviors = behaviors
}

// nextBehavior pops the next queued behavior, or returns nil if exhausted.
func (w *walController) nextBehavior() *appendBehavior {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.behaviors) == 0 {
		return nil
	}
	b := w.behaviors[0]
	w.behaviors = w.behaviors[1:]
	return &b
}

func (w *walController) recordAppend(msg message.MutableMessage) {
	w.appendRecordsMu.Lock()
	defer w.appendRecordsMu.Unlock()
	w.appendRecords = append(w.appendRecords, appendRecord{
		BroadcastID: msg.BroadcastHeader().BroadcastID,
		VChannel:    msg.VChannel(),
		MessageType: msg.MessageType(),
	})
}

// getAppendRecords returns a copy of all append records.
func (w *walController) getAppendRecords() []appendRecord {
	w.appendRecordsMu.Lock()
	defer w.appendRecordsMu.Unlock()
	result := make([]appendRecord, len(w.appendRecords))
	copy(result, w.appendRecords)
	return result
}

// defaultAppend is the default WAL behavior: all success + auto ack.
func (w *walController) defaultAppend(ctx context.Context, msgs []message.MutableMessage) types.AppendResponses {
	resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
	for i, msg := range msgs {
		newID := walimplstest.NewTestMessageID(w.idCounter.Inc())
		resps.Responses[i] = types.AppendResponse{
			AppendResult: &types.AppendResult{
				MessageID: newID,
				TimeTick:  uint64(time.Now().UnixMilli()),
			},
		}
		w.appendCount.Inc()
		w.recordAppend(msg)

		// Auto ack: simulate streaming node consuming and acking the message
		broadcastID := msg.BroadcastHeader().BroadcastID
		vchannel := msg.VChannel()
		go func() {
			time.Sleep(10 * time.Millisecond)
			w.ackMessage(broadcastID, vchannel)
		}()
	}
	return resps
}

func (w *walController) ackMessage(broadcastID uint64, vchannel string) {
	bc := w.broadcaster.Get()
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{vchannel}).
		MustBuildBroadcast().
		WithBroadcastID(broadcastID).
		SplitIntoMutableMessage()[0].
		WithTimeTick(100).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(w.idCounter.Inc()))

	for {
		if err := bc.Ack(context.Background(), msg); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// handleAppend is the main entry point called by the mock WAL.
func (w *walController) handleAppend(ctx context.Context, msgs []message.MutableMessage) types.AppendResponses {
	if b := w.nextBehavior(); b != nil {
		return b.handler(ctx, msgs)
	}
	return w.defaultAppend(ctx, msgs)
}

// setupMockWAL registers the walController as the mock WAL.
func (w *walController) setupMockWAL(t *testing.T) {
	mw := mock_streaming.NewMockWALAccesser(t)
	f := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		return w.handleAppend(ctx, msgs)
	}
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f).Maybe()
	streaming.SetWALForTest(mw)
}

// catalogRecord records a single SaveBroadcastTask call.
type catalogRecord struct {
	BroadcastID uint64
	State       streamingpb.BroadcastTaskState
}

// forcePromoteTestEnv holds the test environment for force promote failover tests.
type forcePromoteTestEnv struct {
	t           *testing.T
	broadcaster Broadcaster
	walCtrl     *walController
	catalog     *mock_metastore.MockStreamingCoordCataLog
	tombstoned  *typeutil.ConcurrentSet[uint64]

	// catalogRecordsMu protects catalogRecords.
	catalogRecordsMu sync.Mutex
	catalogRecords   []catalogRecord
}

// initForcePromoteTestGlobals sets up global state (paramtable, balance, registry) once
// at the top level of TestForcePromoteFailover to avoid race conditions between sub-tests.
func initForcePromoteTestGlobals(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()
	balance.ResetBalancer()
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("10ms")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("2")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("20ms")

	// Register dummy ack callbacks for all message types used in tests
	registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
		return nil
	})
	registry.RegisterAlterReplicateConfigV2AckCallback(func(ctx context.Context, msg message.BroadcastResultAlterReplicateConfigMessageV2) error {
		return nil
	})
	registry.RegisterCreateCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultCreateCollectionMessageV1) error {
		return nil
	})
	registry.RegisterCreatePartitionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultCreatePartitionMessageV1) error {
		return nil
	})
	registry.RegisterDropPartitionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropPartitionMessageV1) error {
		return nil
	})

	// Mock balancer: secondary cluster (for force promote)
	mb := mock_balancer.NewMockBalancer(t)
	mb.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary).Maybe()
	mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	balance.Register(mb)
}

// setupForcePromoteTest creates a full test environment with real broadcaster and mock WAL.
func setupForcePromoteTest(
	t *testing.T,
	recoveryTasks []*streamingpb.BroadcastTask,
	walBehaviors []appendBehavior,
) *forcePromoteTestEnv {
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().ListBroadcastTask(mock.Anything).
		Return(recoveryTasks, nil).Times(1)

	tombstoned := typeutil.NewConcurrentSet[uint64]()
	env := &forcePromoteTestEnv{
		t:          t,
		tombstoned: tombstoned,
		catalog:    catalog,
	}

	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			env.catalogRecordsMu.Lock()
			env.catalogRecords = append(env.catalogRecords, catalogRecord{
				BroadcastID: broadcastID,
				State:       bt.State,
			})
			env.catalogRecordsMu.Unlock()
			if bt.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
				tombstoned.Insert(broadcastID)
			}
			return nil
		}).Maybe()

	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptMixCoordClient(f))

	bcFuture := syncutil.NewFuture[Broadcaster]()
	walCtrl := newWALController(t, bcFuture)
	if len(walBehaviors) > 0 {
		walCtrl.setBehaviors(walBehaviors)
	}
	walCtrl.setupMockWAL(t)

	bc, err := RecoverBroadcaster(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, bc)
	bcFuture.Set(bc)

	env.broadcaster = bc
	env.walCtrl = walCtrl
	return env
}

func (env *forcePromoteTestEnv) close() {
	env.broadcaster.Close()
	time.Sleep(50 * time.Millisecond)
}

// waitForTombstone waits until the given broadcastIDs all reach TOMBSTONE state.
func (env *forcePromoteTestEnv) waitForTombstone(t *testing.T, timeout time.Duration, broadcastIDs ...uint64) {
	assert.Eventually(t, func() bool {
		for _, id := range broadcastIDs {
			if !env.tombstoned.Contain(id) {
				return false
			}
		}
		return true
	}, timeout, 10*time.Millisecond)
}

// getCatalogRecords returns a copy of all catalog save records.
func (env *forcePromoteTestEnv) getCatalogRecords() []catalogRecord {
	env.catalogRecordsMu.Lock()
	defer env.catalogRecordsMu.Unlock()
	result := make([]catalogRecord, len(env.catalogRecords))
	copy(result, env.catalogRecords)
	return result
}

// countCatalogStateTransitions counts how many times a broadcastID transitioned to a given state.
func (env *forcePromoteTestEnv) countCatalogStateTransitions(broadcastID uint64, state streamingpb.BroadcastTaskState) int {
	count := 0
	for _, r := range env.getCatalogRecords() {
		if r.BroadcastID == broadcastID && r.State == state {
			count++
		}
	}
	return count
}

// --- Helper functions for creating recovery tasks ---

// buildBitmapFromAckedVChannels builds a bitmap aligned to the actual vchannel order in the broadcast header.
// WithBroadcast may reorder vchannels (due to Set deduplication), so bitmaps must be built
// from the header's actual order, not the caller's input order.
func buildBitmapFromAckedVChannels(msg message.BroadcastMutableMessage, ackedVChannels []string) []byte {
	headerVChannels := msg.BroadcastHeader().VChannels
	ackedSet := typeutil.NewSet(ackedVChannels...)
	bitmap := make([]byte, len(headerVChannels))
	for i, vc := range headerVChannels {
		if ackedSet.Contain(vc) {
			bitmap[i] = 1
		}
	}
	return bitmap
}

// createReplicatedDropCollectionTask creates a REPLICATED DropCollection task.
func createReplicatedDropCollectionTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg(vchannels).WithBroadcastID(broadcastID)
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(msg,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createReplicatedCreateCollectionTask creates a REPLICATED CreateCollection task.
func createReplicatedCreateCollectionTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg, err := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(
		msg.WithBroadcastID(broadcastID),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createReplicatedCreatePartitionTask creates a REPLICATED CreatePartition task.
func createReplicatedCreatePartitionTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg, err := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(
		msg.WithBroadcastID(broadcastID),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createReplicatedDropPartitionTask creates a REPLICATED DropPartition task.
func createReplicatedDropPartitionTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg, err := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{}).
		WithBody(&msgpb.DropPartitionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(
		msg.WithBroadcastID(broadcastID),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createReplicatedAlterReplicateConfigTask creates a REPLICATED AlterReplicateConfig task.
func createReplicatedAlterReplicateConfigTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg := createAlterReplicateConfigBroadcastMsg(vchannels, false).WithBroadcastID(broadcastID)
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(msg,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createReplicatedForcePromoteTask creates a REPLICATED force promote task.
func createReplicatedForcePromoteTask(broadcastID uint64, vchannels []string, ackedVChannels []string) *streamingpb.BroadcastTask {
	msg := createAlterReplicateConfigBroadcastMsg(vchannels, true).WithBroadcastID(broadcastID)
	bitmap := buildBitmapFromAckedVChannels(msg, ackedVChannels)
	return createNewWaitAckBroadcastTaskFromMessage(msg,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		bitmap)
}

// createPendingForcePromoteTask creates a PENDING force promote task (not yet broadcast).
func createPendingForcePromoteTask(broadcastID uint64, vchannels []string) *streamingpb.BroadcastTask {
	msg := createAlterReplicateConfigBroadcastMsg(vchannels, true).WithBroadcastID(broadcastID)
	pb := msg.IntoMessageProto()
	headerVChannels := msg.BroadcastHeader().VChannels
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
		State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		AckedVchannelBitmap: make([]byte, len(headerVChannels)),
	}
}

// --- Test Cases ---

func TestForcePromoteFailover(t *testing.T) {
	initForcePromoteTestGlobals(t)

	// Control channel: "cc_vcchan"; data channels: "v1", "v2"
	vchannels := []string{"cc_vcchan", "v1", "v2"}

	t.Run("no_incomplete_tasks", func(t *testing.T) {
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedForcePromoteTask(100, vchannels, vchannels), // all acked
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 100)

		// Verify: no WAL appends for fix (no incomplete tasks)
		assert.Equal(t, int64(0), env.walCtrl.appendCount.Load())
		// Verify: force promote task reached TOMBSTONE in catalog
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(100,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
		// Verify: no append records at all
		assert.Empty(t, env.walCtrl.getAppendRecords())
	})

	t.Run("fix_incomplete_drop_collection", func(t *testing.T) {
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedDropCollectionTask(10, vchannels, []string{"cc_vcchan"}), // cc acked, v1/v2 pending
				createReplicatedForcePromoteTask(100, vchannels, vchannels),              // all acked
			},
			nil,
		)
		defer env.close()

		// Wait for ALL tasks to complete, not just force promote
		env.waitForTombstone(t, 30*time.Second, 10, 100)

		// Verify: exactly 2 pending messages supplemented (v1, v2 of Task 10)
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(2))
		// Verify: supplemented messages target the correct vchannels and broadcastID
		records := env.walCtrl.getAppendRecords()
		supplementedForTask10 := filterRecords(records, 10)
		assert.GreaterOrEqual(t, len(supplementedForTask10), 2)
		supplementedVChannels := collectVChannels(supplementedForTask10)
		assert.Contains(t, supplementedVChannels, "v1")
		assert.Contains(t, supplementedVChannels, "v2")
		// Verify: supplemented messages are DropCollection type
		for _, r := range supplementedForTask10 {
			assert.Equal(t, message.MessageTypeDropCollection, r.MessageType)
		}
		// Verify: catalog persisted TOMBSTONE for both tasks
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(10,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(100,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
	})

	t.Run("fix_alter_replicate_config_with_ignore", func(t *testing.T) {
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedAlterReplicateConfigTask(10, vchannels, []string{"cc_vcchan", "v1"}), // cc,v1 acked, v2 pending
				createReplicatedDropCollectionTask(20, vchannels, []string{"cc_vcchan"}),             // cc acked, v1/v2 pending
				createReplicatedForcePromoteTask(100, vchannels, vchannels),                          // all acked
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 20, 100)

		// Verify: all 3 tasks reached TOMBSTONE (full lifecycle completed)
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(10,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(20,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
		assert.GreaterOrEqual(t, env.countCatalogStateTransitions(100,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1)
		// Verify: supplemented messages include AlterReplicateConfig + DropCollection
		records := env.walCtrl.getAppendRecords()
		alterRecords := filterRecords(records, 10)
		dropRecords := filterRecords(records, 20)
		assert.GreaterOrEqual(t, len(alterRecords), 1, "AlterReplicateConfig pending msg should be supplemented")
		assert.GreaterOrEqual(t, len(dropRecords), 2, "DropCollection pending v1,v2 should be supplemented")
		// Verify: message types are correct
		for _, r := range alterRecords {
			assert.Equal(t, message.MessageTypeAlterReplicateConfig, r.MessageType)
		}
		for _, r := range dropRecords {
			assert.Equal(t, message.MessageTypeDropCollection, r.MessageType)
		}
		// Verify: DropCollection supplemented on v1 and v2
		dropVCs := collectVChannels(dropRecords)
		assert.Contains(t, dropVCs, "v1")
		assert.Contains(t, dropVCs, "v2")
	})

	t.Run("wal_partial_failure_with_retry", func(t *testing.T) {
		callCount := atomic.NewInt32(0)
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedDropCollectionTask(10, vchannels, []string{"cc_vcchan"}),
				createReplicatedForcePromoteTask(100, vchannels, vchannels),
			},
			[]appendBehavior{
				{
					handler: func(ctx context.Context, msgs []message.MutableMessage) types.AppendResponses {
						callCount.Inc()
						resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
						for i := range msgs {
							resps.Responses[i] = types.AppendResponse{
								Error: errors.New("wal unavailable"),
							}
						}
						return resps
					},
				},
			},
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 100)
		assert.GreaterOrEqual(t, callCount.Load(), int32(1))
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(2))
	})

	t.Run("pending_force_promote_broadcast_then_fix", func(t *testing.T) {
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedDropCollectionTask(10, vchannels, nil), // none acked
				createPendingForcePromoteTask(100, vchannels),
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 100)

		// Force promote broadcast (3) + fix Task 10 (3) = 6 total
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(6))
		// Verify: both force promote (ID=100) and DropCollection (ID=10) messages appended
		records := env.walCtrl.getAppendRecords()
		assert.GreaterOrEqual(t, len(filterRecords(records, 100)), 3, "force promote should be broadcast to 3 vchannels")
		assert.GreaterOrEqual(t, len(filterRecords(records, 10)), 3, "DropCollection should be supplemented to 3 vchannels")
	})

	t.Run("context_canceled_exits_cleanly", func(t *testing.T) {
		blockCh := make(chan struct{})
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				// Force promote in PENDING state: WAL append blocks, then Close() cancels context.
				// No incomplete tasks — tests that Close() doesn't hang when WAL is blocked.
				createPendingForcePromoteTask(100, vchannels),
			},
			[]appendBehavior{
				{
					handler: func(ctx context.Context, msgs []message.MutableMessage) types.AppendResponses {
						select {
						case <-ctx.Done():
						case <-blockCh:
						}
						resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
						for i := range msgs {
							resps.Responses[i] = types.AppendResponse{Error: errors.New("context canceled")}
						}
						return resps
					},
				},
			},
		)

		done := make(chan struct{})
		go func() {
			env.broadcaster.Close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			close(blockCh)
			t.Fatal("broadcaster.Close() hung — possible goroutine leak")
		}
	})

	t.Run("mixed_incomplete_tasks_ordering", func(t *testing.T) {
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedAlterReplicateConfigTask(10, vchannels, []string{"cc_vcchan", "v1"}), // cc,v1 acked, v2 pending
				createReplicatedDropCollectionTask(20, vchannels, []string{"cc_vcchan"}),             // cc acked, v1/v2 pending
				createReplicatedDropCollectionTask(30, vchannels, []string{"cc_vcchan"}),             // cc acked, v1/v2 pending
				createReplicatedForcePromoteTask(100, vchannels, vchannels),                          // all acked
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 20, 30, 100)

		// Pending: Task 10 v2 (1) + Task 20 v1,v2 (2) + Task 30 v1,v2 (2) = 5
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(5))
		// Verify: records cover all 3 incomplete broadcastIDs
		records := env.walCtrl.getAppendRecords()
		broadcastIDsFound := collectBroadcastIDs(records)
		assert.Contains(t, broadcastIDsFound, uint64(10))
		assert.Contains(t, broadcastIDsFound, uint64(20))
		assert.Contains(t, broadcastIDsFound, uint64(30))
	})

	t.Run("complex_multi_ddl_staggered_watermarks", func(t *testing.T) {
		// Complex scenario: 5 incomplete tasks of different DDL types with staggered ack watermarks.
		// WAL ordering constraint: on same vchannel, earlier broadcastID must ack before later one.
		//
		// Watermark analysis (per vchannel):
		//   cc_vcchan: all tasks acked (all have cc=0x01)
		//   v1: Task 10 acked, Task 15/20/25 pending → valid (watermark between 10 and 15)
		//   v2: Task 10/15 acked, Task 20/25 pending → valid (watermark between 15 and 20)
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedAlterReplicateConfigTask(10, vchannels, vchannels),               // cc,v1,v2 all acked → not incomplete
				createReplicatedCreateCollectionTask(15, vchannels, []string{"cc_vcchan", "v2"}), // cc,v2 acked, v1 pending
				createReplicatedDropCollectionTask(20, vchannels, []string{"cc_vcchan"}),         // cc acked, v1/v2 pending
				createReplicatedCreatePartitionTask(25, vchannels, []string{"cc_vcchan"}),        // cc acked, v1/v2 pending
				createReplicatedDropPartitionTask(30, vchannels, []string{"cc_vcchan"}),          // cc acked, v1/v2 pending
				createReplicatedForcePromoteTask(100, vchannels, vchannels),                      // all acked
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 15, 20, 25, 30, 100)

		records := env.walCtrl.getAppendRecords()

		// Task 10: fully acked → NOT supplemented (not incomplete)
		assert.Empty(t, filterRecords(records, 10), "fully acked task should not be supplemented")

		// Task 15 (CreateCollection): v1 pending → at least 1 msg supplemented
		task15Records := filterRecords(records, 15)
		assert.GreaterOrEqual(t, len(task15Records), 1, "CreateCollection should have pending msgs supplemented")
		for _, r := range task15Records {
			assert.Equal(t, message.MessageTypeCreateCollection, r.MessageType)
		}

		// Task 20 (DropCollection): v1,v2 pending → at least 2 msgs supplemented
		task20Records := filterRecords(records, 20)
		assert.GreaterOrEqual(t, len(task20Records), 2, "DropCollection should have 2 pending msgs supplemented")
		for _, r := range task20Records {
			assert.Equal(t, message.MessageTypeDropCollection, r.MessageType)
		}

		// Task 25 (CreatePartition): v1,v2 pending → at least 2 msgs supplemented
		task25Records := filterRecords(records, 25)
		assert.GreaterOrEqual(t, len(task25Records), 2, "CreatePartition should have 2 pending msgs supplemented")
		for _, r := range task25Records {
			assert.Equal(t, message.MessageTypeCreatePartition, r.MessageType)
		}

		// Task 30 (DropPartition): v1,v2 pending → at least 2 msgs supplemented
		task30Records := filterRecords(records, 30)
		assert.GreaterOrEqual(t, len(task30Records), 2, "DropPartition should have 2 pending msgs supplemented")
		for _, r := range task30Records {
			assert.Equal(t, message.MessageTypeDropPartition, r.MessageType)
		}

		// Total: 1 + 2 + 2 + 2 = 7 supplemented messages
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(7))

		// Verify: catalog has TOMBSTONE for ALL tasks (full lifecycle)
		for _, id := range []uint64{10, 15, 20, 25, 30, 100} {
			assert.GreaterOrEqual(t, env.countCatalogStateTransitions(id,
				streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE), 1,
				"task %d should reach TOMBSTONE", id)
		}
	})

	t.Run("multiple_alter_replicate_configs_all_marked_ignore", func(t *testing.T) {
		// Two AlterReplicateConfig tasks (pre-failover config changes) + other DDL.
		// Both AlterReplicateConfig should be marked ignore=true and supplemented.
		// The DDL tasks should be supplemented normally.
		//
		// Watermark:
		//   v1: Task 10 acked, Task 12/20 pending → valid
		//   v2: Task 10 acked, Task 12/20 pending → valid
		env := setupForcePromoteTest(t,
			[]*streamingpb.BroadcastTask{
				createReplicatedAlterReplicateConfigTask(10, vchannels, vchannels),             // fully acked → not incomplete
				createReplicatedAlterReplicateConfigTask(12, vchannels, []string{"cc_vcchan"}), // cc acked, v1/v2 pending
				createReplicatedCreateCollectionTask(20, vchannels, []string{"cc_vcchan"}),     // cc acked, v1/v2 pending
				createReplicatedDropPartitionTask(25, vchannels, []string{"cc_vcchan"}),        // cc acked, v1/v2 pending
				createReplicatedForcePromoteTask(100, vchannels, vchannels),                    // all acked
			},
			nil,
		)
		defer env.close()

		env.waitForTombstone(t, 30*time.Second, 10, 12, 20, 25, 100)

		records := env.walCtrl.getAppendRecords()

		// Task 10: fully acked → NOT supplemented
		assert.Empty(t, filterRecords(records, 10))

		// Task 12 (AlterReplicateConfig): v1,v2 pending → 2 msgs, marked ignore
		task12Records := filterRecords(records, 12)
		assert.GreaterOrEqual(t, len(task12Records), 2)
		for _, r := range task12Records {
			assert.Equal(t, message.MessageTypeAlterReplicateConfig, r.MessageType)
		}

		// Task 20 (CreateCollection): v1,v2 pending → 2 msgs
		task20Records := filterRecords(records, 20)
		assert.GreaterOrEqual(t, len(task20Records), 2)
		for _, r := range task20Records {
			assert.Equal(t, message.MessageTypeCreateCollection, r.MessageType)
		}

		// Task 25 (DropPartition): v1,v2 pending → 2 msgs
		task25Records := filterRecords(records, 25)
		assert.GreaterOrEqual(t, len(task25Records), 2)
		for _, r := range task25Records {
			assert.Equal(t, message.MessageTypeDropPartition, r.MessageType)
		}

		// Total: 2 + 2 + 2 = 6 supplemented
		assert.GreaterOrEqual(t, env.walCtrl.appendCount.Load(), int64(6))

		// Verify: catalog recorded saves for all incomplete tasks (ignore marking, ack checkpoints, etc.)
		catalogRecords := env.getCatalogRecords()
		savedBroadcastIDs := make(map[uint64]bool)
		for _, r := range catalogRecords {
			savedBroadcastIDs[r.BroadcastID] = true
		}
		assert.True(t, savedBroadcastIDs[12], "AlterReplicateConfig task 12 should be saved (ignore marking)")
	})
}

func TestForcePromoteLockContention(t *testing.T) {
	// Verifies that doForcePromoteFixIncompleteBroadcasts succeeds even when the
	// resource key lock is temporarily held by another goroutine (e.g., a concurrent
	// ack callback for a replicated DropLoadConfig). Previously this panicked with
	// "FastLock failed during force promote with zero contenders".
	initForcePromoteTestGlobals(t)

	vchannels := []string{"cc_vcchan", "v1", "v2"}

	// An incomplete DropCollection task holds the Cluster resource key lock
	// via its ack callback. The force promote task needs the same lock.
	env := setupForcePromoteTest(t,
		[]*streamingpb.BroadcastTask{
			createReplicatedDropCollectionTask(10, vchannels, []string{"cc_vcchan"}), // cc acked, v1/v2 pending
			createReplicatedForcePromoteTask(100, vchannels, vchannels),              // all acked
		},
		nil,
	)
	defer env.close()

	// Both tasks should complete without panic. Task 10's ack callback holds the
	// resource key lock while running; force promote must wait for it instead of panicking.
	env.waitForTombstone(t, 30*time.Second, 10, 100)
}

// --- Record filtering helpers ---

func filterRecords(records []appendRecord, broadcastID uint64) []appendRecord {
	var result []appendRecord
	for _, r := range records {
		if r.BroadcastID == broadcastID {
			result = append(result, r)
		}
	}
	return result
}

func collectVChannels(records []appendRecord) map[string]struct{} {
	result := make(map[string]struct{})
	for _, r := range records {
		result[r.VChannel] = struct{}{}
	}
	return result
}

func collectBroadcastIDs(records []appendRecord) map[uint64]struct{} {
	result := make(map[uint64]struct{})
	for _, r := range records {
		result[r.BroadcastID] = struct{}{}
	}
	return result
}
