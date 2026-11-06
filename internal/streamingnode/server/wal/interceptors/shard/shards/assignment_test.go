package shards

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newAssignmentTestManager(t *testing.T, observers ...func(utils.SealSegmentSignal)) (*shardManagerImpl, []*segmentAllocManager) {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: "shared-assignment", Term: 1}
	operator := mock_utils.NewMockSealOperator(t)
	operator.EXPECT().Channel().Return(channel).Maybe()
	operator.EXPECT().AsyncFlushSegment(mock.Anything).Run(func(signal utils.SealSegmentSignal) {
		for _, observe := range observers {
			observe(signal)
		}
	}).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(operator, nil, nil)
	metrics := metricsutil.NewSegmentAssignMetrics(channel.Name)
	t.Cleanup(metrics.Close)
	manager := &shardManagerImpl{ctx: t.Context(), pchannel: channel, metrics: metrics, partitionManagers: make(map[PartitionUniqueKey]*partitionManager)}
	var segments []*segmentAllocManager
	for _, partition := range []int64{2, 3} {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: partition, SegmentId: partition, MaxRows: 100, MaxSegmentSize: 1000}, 100)
		pm := &partitionManager{ctx: t.Context(), pchannel: channel, collectionID: 1, partitionID: partition, segments: map[int64]*segmentAllocManager{partition: segment}, metrics: metrics, onAllocating: make(chan struct{})}
		pm.SetLogger(mlog.With())
		manager.partitionManagers[PartitionUniqueKey{CollectionID: 1, PartitionID: partition}] = pm
		segments = append(segments, segment)
	}
	return manager, segments
}

func assignmentRequest(partition int64) *AssignSegmentRequest {
	return &AssignSegmentRequest{CollectionID: 1, PartitionID: partition, TimeTick: 200, ModifiedMetrics: stats.ModifiedMetrics{Rows: 5, BinarySize: 50}, RuntimeFlushSize: 100}
}

func TestAssignmentSharedTimeTickOrder(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			manager, segments := newAssignmentTestManager(t)
			segments[0].Flush(policy.PolicyCapacity())
			req := assignmentRequest(2)
			req.TimeTick = 100
			var err error
			if batch {
				_, err = manager.AssignSegments([]*AssignSegmentRequest{req})
			} else {
				_, err = manager.AssignSegment(req)
			}
			require.ErrorIs(t, err, ErrTimeTickTooOld)
			require.Zero(t, segments[0].AckSem())
		})
	}
}

func TestAssignmentFailureLeavesNoPartialAccounting(t *testing.T) {
	manager, segments := newAssignmentTestManager(t)
	first, second := assignmentRequest(2), assignmentRequest(3)
	pm := manager.partitionManagers[PartitionUniqueKey{CollectionID: 1, PartitionID: 3}]
	pm.fencedAssignTimeTick = 200
	_, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
	require.ErrorIs(t, err, ErrFencedAssign)
	state := resource.Resource().SegmentStatsManager().GetStatsOfSegment(2)
	require.Zero(t, state.Modified.Rows)
	require.Zero(t, state.Modified.BinarySize)
	require.Zero(t, state.RuntimeFlushSize)
	require.Zero(t, segments[0].AckSem())
	second.TimeTick = 201
	results, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
	require.NoError(t, err)
	for index, result := range results {
		require.EqualValues(t, 1, segments[index].AckSem())
		result.Ack()
		require.Zero(t, segments[index].AckSem())
		state := resource.Resource().SegmentStatsManager().GetStatsOfSegment(segments[index].GetSegmentID())
		require.EqualValues(t, 5, state.Modified.Rows)
		require.EqualValues(t, 50, state.Modified.BinarySize)
		require.EqualValues(t, 100, state.RuntimeFlushSize)
	}
}

func TestAssignmentBatchFailureReleasesAllAcks(t *testing.T) {
	manager, segments := newAssignmentTestManager(t)
	// Duplicate targets fail the authoritative batch check after both Acks exist.
	_, err := manager.AssignSegments([]*AssignSegmentRequest{assignmentRequest(2), assignmentRequest(2)})
	require.ErrorIs(t, err, ErrWaitForNewSegment)
	require.Zero(t, segments[0].AckSem())
	require.Zero(t, segments[0].TxnSem())
	require.Zero(t, resource.Resource().SegmentStatsManager().GetStatsOfSegment(2).Modified.Rows)
	result, err := manager.AssignSegment(assignmentRequest(2))
	require.NoError(t, err)
	result.Ack()
}

func TestPrepareAssignmentDoesNotAcquireReferencesOrAccount(t *testing.T) {
	manager, segments := newAssignmentTestManager(t)
	req := assignmentRequest(2)
	req.TxnSession = &mockedSession{}
	manager.mu.Lock()
	selected, err := manager.partitionManagers[PartitionUniqueKey{CollectionID: 1, PartitionID: 2}].prepareAssignment(req)
	manager.mu.Unlock()
	require.NoError(t, err)
	require.Same(t, segments[0], selected)
	require.Zero(t, selected.AckSem())
	require.Zero(t, selected.TxnSem())
	state := resource.Resource().SegmentStatsManager().GetStatsOfSegment(2)
	require.Zero(t, state.Modified.Rows)
	require.Zero(t, state.Modified.BinarySize)
	require.Zero(t, state.RuntimeFlushSize)
}

func TestAssignmentAccountingFailureReleasesDistinctAcks(t *testing.T) {
	manager, segments := newAssignmentTestManager(t)
	calls := 0
	patch := mockey.Mock((*stats.StatsManager).AllocRowsBatch).To(func(_ *stats.StatsManager, batch []stats.Allocation) error {
		calls++
		require.Len(t, batch, 2)
		for _, segment := range segments {
			require.EqualValues(t, 1, segment.AckSem(), "all references precede accounting")
			require.Zero(t, segment.TxnSem())
		}
		return stats.ErrNotEnoughSpace
	}).Build()
	first, second := assignmentRequest(2), assignmentRequest(3)
	first.TxnSession, second.TxnSession = &mockedSession{}, &mockedSession{}
	_, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
	patch.UnPatch()
	require.ErrorIs(t, err, ErrWaitForNewSegment)
	require.Equal(t, 1, calls)
	for _, segment := range segments {
		require.Zero(t, segment.AckSem())
		require.Zero(t, segment.TxnSem())
		require.Zero(t, resource.Resource().SegmentStatsManager().GetStatsOfSegment(segment.GetSegmentID()).Modified.Rows)
	}
}

func TestAssignmentCapacitySealingWaitsForAllAcks(t *testing.T) {
	observed := make(chan utils.SealSegmentSignal, 2)
	manager, segments := newAssignmentTestManager(t, func(signal utils.SealSegmentSignal) { observed <- signal })
	var original func(*stats.StatsManager, []stats.Allocation) error
	patch := mockey.Mock((*stats.StatsManager).AllocRowsBatch).Origin(&original).To(func(m *stats.StatsManager, batch []stats.Allocation) error {
		err := original(m, batch)
		for range 2 {
			select {
			case signal := <-observed:
				require.Equal(t, policy.PolicyNameCapacity, signal.SealPolicy.Policy)
				segment := segments[int(signal.SegmentBelongs.SegmentID)-2]
				require.EqualValues(t, 1, segment.AckSem())
				worker := &segmentFlushWorker{segment: segment}
				worker.SetLogger(mlog.With())
				require.False(t, worker.checkIfReady(), "a capacity notification cannot flush before append Ack")
			case <-time.After(5 * time.Second):
				t.Fatal("capacity notification was not delivered")
			}
		}
		return err
	}).Build()
	defer patch.UnPatch()
	first, second := assignmentRequest(2), assignmentRequest(3)
	first.ModifiedMetrics.Rows, second.ModifiedMetrics.Rows = 101, 101
	results, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
	require.NoError(t, err)
	for index, result := range results {
		result.Ack()
		worker := &segmentFlushWorker{segment: segments[index]}
		worker.SetLogger(mlog.With())
		require.True(t, worker.checkIfReady())
	}
}

func TestAssignmentTransactionReferencesFollowSessionLifecycle(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		for _, outcome := range []string{"commit", "rollback", "expired", "abort", "rejected", "shutdown"} {
			origin := "live"
			if recovered {
				origin = "recovered"
			}
			t.Run(origin+"/"+outcome, func(t *testing.T) {
				manager, segments := newAssignmentTestManager(t)
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 1}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(100)
				txnManager := txn.NewTxnManager(manager.pchannel, nil)
				var session *txn.TxnSession
				count := uint64(1)
				if recovered {
					txnManager.Close()
					txnCtx := message.TxnContext{TxnID: 5, Keepalive: time.Millisecond}
					immutable := begin.WithTxnContext(txnCtx).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
					builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(immutable))
					body := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(150).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(2))
					builder.Add(body)
					txnManager = txn.NewTxnManager(manager.pchannel, map[message.TxnID]*message.ImmutableTxnMessageBuilder{5: builder})
					var err error
					session, err = txnManager.GetSessionOfTxn(5)
					require.NoError(t, err)
					count++
				} else {
					var err error
					session, err = txnManager.BeginNewTxn(t.Context(), message.MustAsMutableBeginTxnMessageV2(begin))
					require.NoError(t, err)
				}
				defer txnManager.Close()
				require.NoError(t, session.AddNewMessage(t.Context(), 200))
				first, second := assignmentRequest(2), assignmentRequest(3)
				first.TxnSession, second.TxnSession = session, session
				results, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
				require.NoError(t, err)
				for index, result := range results {
					require.EqualValues(t, 1, segments[index].AckSem())
					require.EqualValues(t, 1, segments[index].TxnSem())
					result.Ack()
					require.Zero(t, segments[index].AckSem())
					worker := &segmentFlushWorker{segment: segments[index]}
					worker.SetLogger(mlog.With())
					require.False(t, worker.checkIfReady(), "body Ack cannot release a transaction reference")
				}
				session.AddNewMessageDoneAndKeepalive(200, message.MessageTypeInsert)
				switch outcome {
				case "commit", "rejected":
					require.NoError(t, session.RequestCommitAndWait(t.Context(), 201, count))
					if outcome == "commit" {
						session.CommitDone()
					} else {
						session.RejectCommit()
					}
					txnManager.CleanupTxnUntil(201)
				case "rollback":
					require.NoError(t, session.RequestRollback(t.Context(), 201))
					session.RollbackDone()
					txnManager.CleanupTxnUntil(201)
				case "expired":
					txnManager.CleanupTxnUntil(1 << 30)
				case "abort":
					txnManager.FailTxnAtVChannel("p_1v0")
				case "shutdown":
					txnManager.Close()
				}
				for _, segment := range segments {
					require.Zero(t, segment.TxnSem())
					worker := &segmentFlushWorker{segment: segment}
					worker.SetLogger(mlog.With())
					require.True(t, worker.checkIfReady())
				}
				select {
				case <-txnManager.RecoverDone():
				default:
					t.Fatal("recovered transaction cleanup did not release the recovery barrier")
				}
				require.NoError(t, txnManager.GracefulClose(context.Background()))
			})
		}
	}
}

func TestAssignmentLaterPartitionFailuresCanRetryWithoutDoubleCharge(t *testing.T) {
	for _, failure := range []string{"missing", "waiting", "old"} {
		t.Run(failure, func(t *testing.T) {
			manager, segments := newAssignmentTestManager(t)
			key := PartitionUniqueKey{CollectionID: 1, PartitionID: 3}
			pm := manager.partitionManagers[key]
			first, second := assignmentRequest(2), assignmentRequest(3)
			var want error
			switch failure {
			case "missing":
				delete(manager.partitionManagers, key)
				want = ErrPartitionNotFound
			case "waiting":
				pm.segments = map[int64]*segmentAllocManager{}
				want = ErrWaitForNewSegment
			case "old":
				second.TimeTick = 100
				want = ErrTimeTickTooOld
			}
			_, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
			require.ErrorIs(t, err, want)
			for _, segment := range segments {
				require.Zero(t, segment.AckSem())
				state := resource.Resource().SegmentStatsManager().GetStatsOfSegment(segment.GetSegmentID())
				require.Zero(t, state.Modified.Rows)
				require.Zero(t, state.Modified.BinarySize)
				require.Zero(t, state.RuntimeFlushSize)
			}
			manager.partitionManagers[key] = pm
			pm.segments = map[int64]*segmentAllocManager{3: segments[1]}
			second.TimeTick = 201
			results, err := manager.AssignSegments([]*AssignSegmentRequest{first, second})
			require.NoError(t, err)
			for index, result := range results {
				result.Ack()
				state := resource.Resource().SegmentStatsManager().GetStatsOfSegment(segments[index].GetSegmentID())
				require.EqualValues(t, 5, state.Modified.Rows)
				require.EqualValues(t, 50, state.Modified.BinarySize)
				require.EqualValues(t, 100, state.RuntimeFlushSize)
			}
		})
	}
}
