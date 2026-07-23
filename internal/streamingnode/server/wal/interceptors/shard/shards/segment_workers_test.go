//go:build test
// +build test

package shards

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type capturedShardScheduler struct {
	tasks []nodescheduler.Task
}

func (s *capturedShardScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return noopShardTaskHandle{}
}

type noopShardTaskHandle struct{}

func (noopShardTaskHandle) Cancel() {}

func (noopShardTaskHandle) Wait(context.Context) error { return nil }

func TestPartitionManagerSubmitsFlushAndAllocationToNodeScheduler(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	scheduler := &capturedShardScheduler{}
	walFuture := syncutil.NewFuture[wal.WAL]()
	walFuture.Set(mock_wal.NewMockWAL(t))
	channel := types.PChannelInfo{Name: "pchannel", Term: 1}
	operator := mock_utils.NewMockSealOperator(t)
	operator.EXPECT().Channel().Return(channel)
	operator.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(operator, nil, nil)
	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId: 1,
		PartitionId:  2,
		SegmentId:    1001,
	}, 100)
	segment.Flush(policy.PolicyCapacity())
	m := &partitionManager{
		ctx:          context.Background(),
		scheduler:    scheduler,
		txnManager:   &mockedTxnManager{},
		wal:          walFuture,
		vchannel:     "v1",
		collectionID: 1,
		partitionID:  2,
	}
	m.SetLogger(mlog.With())

	m.asyncFlushSegment(context.Background(), segment)
	m.asyncAllocSegment(7, true)

	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &segmentFlushWorker{}, scheduler.tasks[0])
	assert.IsType(t, &segmentAllocWorker{}, scheduler.tasks[1])
}

func TestSegmentFlushWorkerExecuteUsesErrDelay(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: "pchannel", Term: 1}
	operator := mock_utils.NewMockSealOperator(t)
	operator.EXPECT().Channel().Return(channel)
	operator.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(operator, nil, nil)

	t.Run("txn recovery", func(t *testing.T) {
		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().Return(make(chan struct{})).Maybe()
		w := &segmentFlushWorker{
			txnManager: &neverReadyTxnManager{},
			ctx:        context.Background(),
			wal:        mockWAL,
		}
		w.SetLogger(mlog.With())
		require.ErrorIs(t, w.Execute(context.Background()), nodescheduler.ErrDelay)
	})

	t.Run("ack precondition", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1001,
			MaxRows:        100,
			MaxSegmentSize: 100,
		}, 100)
		result, err := segment.AllocRows(&AssignSegmentRequest{TimeTick: 101, ModifiedMetrics: stats.ModifiedMetrics{Rows: 1, BinarySize: 1}})
		require.NoError(t, err)
		defer result.Ack()
		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().Return(make(chan struct{})).Maybe()
		w := &segmentFlushWorker{
			txnManager: &mockedTxnManager{},
			ctx:        context.Background(),
			segment:    segment,
			wal:        mockWAL,
		}
		w.SetLogger(mlog.With())
		require.ErrorIs(t, w.Execute(context.Background()), nodescheduler.ErrDelay)
	})

	t.Run("transient append", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1002,
			MaxRows:        100,
			MaxSegmentSize: 100,
		}, 100)
		segment.Flush(policy.PolicyCapacity())
		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().Return(make(chan struct{})).Maybe()
		mockWAL.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, errors.New("temporary append failure")).Once()
		w := &segmentFlushWorker{
			txnManager: &mockedTxnManager{},
			ctx:        context.Background(),
			vchannel:   "v1",
			segment:    segment,
			wal:        mockWAL,
		}
		w.SetLogger(mlog.With())
		require.ErrorIs(t, w.Execute(context.Background()), nodescheduler.ErrDelay)
	})
}

func TestSegmentAllocWorkerExecuteRetriesAndPreservesConfiguration(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	var attempts atomic.Int32
	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().Return(make(chan struct{})).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, message.MutableMessage) (*wal.AppendResult, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("temporary append failure")
		}
		return &wal.AppendResult{MessageID: rmq.NewRmqID(100), TimeTick: 200}, nil
	}).Twice()
	w := &segmentAllocWorker{
		ctx:          context.Background(),
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	require.ErrorIs(t, w.Execute(context.Background()), nodescheduler.ErrDelay)
	segmentID := w.segmentID
	storageVersion := w.storageVersion
	limitation := w.limitation
	require.NotZero(t, segmentID)

	require.NoError(t, w.Execute(context.Background()))
	assert.Equal(t, segmentID, w.segmentID)
	assert.Equal(t, storageVersion, w.storageVersion)
	assert.Equal(t, limitation, w.limitation)
}

// TestSegmentFlushWorker_RetryAfterAppendFailure tests that the flush worker
// regenerates the message after a failed append operation.
// This is a regression test for issue #47295 where the worker would panic
// with "wal term already set" when retrying after a network failure.
func TestSegmentFlushWorker_RetryAfterAppendFailure(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	// Create a test segment and mark it as flushed
	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	// Mark segment as flushed so SealPolicy() doesn't panic
	segment.Flush(policy.PolicyCapacity())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()

	// Track the number of append calls and messages received
	appendCount := atomic.Int32{}
	var firstMsg message.MutableMessage
	var secondMsg message.MutableMessage

	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			count := appendCount.Add(1)
			if count == 1 {
				// First call: store the message and simulate failure
				firstMsg = msg
				// Simulate WAL setting the term before failing (this is what happens in real WAL)
				msg.WithWALTerm(1)
				return nil, errors.New("simulated network failure")
			}
			// Second call: store the message and succeed
			secondMsg = msg
			return &wal.AppendResult{
				MessageID: rmq.NewRmqID(100),
				TimeTick:  200,
			}, nil
		})

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	// First call should fail
	err := w.doOnce()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated network failure")

	// Second call should succeed with a new message (not the contaminated one)
	err = w.doOnce()
	assert.NoError(t, err)

	// Verify that two different messages were used
	assert.NotNil(t, firstMsg)
	assert.NotNil(t, secondMsg)
	// The second message should NOT be the same as the first (contaminated) message
	assert.NotSame(t, firstMsg, secondMsg)

	// Verify append was called twice
	assert.Equal(t, int32(2), appendCount.Load())
}

// TestSegmentAllocWorker_RetryAfterAppendFailure tests that the alloc worker
// regenerates the message but preserves the segmentID after a failed append operation.
// This is a regression test for issue #47295 where the worker would panic
// with "wal term already set" when retrying after a network failure.
func TestSegmentAllocWorker_RetryAfterAppendFailure(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()

	// Track the number of append calls and messages received
	appendCount := atomic.Int32{}
	var firstMsg message.MutableMessage
	var secondMsg message.MutableMessage
	var firstSegmentID int64
	var secondSegmentID int64

	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			count := appendCount.Add(1)
			// Extract segment ID from the message header
			createSegmentMsg := message.MustAsImmutableCreateSegmentMessageV2(
				msg.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(1)),
			)
			segmentID := createSegmentMsg.Header().SegmentId

			if count == 1 {
				// First call: store the message and segment ID, then simulate failure
				firstMsg = msg
				firstSegmentID = segmentID
				// Simulate WAL setting the term before failing (this is what happens in real WAL)
				msg.WithWALTerm(1)
				return nil, errors.New("simulated network failure")
			}
			// Second call: store the message and segment ID, then succeed
			secondMsg = msg
			secondSegmentID = segmentID
			return &wal.AppendResult{
				MessageID: rmq.NewRmqID(100),
				TimeTick:  200,
			}, nil
		})

	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0, // Initially not allocated
	}
	w.SetLogger(mlog.With())

	// First call should fail
	err := w.doOnce()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated network failure")
	// segmentID should be preserved (not 0)
	assert.NotZero(t, w.segmentID)
	// storageVersion and limitation should also be preserved
	assert.NotZero(t, w.storageVersion)
	assert.NotEmpty(t, w.limitation.PolicyName)

	// Store the allocated segment ID for verification
	allocatedSegmentID := w.segmentID

	// Second call should succeed with a new message but the SAME segmentID
	err = w.doOnce()
	assert.NoError(t, err)

	// Verify that two different messages were used
	assert.NotNil(t, firstMsg)
	assert.NotNil(t, secondMsg)
	// The second message should NOT be the same as the first (contaminated) message
	assert.NotSame(t, firstMsg, secondMsg)

	// CRITICAL: Verify that the same segment ID was used in both messages
	assert.Equal(t, firstSegmentID, secondSegmentID, "segment ID should be preserved across retries")
	assert.Equal(t, int64(allocatedSegmentID), firstSegmentID, "segment ID in message should match allocated ID")

	// Verify append was called twice
	assert.Equal(t, int32(2), appendCount.Load())
}

// TestSegmentFlushWorker_DoOnceCheckIfReady tests the checkIfReady behavior
func TestSegmentFlushWorker_DoOnceCheckIfReady(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	// Create a test segment and mark it as flushed
	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	segment.Flush(policy.PolicyCapacity())

	ctx := context.Background()

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	// When segment is ready (no pending acks), it should try to flush
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			return &wal.AppendResult{
				MessageID: rmq.NewRmqID(100),
				TimeTick:  200,
			}, nil
		}).Once()

	err := w.doOnce()
	assert.NoError(t, err)
}

// TestSegmentAllocWorker_InitSegmentConfig tests segment config initialization
func TestSegmentAllocWorker_InitSegmentConfig(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	ctx := context.Background()

	mockWAL := mock_wal.NewMockWAL(t)

	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(mlog.With())

	// Initialize config - should allocate segment ID and set storageVersion/limitation
	err := w.initSegmentConfig()
	assert.NoError(t, err)
	assert.NotZero(t, w.segmentID)
	assert.NotZero(t, w.storageVersion)
	assert.NotEmpty(t, w.limitation.PolicyName)

	firstSegmentID := w.segmentID
	firstStorageVersion := w.storageVersion
	firstLimitation := w.limitation

	// Calling again should not reinitialize (segmentID is not 0)
	err = w.initSegmentConfig()
	assert.NoError(t, err)
	assert.Equal(t, firstSegmentID, w.segmentID)
	assert.Equal(t, firstStorageVersion, w.storageVersion)
	assert.Equal(t, firstLimitation, w.limitation)
}

func TestSegmentAllocWorkerStorageVersionFollowsRequirements(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	param := paramtable.Get()
	defer param.Reset(param.CommonCfg.UseLoonFFI.Key)

	for name, tc := range map[string]struct {
		useLoonFFI        string
		requiresStorageV3 bool
		expected          int64
	}{
		"v2_without_requirement": {useLoonFFI: "false", expected: storage.StorageV2},
		"v3_required_by_schema":  {useLoonFFI: "false", requiresStorageV3: true, expected: storage.StorageV3},
		"v3_with_ffi":            {useLoonFFI: "true", expected: storage.StorageV3},
	} {
		t.Run(name, func(t *testing.T) {
			param.Save(param.CommonCfg.UseLoonFFI.Key, tc.useLoonFFI)
			w := &segmentAllocWorker{
				ctx:               context.Background(),
				collectionID:      1,
				partitionID:       2,
				vchannel:          "v1",
				wal:               mock_wal.NewMockWAL(t),
				requiresStorageV3: tc.requiresStorageV3,
			}
			w.SetLogger(mlog.With())

			err := w.initSegmentConfig()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, w.storageVersion)
		})
	}
}

func TestTxnManagerRecovered(t *testing.T) {
	assert.True(t, txnManagerRecovered(&mockedTxnManager{}))
	assert.False(t, txnManagerRecovered(&neverReadyTxnManager{}))
}

// TestSegmentAllocWorker_DoLoop tests the main do loop
func TestSegmentAllocWorker_DoLoop(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	t.Run("success on first try", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return make(chan struct{})
		}).Maybe()
		mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
				return &wal.AppendResult{
					MessageID: rmq.NewRmqID(100),
					TimeTick:  200,
				}, nil
			}).Once()

		w := &segmentAllocWorker{
			ctx:          ctx,
			collectionID: 1,
			partitionID:  2,
			vchannel:     "v1",
			wal:          mockWAL,
			segmentID:    0,
		}
		w.SetLogger(mlog.With())

		require.NoError(t, w.Execute(context.Background()))
	})

	t.Run("context canceled during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return make(chan struct{})
		}).Maybe()
		mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
				// Cancel context after first failure
				cancel()
				return nil, errors.New("simulated failure")
			}).Once()

		w := &segmentAllocWorker{
			ctx:          ctx,
			collectionID: 1,
			partitionID:  2,
			vchannel:     "v1",
			wal:          mockWAL,
			segmentID:    0,
		}
		w.SetLogger(mlog.With())

		require.NoError(t, w.Execute(context.Background()))
	})
}

// TestSegmentFlushWorker_DoLoop tests the main do loop
func TestSegmentFlushWorker_DoLoop(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	t.Run("success on first try", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1001,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}, 100)
		segment.Flush(policy.PolicyCapacity())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return make(chan struct{})
		}).Maybe()
		mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
				return &wal.AppendResult{
					MessageID: rmq.NewRmqID(100),
					TimeTick:  200,
				}, nil
			}).Once()

		w := &segmentFlushWorker{
			txnManager:   &mockedTxnManager{},
			ctx:          ctx,
			collectionID: 1,
			vchannel:     "v1",
			segment:      segment,
			wal:          mockWAL,
		}
		w.SetLogger(mlog.With())

		require.NoError(t, w.Execute(context.Background()))
	})
}

// neverReadyTxnManager is a mock txn manager that never becomes ready
type neverReadyTxnManager struct{}

func (m *neverReadyTxnManager) RecoverDone() <-chan struct{} {
	return make(chan struct{}) // Never closes
}

// TestSegmentAllocWorker_InitSegmentConfigPreservesValues tests that segment config is preserved across retries
func TestSegmentAllocWorker_InitSegmentConfigPreservesValues(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	ctx := context.Background()
	mockWAL := mock_wal.NewMockWAL(t)

	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(mlog.With())

	// Initialize config - should allocate segment ID
	err := w.initSegmentConfig()
	assert.NoError(t, err)
	firstSegmentID := w.segmentID
	firstStorageVersion := w.storageVersion
	firstLimitation := w.limitation
	assert.NotZero(t, firstSegmentID)

	// Call again - should preserve the same values
	err = w.initSegmentConfig()
	assert.NoError(t, err)
	assert.Equal(t, firstSegmentID, w.segmentID, "segment ID should be preserved")
	assert.Equal(t, firstStorageVersion, w.storageVersion, "storage version should be preserved")
	assert.Equal(t, firstLimitation, w.limitation, "limitation should be preserved")
}

// TestSegmentFlushWorker_CheckIfReady tests the checkIfReady behavior with pending operations
func TestSegmentFlushWorker_CheckIfReady(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	t.Run("ready when no pending operations", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1001,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}, 100)

		w := &segmentFlushWorker{
			segment: segment,
		}
		w.SetLogger(mlog.With())

		assert.True(t, w.checkIfReady())
	})

	t.Run("not ready when ackSem > 0", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1002,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}, 100)

		// Allocate rows to increase ackSem
		result, err := segment.AllocRows(&AssignSegmentRequest{
			TimeTick: 120,
			ModifiedMetrics: stats.ModifiedMetrics{
				Rows:       10,
				BinarySize: 20,
			},
		})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Don't call Ack() so ackSem stays > 0

		w := &segmentFlushWorker{
			segment: segment,
		}
		w.SetLogger(mlog.With())

		assert.False(t, w.checkIfReady())

		// Clean up by acknowledging
		result.Ack()
	})

	t.Run("not ready when txnSem > 0", func(t *testing.T) {
		segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      1003,
			StorageVersion: 2,
			MaxSegmentSize: 150,
		}, 100)

		// Allocate rows with a txn session to increase txnSem
		result, err := segment.AllocRows(&AssignSegmentRequest{
			TimeTick: 120,
			ModifiedMetrics: stats.ModifiedMetrics{
				Rows:       10,
				BinarySize: 20,
			},
			TxnSession: &mockedSession{},
		})
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Ack to decrease ackSem, but txnSem remains > 0
		result.Ack()

		w := &segmentFlushWorker{
			segment: segment,
		}
		w.SetLogger(mlog.With())

		// Now ackSem = 0 but txnSem > 0, so should hit the txnSem branch
		assert.False(t, w.checkIfReady())
	})
}

// TestSegmentFlushWorker_DoOnceDelayFlush tests the delay flush behavior
func TestSegmentFlushWorker_DoOnceDelayFlush(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	// Create segment with pending operations
	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)

	// Allocate to make ackSem > 0
	result, _ := segment.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       10,
			BinarySize: 20,
		},
	})

	ctx := context.Background()
	mockWAL := mock_wal.NewMockWAL(t)

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	// Should return errDelayFlush because segment has pending operations
	err := w.doOnce()
	assert.Error(t, err)
	assert.ErrorIs(t, err, errDelayFlush)

	// Clean up
	result.Ack()
}

// TestSegmentAllocWorker_UnrecoverableError tests handling of unrecoverable errors
func TestSegmentAllocWorker_UnrecoverableError(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			return nil, status.NewUnrecoverableError("unrecoverable error")
		}).Once()

	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(mlog.With())

	err := w.Execute(context.Background())
	require.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

// TestSegmentFlushWorker_UnrecoverableError tests handling of unrecoverable errors
func TestSegmentFlushWorker_UnrecoverableError(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	segment.Flush(policy.PolicyCapacity())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			return nil, status.NewUnrecoverableError("unrecoverable error")
		}).Once()

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	err := w.Execute(context.Background())
	require.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

// TestSegmentAllocWorker_WALUnavailable tests handling when WAL becomes unavailable
func TestSegmentAllocWorker_WALUnavailable(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unavailableCh := make(chan struct{})
	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return unavailableCh
	}).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			// Close the unavailable channel after failure to simulate WAL becoming unavailable
			close(unavailableCh)
			return nil, errors.New("simulated failure")
		}).Once()

	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(mlog.With())

	require.NoError(t, w.Execute(context.Background()))
}

// TestSegmentFlushWorker_ContextCanceledDuringRetry tests context cancellation during retry
func TestSegmentFlushWorker_ContextCanceledDuringRetry(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	segment.Flush(policy.PolicyCapacity())

	ctx, cancel := context.WithCancel(context.Background())

	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			// Cancel context after first failure
			cancel()
			return nil, errors.New("simulated failure")
		}).Once()

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	require.NoError(t, w.Execute(context.Background()))
}

// TestSegmentFlushWorker_TxnManagerRecoverFailed tests when txn manager recovery fails
func TestSegmentFlushWorker_TxnManagerRecoverFailed(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	segment.Flush(policy.PolicyCapacity())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	unavailableCh := make(chan struct{})
	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return unavailableCh
	}).Maybe()

	w := &segmentFlushWorker{
		txnManager:   &neverReadyTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	require.NoError(t, w.Execute(context.Background()))
}

// TestSegmentFlushWorker_WALUnavailableDuringRetry tests WAL unavailable during retry
func TestSegmentFlushWorker_WALUnavailableDuringRetry(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	segment := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1001,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 100)
	segment.Flush(policy.PolicyCapacity())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unavailableCh := make(chan struct{})
	mockWAL := mock_wal.NewMockWAL(t)
	mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return unavailableCh
	}).Maybe()
	mockWAL.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			// Close the unavailable channel after failure
			close(unavailableCh)
			return nil, errors.New("simulated failure")
		}).Once()

	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(mlog.With())

	require.NoError(t, w.Execute(context.Background()))
}
