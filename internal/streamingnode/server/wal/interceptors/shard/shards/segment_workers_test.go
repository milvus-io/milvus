//go:build test
// +build test

package shards

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

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
	w.SetLogger(log.With())

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
	w.SetLogger(log.With())

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
	w.SetLogger(log.With())

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
	w.SetLogger(log.With())

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

// TestSegmentFlushWorker_WaitForTxnManagerRecoverDone tests the txn manager wait behavior
func TestSegmentFlushWorker_WaitForTxnManagerRecoverDone(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	t.Run("txn manager ready", func(t *testing.T) {
		ctx := context.Background()
		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return make(chan struct{})
		}).Maybe()

		w := &segmentFlushWorker{
			txnManager: &mockedTxnManager{}, // RecoverDone returns closed channel
			ctx:        ctx,
			wal:        mockWAL,
		}
		w.SetLogger(log.With())

		err := w.waitForTxnManagerRecoverDone()
		assert.NoError(t, err)
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return make(chan struct{})
		}).Maybe()

		// Create a txn manager that never becomes ready
		neverReadyTxnMgr := &neverReadyTxnManager{}

		w := &segmentFlushWorker{
			txnManager: neverReadyTxnMgr,
			ctx:        ctx,
			wal:        mockWAL,
		}
		w.SetLogger(log.With())

		err := w.waitForTxnManagerRecoverDone()
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("wal unavailable", func(t *testing.T) {
		ctx := context.Background()

		// Create a WAL that becomes unavailable
		unavailableCh := make(chan struct{})
		close(unavailableCh)
		mockWAL := mock_wal.NewMockWAL(t)
		mockWAL.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
			return unavailableCh
		}).Maybe()

		// Create a txn manager that never becomes ready
		neverReadyTxnMgr := &neverReadyTxnManager{}

		w := &segmentFlushWorker{
			txnManager: neverReadyTxnMgr,
			ctx:        ctx,
			wal:        mockWAL,
		}
		w.SetLogger(log.With())

		err := w.waitForTxnManagerRecoverDone()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wal is unavailable")
	})
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

		done := make(chan struct{})
		w := &segmentAllocWorker{
			ctx:          ctx,
			collectionID: 1,
			partitionID:  2,
			vchannel:     "v1",
			wal:          mockWAL,
			segmentID:    0,
		}
		w.SetLogger(log.With())

		go func() {
			w.do()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for do() to complete")
		}
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

		done := make(chan struct{})
		w := &segmentAllocWorker{
			ctx:          ctx,
			collectionID: 1,
			partitionID:  2,
			vchannel:     "v1",
			wal:          mockWAL,
			segmentID:    0,
		}
		w.SetLogger(log.With())

		go func() {
			w.do()
			close(done)
		}()

		select {
		case <-done:
			// Success - should exit due to context cancellation
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for do() to complete")
		}
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

		done := make(chan struct{})
		w := &segmentFlushWorker{
			txnManager:   &mockedTxnManager{},
			ctx:          ctx,
			collectionID: 1,
			vchannel:     "v1",
			segment:      segment,
			wal:          mockWAL,
		}
		w.SetLogger(log.With())

		go func() {
			w.do()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for do() to complete")
		}
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
	w.SetLogger(log.With())

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
		w.SetLogger(log.With())

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
		w.SetLogger(log.With())

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
		w.SetLogger(log.With())

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
	w.SetLogger(log.With())

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

	done := make(chan struct{})
	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit due to unrecoverable error
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
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

	done := make(chan struct{})
	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit due to unrecoverable error
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
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

	done := make(chan struct{})
	w := &segmentAllocWorker{
		ctx:          ctx,
		collectionID: 1,
		partitionID:  2,
		vchannel:     "v1",
		wal:          mockWAL,
		segmentID:    0,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit because WAL became unavailable
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
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

	done := make(chan struct{})
	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit due to context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
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

	done := make(chan struct{})
	w := &segmentFlushWorker{
		txnManager:   &neverReadyTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit due to context cancellation in waitForTxnManagerRecoverDone
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
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

	done := make(chan struct{})
	w := &segmentFlushWorker{
		txnManager:   &mockedTxnManager{},
		ctx:          ctx,
		collectionID: 1,
		vchannel:     "v1",
		segment:      segment,
		wal:          mockWAL,
	}
	w.SetLogger(log.With())

	go func() {
		w.do()
		close(done)
	}()

	select {
	case <-done:
		// Success - should exit because WAL became unavailable
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for do() to complete")
	}
}
