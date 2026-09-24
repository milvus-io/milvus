//go:build test
// +build test

package flusherimpl

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// TestFlusherComponentsDispatchAndDrainedCloseAreRaceFree runs the two real
// goroutines that meet at a fenced source's data sync service, under -race:
//
//   - the flusher's dispatch goroutine, calling flusherComponents.HandleMessage
//     with time ticks, which are built WithAllVChannel and so broadcast into
//     every data sync service's input channel, the fenced source included;
//   - the checkpoint updater's goroutine, calling the real checkpoint callback
//     (WALFlusherImpl.onCheckpointUpdated) with an ack at the fence tick while
//     those broadcasts are in flight.
//
// Nothing on the close path is stubbed: dataSyncServiceWrapper.Close really
// closes the input channel. A close issued from the callback goroutine (the
// shape this replaced) lets a broadcast that already snapshotted the service
// send on the closed channel, which panics and kills the test binary. With the
// close decided and run on the dispatch goroutine, every round must end with
// the source removed, its input channel closed, and no panic or data race.
func TestFlusherComponentsDispatchAndDrainedCloseAreRaceFree(t *testing.T) {
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().RemoveChannel(mock.Anything).Return().Maybe()
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().UpdateFlusherCheckpoint(mock.Anything, mock.Anything).Return().Maybe()

	walFuture := syncutil.NewFuture[wal.WAL]()
	walFuture.Set(newMockWAL(t, true))
	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       make(map[string]uint64),
		logger:       mlog.With(),
		rs:           rs,
	}
	flusher := &WALFlusherImpl{
		notifier:          syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:            mlog.With(),
		wal:               walFuture,
		RecoveryStorage:   rs,
		flusherComponents: fc,
	}
	msgID := adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(1)).Serialize()

	const (
		rounds        = 300
		fenceTick     = uint64(1000)
		maxDispatches = 100000
		liveVChann    = "live"
	)
	// A live service that is never fenced keeps every broadcast
	// iterating over more than one service, as on a real pchannel.
	liveInput := make(chan *msgstream.MsgPack, 64)
	liveDone := make(chan struct{})
	go func() {
		defer close(liveDone)
		for range liveInput {
		}
	}()
	fc.mu.Lock()
	fc.dataServices[liveVChann] = newDataSyncServiceWrapper(liveVChann, liveInput, &pipeline.DataSyncService{}, 0)
	fc.mu.Unlock()

	for round := 0; round < rounds; round++ {
		vchannel := fmt.Sprintf("source-%d", round)
		input := make(chan *msgstream.MsgPack, 4)
		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			for range input {
			}
		}()
		fc.mu.Lock()
		fc.dataServices[vchannel] = newDataSyncServiceWrapper(vchannel, input, &pipeline.DataSyncService{}, 0)
		fc.mu.Unlock()
		fc.RecordFence(vchannel, fenceTick)

		// The ack lands after a round-dependent number of dispatches, so over
		// the rounds it meets the broadcast at different points.
		ackAfter := round % 5
		dispatched := make(chan struct{}, maxDispatches)
		acked := make(chan struct{})
		go func() {
			for i := 0; i < ackAfter; i++ {
				<-dispatched
			}
			runtime.Gosched()
			flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: vchannel, MsgID: msgID, Timestamp: fenceTick})
			close(acked)
		}()

		dispatchDone := make(chan struct{})
		go func() {
			defer close(dispatchDone)
			for i := 0; i < maxDispatches; i++ {
				tt := message.CreateTestTimeTickSyncMessage(t, 1, fenceTick+uint64(i)+1, rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(2))
				if err := fc.HandleMessage(context.Background(), tt); err != nil {
					assert.NoError(t, err)
					return
				}
				dispatched <- struct{}{}
				select {
				case <-acked:
					if !fc.hasDataSyncService(vchannel) {
						return
					}
				default:
				}
			}
			assert.Fail(t, "the drained source was never closed", "vchannel %s", vchannel)
		}()

		<-dispatchDone
		<-acked
		// The input channel of the source must end up closed by the real Close.
		<-consumerDone
	}

	assert.True(t, fc.hasDataSyncService(liveVChann), "the unfenced service must never be closed")
	fc.WhenDropCollection(context.Background(), liveVChann)
	<-liveDone
}
