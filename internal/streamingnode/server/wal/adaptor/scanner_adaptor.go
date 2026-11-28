// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adaptor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	_               wal.Scanner = (*scannerAdaptorImpl)(nil)
	consumerCounter atomic.Int64
)

// scannerConfig supplies an already available WAB and an optional startup boundary.
// Without an injected WAB, RW scanners resolve it through the TimeTick inspector.
type scannerConfig struct {
	replicaObserve func([]message.ImmutableMessage, uint64)
	replicaBuffer  *replica.Buffer
	// RecoveryStorage observes control records; public subscriptions receive confirmations.
	deliverWALControl bool
	writeAheadBuffer  wab.ROWriteAheadBuffer
	startupBarrier    *scannerStartupBarrier
}

// scannerStartupBarrier pauses raw input after this exact barrier until the
// caller finishes initialization. The consumer snapshots unfinished transactions
// before delivering the barrier. Cancellation also releases the pause.
type scannerStartupBarrier struct {
	message         message.ImmutableMessage
	resume          <-chan struct{}
	stopAtWALSwitch bool
	liveSource      func() (walimpls.ROWALImpls, wab.ROWriteAheadBuffer)
}

func (b *scannerStartupBarrier) matches(msg message.ImmutableMessage) bool {
	return msg.MessageType() == message.MessageTypeRecoveryBarrier &&
		msg.TimeTick() == b.message.TimeTick() && msg.WALName() == b.message.WALName() && msg.MessageID().EQ(b.message.MessageID())
}

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.ROWALImpls,
	readOption wal.ReadOption,
	scanMetrics *metricsutil.ScannerMetrics,
	cleanup func(),
	config scannerConfig,
) *scannerAdaptorImpl {
	if readOption.MesasgeHandler == nil {
		readOption.MesasgeHandler = adaptor.ChanMessageHandler(make(chan message.ImmutableMessage))
	}
	logger := resource.Resource().Logger().With(
		mlog.FieldComponent("scanner"),
		mlog.String("name", name),
		mlog.String("channel", l.Channel().Name),
	)
	s := &scannerAdaptorImpl{
		replicaObserve:    config.replicaObserve,
		replicaBuffer:     config.replicaBuffer,
		logger:            logger,
		deliverWALControl: config.deliverWALControl,
		writeAheadBuffer:  config.writeAheadBuffer,
		startupBarrier:    config.startupBarrier,
		innerWAL:          l,
		readOption:        readOption,
		filterFunc:        options.GetFilterFunc(readOption.MessageFilter),
		reorderBuffer:     utility.NewReOrderBuffer(),
		pendingQueue:      utility.NewPendingQueue(),
		txnBuffer:         utility.NewTxnBuffer(logger, scanMetrics),
		cleanup:           cleanup,
		ScannerHelper:     helper.NewScannerHelper(name),
		metrics:           scanMetrics,
	}
	go s.execute()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	replicaObserve    func([]message.ImmutableMessage, uint64)
	replicaPending    []message.ImmutableMessage // assembled messages awaiting a physical confirmation
	replicaBuffer     *replica.Buffer
	deliverWALControl bool
	startupBarrier    *scannerStartupBarrier
	startupTxnBuffer  *utility.TxnBuffer // published by delivery of the startup barrier
	*helper.ScannerHelper
	writeAheadBuffer wab.ROWriteAheadBuffer
	logger           *mlog.Logger
	innerWAL         walimpls.ROWALImpls
	readOption       wal.ReadOption
	filterFunc       func(message.ImmutableMessage) bool
	reorderBuffer    *utility.ReOrderByTimeTickBuffer // support time tick reorder.
	pendingQueue     *utility.PendingQueue
	txnBuffer        *utility.TxnBuffer // txn buffer for txn message.

	cleanup   func()
	clearOnce sync.Once
	metrics   *metricsutil.ScannerMetrics
}

// Channel returns the channel assignment info of the wal.
func (s *scannerAdaptorImpl) Channel() types.PChannelInfo {
	return s.innerWAL.Channel()
}

// Chan returns the message channel of the scanner.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.readOption.MesasgeHandler.(adaptor.ChanMessageHandler)
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerAdaptorImpl) Close() error {
	err := s.ScannerHelper.Close()
	// Close may be called multiple times, so we need to clear the resources only once.
	s.clear()
	return err
}

// clear clears the resources of the scanner.
func (s *scannerAdaptorImpl) clear() {
	s.clearOnce.Do(func() {
		if s.cleanup != nil {
			s.cleanup()
		}
		s.metrics.Close()
	})
}

func (s *scannerAdaptorImpl) execute() {
	var finalErr error
	defer func() {
		s.readOption.MesasgeHandler.Close()
		s.Finish(finalErr)
		s.logger.Info(context.TODO(), "scanner is closed")
	}()
	s.logger.Info(context.TODO(), "scanner start background task")

	msgChan := make(chan message.ImmutableMessage)

	producerErrCh := make(chan error, 1)
	// TODO: optimize the extra goroutine here after msgstream is removed.
	go func() {
		err := s.produceEventLoop(msgChan)
		producerErrCh <- err
		// Wake a consumer blocked in its handler. execute reconciles the two
		// loop results below and preserves a non-cancellation producer error.
		s.Cancel()
	}()

	consumeErr := s.consumeEventLoop(msgChan)
	s.Cancel()
	producerErr := <-producerErrCh

	if errors.Is(producerErr, context.Canceled) {
		s.logger.Info(context.TODO(), "the produce event loop of scanner is closed")
	} else if producerErr != nil {
		s.logger.Warn(context.TODO(), "the produce event loop of scanner is closed with unexpected error", mlog.Err(producerErr))
	}
	if errors.Is(consumeErr, context.Canceled) {
		s.logger.Info(context.TODO(), "the consuming event loop of scanner is closed")
	} else if consumeErr != nil {
		s.logger.Warn(context.TODO(), "the consuming event loop of scanner is closed with unexpected error", mlog.Err(consumeErr))
	}

	// A consumer-side processing failure wins if both loops fail. Otherwise a
	// fatal durable-reader/assembly error must be visible from Scanner.Error().
	if consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
		finalErr = consumeErr
	} else if producerErr != nil && !errors.Is(producerErr, context.Canceled) {
		finalErr = producerErr
	}
}

// produceEventLoop produces the message from the wal and write ahead buffer.
func (s *scannerAdaptorImpl) produceEventLoop(msgChan chan<- message.ImmutableMessage) error {
	wb := s.writeAheadBuffer
	var err error
	if wb == nil && s.startupBarrier == nil && s.Channel().AccessMode == types.AccessModeRW {
		if wb, err = s.waitWriteAheadBuffer(); err != nil {
			return err
		}
	}

	scanner := newSwithableScanner(s.Name(), s.logger, s.innerWAL, wb, s.readOption.DeliverPolicy, msgChan, s.startupBarrier)
	s.logger.Info(context.TODO(), "start produce loop of scanner at model", mlog.String("model", getScannerModel(scanner)))
	for {
		if scanner, err = scanner.Do(s.Context()); err != nil {
			return err
		}
		m := getScannerModel(scanner)
		s.metrics.SwitchModel(m)
		s.logger.Info(context.TODO(), "switch scanner model", mlog.String("model", m))
	}
}

func (s *scannerAdaptorImpl) waitWriteAheadBuffer() (wab.ROWriteAheadBuffer, error) {
	inspector := resource.Resource().TimeTickInspector()
	for {
		if operator, ok := inspector.GetOperator(s.Channel()); ok {
			// Trigger a persisted time tick to make sure the timetick is pushed forward.
			// The underlying WAL may be deleted by retention policy, so catchup needs
			// a fresh durable timetick before switching to WAB tailing.
			inspector.TriggerSync(s.Channel(), true)
			return operator.WriteAheadBuffer(), nil
		}

		s.logger.Debug(context.TODO(), "wait for timetick sync operator before using write ahead buffer")
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-s.Context().Done():
			timer.Stop()
			return nil, s.Context().Err()
		case <-timer.C:
		}
	}
}

// consumeEventLoop consumes the message from the message channel and handle it.
func (s *scannerAdaptorImpl) consumeEventLoop(msgChan <-chan message.ImmutableMessage) error {
	s.waitUntilStartConsumption()
	for {
		var upstream <-chan message.ImmutableMessage
		if s.pendingQueue.Len() > 16 {
			// If the pending queue is full, we need to wait until it's consumed to avoid scanner overloading.
			upstream = nil
		} else {
			upstream = msgChan
		}
		// generate the event channel and do the event loop.
		handleResult := s.readOption.MesasgeHandler.Handle(message.HandleParam{
			Ctx:      s.Context(),
			Upstream: upstream,
			Message:  s.pendingQueue.Next(),
		})
		if handleResult.Error != nil {
			return handleResult.Error
		}
		if handleResult.MessageHandled {
			s.pendingQueue.UnsafeAdvance()
			s.metrics.UpdatePendingQueueSize(s.pendingQueue.Bytes())
		}
		if handleResult.Incoming != nil {
			if err := s.handleUpstream(handleResult.Incoming); err != nil {
				return err
			}
		}
	}
}

// waitUntilStartConsumption is used to wait until the consumption is started.
func (s *scannerAdaptorImpl) waitUntilStartConsumption() {
	s.metrics.PauseConsumption()
	defer s.metrics.ResumeConsumption()

	pauseConsumption := paramtable.Get().StreamingCfg.WALScannerPauseConsumption.GetAsBool()
	if !s.readOption.IgnorePauseConsumption && pauseConsumption {
		resumeChan := make(chan struct{}, 1)
		watchKey := paramtable.Get().StreamingCfg.WALScannerPauseConsumption.Key
		handler := config.NewHandler(fmt.Sprintf("%s-%d", watchKey, consumerCounter.Inc()), func(event *config.Event) {
			pause := paramtable.Get().StreamingCfg.WALScannerPauseConsumption.GetAsBool()
			if !pause {
				select {
				case resumeChan <- struct{}{}:
				default:
				}
			}
		})
		paramtable.Get().Watch(watchKey, handler)
		defer paramtable.Get().Unwatch(watchKey, handler)

		s.logger.Info(context.TODO(), "pause consumption...")
		select {
		case <-resumeChan:
			s.logger.Info(context.TODO(), "continue to consume messages")
		case <-s.Context().Done():
			s.logger.Info(context.TODO(), "pause consumption is canceled")
		}
	}
}

// handleUpstream handles the incoming message from the upstream.
func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) error {
	confirm := message.IsTimeTickConfirmBarrier(msg)
	// Raw transaction fragments must keep the existing input filter semantics:
	// the commit decides the transaction's timestamp and bodies are type-filtered.
	// Confirmations always finish internal processing before delivery filtering.
	if !confirm && s.filterFunc != nil && !s.filterFunc(msg) {
		return nil
	}
	var isTailing bool
	msg, isTailing = isTailingScanImmutableMessage(msg)
	s.metrics.ObserveMessage(isTailing, msg.MessageType(), msg.EstimateSize())

	// Ordinary records, including exclusive control operations, share ordering
	// and deduplication. Pure confirmations need no reorder-buffer entry.
	ordered := msg.MessageType() != message.MessageTypeTimeTick &&
		msg.MessageType() != message.MessageTypeMVCCBarrier &&
		msg.MessageType() != message.MessageTypeRecoveryBarrier
	if ordered {
		// Confirmations cover the whole PChannel, including when a broadcast
		// copy carries its PChannel or control channel as the VChannel.
		if !confirm && msg.VChannel() != "" && s.readOption.VChannel != "" && s.readOption.VChannel != msg.VChannel() {
			return nil
		}
		if !s.pushToReorderBuffer(msg, isTailing) {
			return nil
		}
	}
	if !confirm {
		return nil
	}
	messages := s.reorderBuffer.PopUtilTimeTick(msg.TimeTick())
	s.metrics.UpdateTimeTickBufSize(s.reorderBuffer.Bytes())
	msgs := s.txnBuffer.HandleImmutableMessages(messages, msg.TimeTick())
	s.metrics.UpdateTxnBufSize(s.txnBuffer.Bytes())
	if !ordered {
		msgs = append(msgs, msg)
	}
	if s.startupBarrier != nil && s.startupBarrier.matches(msg) && s.startupTxnBuffer == nil {
		s.startupTxnBuffer = s.txnBuffer.Snapshot()
	}
	if s.replicaBuffer != nil {
		if message.IsWALSwitch(msg) {
			start, err := message.WALSwitchStart(msg)
			if err != nil {
				return err
			}
			if start.WALName() != msg.WALName() {
				s.replicaBuffer.ObserveSwitch(msg.TimeTick(), start)
			}
		}
		if msg.MessageType() == message.MessageTypeRecoveryBarrier {
			s.replicaBuffer.ObserveRecoveryBarrier(msg, s.txnBuffer)
		}
		s.replicaPending = append(s.replicaPending, msgs...)
		if msg.IsPersisted() {
			s.replicaBuffer.Observe(s.replicaPending, msg.TimeTick())
			if s.replicaObserve != nil {
				s.replicaObserve(s.replicaPending, msg.TimeTick())
			}
			s.replicaPending = nil
		}
	}
	s.enqueue(msgs)
	return nil
}

// enqueue applies public control-message projection and delivery filters in one
// place, after ordering and transaction effects have completed.
func (s *scannerAdaptorImpl) enqueue(msgs []message.ImmutableMessage) {
	deliver := msgs[:0]
	for _, msg := range msgs {
		if !s.deliverWALControl && msg.MessageType() == message.MessageTypeAlterWAL {
			msg = message.NewTimeTickMessageBuilderV1().
				WithHeader(&message.TimeTickMessageHeader{}).
				WithBody(&msgpb.TimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_TimeTick, Timestamp: msg.TimeTick()}}).
				WithAllVChannel().MustBuildMutable().WithTimeTick(msg.TimeTick()).
				WithLastConfirmed(msg.LastConfirmedMessageID()).IntoImmutableMessage(msg.MessageID())
		}
		if s.filterFunc != nil && !s.filterFunc(msg) {
			continue
		}
		// Persisted TimeTicks always reach consumers. Volatile ticks may coalesce
		// with queued messages, preserving the existing query-pipeline contract.
		if msg.MessageType() == message.MessageTypeTimeTick && !msg.IsPersisted() && (len(deliver) > 0 || s.pendingQueue.Len() > 0) {
			continue
		}
		deliver = append(deliver, msg)
	}
	s.pendingQueue.Add(deliver)
	s.metrics.UpdatePendingQueueSize(s.pendingQueue.Bytes())
}

// pushToReorderBuffer reports whether the message was accepted for ordering.
func (s *scannerAdaptorImpl) pushToReorderBuffer(msg message.ImmutableMessage, isTailing bool) bool {
	pushResult, err := s.reorderBuffer.Push(msg)
	if err != nil {
		if errors.Is(err, utility.ErrTimeTickVoilation) {
			s.metrics.ObserveTimeTickViolation(isTailing, msg.MessageType())
		}
		s.logger.Warn(context.TODO(), "failed to push message into reorder buffer",
			mlog.FieldMessage(msg),
			mlog.Bool("tailing", isTailing),
			mlog.Err(err))
	} else if pushResult.Dropped {
		switch pushResult.DropReason {
		case utility.ReOrderByTimeTickBufferDropReasonDuplicateTimeTick:
			s.metrics.ObservePhysicalDedupDrop(isTailing)
			s.logger.Warn(context.TODO(), "dropped duplicated non-timetick message from reorder buffer",
				mlog.String("vchannel", msg.VChannel()),
				mlog.String("msgID", msg.MessageID().String()),
				mlog.Uint64("timeTick", msg.TimeTick()),
				mlog.Bool("tailing", isTailing),
				mlog.String("dropReason", string(pushResult.DropReason)))
		}
	}
	// Observe the filtered message.
	s.metrics.UpdateTimeTickBufSize(s.reorderBuffer.Bytes())
	s.metrics.ObservePassedMessage(isTailing, msg.MessageType(), msg.EstimateSize())
	return err == nil && !pushResult.Dropped
}
