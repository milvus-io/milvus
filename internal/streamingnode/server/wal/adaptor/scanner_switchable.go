package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchantempstore"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	_ switchableScanner = (*tailingScanner)(nil)
	_ switchableScanner = (*catchupScanner)(nil)
)

// newSwitchableScanner creates a new switchable scanner.
func newSwitchableScanner(
	scannerName string,
	logger *mlog.Logger,
	innerWAL walimpls.ROWALImpls,
	writeAheadBuffer wab.ROWriteAheadBuffer,
	deliverPolicy options.DeliverPolicy,
	msgChan chan<- message.ImmutableMessage,
	roOpener roWALOpener,
	onReaderChanged func(message.WALName),
) switchableScanner {
	impl := switchableScannerImpl{
		scannerName:      scannerName,
		logger:           logger,
		innerWAL:         innerWAL,
		msgChan:          msgChan,
		writeAheadBuffer: writeAheadBuffer,
		roOpener:         roOpener,
		onReaderChanged:  onReaderChanged,
	}
	return newCatchupScanner(impl, deliverPolicy, 0)
}

// switchableScanner is a scanner that can switch between Catchup and Tailing mode
type switchableScanner interface {
	// Execute make a scanner work at background.
	// When the scanner want to change the mode, it will return a new scanner with the new mode.
	// When error is returned, the scanner is canceled and unrecoverable forever.
	Do(ctx context.Context) (switchableScanner, error)
}

type switchableScannerImpl struct {
	scannerName      string
	logger           *mlog.Logger
	innerWAL         walimpls.ROWALImpls
	msgChan          chan<- message.ImmutableMessage
	writeAheadBuffer wab.ROWriteAheadBuffer
	roOpener         roWALOpener
	onReaderChanged  func(message.WALName)
}

func (s *switchableScannerImpl) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.msgChan <- msg:
		return nil
	}
}

// oldVersionLastConfirmedTracker tracks recent message IDs to provide a delayed
// lastConfirmedMessageID for old version (v0) messages.
// Old version messages don't carry lastConfirmedMessageID, so we must synthesize one.
// Using the first-ever v0 message ID (as before) causes the scanner to restart from
// a very old WAL position when tailing→catchup fallback occurs, leading to long catchup
// times and search unavailability.
// Instead, we keep a sliding window and use the message ID from N messages ago,
// so that fallback only replays a bounded number of messages.
type oldVersionLastConfirmedTracker struct {
	window     []message.MessageID
	windowSize int
}

func newOldVersionLastConfirmedTracker(windowSize int) *oldVersionLastConfirmedTracker {
	if windowSize <= 0 {
		windowSize = 30
	}
	return &oldVersionLastConfirmedTracker{
		window:     make([]message.MessageID, 0, windowSize+1),
		windowSize: windowSize,
	}
}

// Track records a new message ID and returns the delayed lastConfirmedMessageID.
// It returns the message ID from windowSize messages ago, or the earliest recorded
// ID if fewer than windowSize messages have been tracked.
func (t *oldVersionLastConfirmedTracker) Track(msgID message.MessageID) message.MessageID {
	t.window = append(t.window, msgID)
	if len(t.window) > t.windowSize {
		confirmed := t.window[0]
		// Trim the oldest entry to prevent unbounded growth.
		t.window = t.window[1:]
		return confirmed
	}
	// Not enough messages yet, return the first one.
	return t.window[0]
}

func newCatchupScanner(
	impl switchableScannerImpl,
	deliverPolicy options.DeliverPolicy,
	exclusiveStartTimeTick uint64,
) *catchupScanner {
	return &catchupScanner{
		switchableScannerImpl:  impl,
		deliverPolicy:          deliverPolicy,
		exclusiveStartTimeTick: exclusiveStartTimeTick,
	}
}

// catchupScanner is a scanner that make a read at underlying wal, and try to catchup the writeahead buffer then switch to tailing mode.
type catchupScanner struct {
	switchableScannerImpl
	deliverPolicy                  options.DeliverPolicy
	exclusiveStartTimeTick         uint64 // scanner should filter out the message that less than or equal to this time tick.
	oldVersionLastConfirmedTracker *oldVersionLastConfirmedTracker
}

func (s *catchupScanner) Do(ctx context.Context) (switchableScanner, error) {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		scanner, err := s.openCatchupScannerImpls(ctx)
		if err != nil {
			return nil, err
		}
		switchedScanner, err := s.consumeWithScanner(ctx, scanner)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if status.AsStreamingError(err).IsUnrecoverable() {
				return nil, err
			}
			waker, nextInterval := backoffTimer.NextTimer()
			s.logger.Warn(ctx, "scanner consuming was interrupted with error, start a backoff",
				mlog.Duration("nextInterval", nextInterval),
				mlog.Err(err))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-waker:
			}
			continue
		}
		return switchedScanner, nil
	}
}

func (s *catchupScanner) openCatchupScannerImpls(ctx context.Context) (walimpls.ScannerImpls, error) {
	_, hasWALSpecificPosition := getDeliverPolicyWALName(s.deliverPolicy)
	// Every WAL-specific position goes through the cross-WAL adaptor, including
	// one that names the current backend. A WAL name identifies the backend, not
	// the migration generation: after an A->B->A migration an old-generation
	// position names the backend that is current again, and reading it directly
	// would resume inside the reused topic and silently skip everything written
	// while B was current. The adaptor instead follows the AlterWAL markers of
	// whatever chain the position belongs to, and a position of the current
	// generation simply never meets a marker.
	if !hasWALSpecificPosition || s.roOpener == nil {
		scanner, err := s.createInnerWALScannerWithBackoff(ctx, s.deliverPolicy)
		if err == nil && s.onReaderChanged != nil {
			s.onReaderChanged(s.innerWAL.WALName())
		}
		return scanner, err
	}
	return newUnderlyingWALScannerAdaptor(
		s.logger,
		s.innerWAL.Channel(),
		walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       s.deliverPolicy,
			ReadAheadBufferSize: getWALReadAheadBufferSize(),
		},
		s.roOpener,
		s.onReaderChanged,
	)
}

func (s *catchupScanner) consumeWithScanner(ctx context.Context, scanner walimpls.ScannerImpls) (switchableScanner, error) {
	defer scanner.Close()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				return nil, scanner.Error()
			}

			if msg.Version() == message.VersionOld {
				if s.oldVersionLastConfirmedTracker == nil {
					windowSize := paramtable.Get().StreamingCfg.OldVersionLastConfirmedWindowSize.GetAsInt()
					s.oldVersionLastConfirmedTracker = newOldVersionLastConfirmedTracker(windowSize)
				}
				// Use a sliding-window tracker to provide a delayed lastConfirmedMessageID.
				// This ensures that when a tailing scanner falls back to catchup mode,
				// it only needs to replay a bounded number of recent messages instead of
				// the entire WAL from the very first v0 message.
				lastConfirmedMessageID := s.oldVersionLastConfirmedTracker.Track(msg.MessageID())
				var err error
				messageID := msg.MessageID()
				msg, err = newOldVersionImmutableMessage(ctx, s.innerWAL.Channel().Name, lastConfirmedMessageID, msg)
				if errors.Is(err, vchantempstore.ErrNotFound) {
					// Skip the message's vchannel is not found in the vchannel temp store.
					s.logger.Info(ctx, "skip the old version message because vchannel not found", mlog.Stringer("messageID", messageID))
					continue
				}
				if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
					return nil, err
				}
				if err != nil {
					panic("unrechable: unexpected error found: " + err.Error())
				}
			}

			if msg.TimeTick() <= s.exclusiveStartTimeTick {
				// we should filter out the message that less than or equal to this time tick to remove duplicate message
				// when we switch from tailing mode to catchup mode.
				continue
			}
			if shouldStartConsumeSpan(msg) {
				startConsumeSpanForMessage(ctx, msg)
			}
			if err := s.HandleMessage(ctx, msg); err != nil {
				return nil, err
			}
			if msg.MessageType() != message.MessageTypeTimeTick || s.writeAheadBuffer == nil {
				// Only timetick message is keep the same order with the write ahead buffer.
				// So we can only use the timetick message to catchup the write ahead buffer.
				continue
			}
			// Here's a timetick message from the scanner, make tailing read if we catch up the writeahead buffer.
			if reader, err := s.writeAheadBuffer.ReadFromExclusiveTimeTick(ctx, msg.TimeTick()); err == nil {
				s.logger.Info(
					ctx, "scanner consuming was interrpted because catup done",
					mlog.Uint64("timetick", msg.TimeTick()),
					mlog.Stringer("messageID", msg.MessageID()),
					mlog.Stringer("lastConfirmedMessageID", msg.LastConfirmedMessageID()),
				)
				return &tailingScanner{
					switchableScannerImpl: s.switchableScannerImpl,
					reader:                reader,
					lastConsumedMessage:   msg,
				}, nil
			}
		}
	}
}

func (s *switchableScannerImpl) createInnerWALScannerWithBackoff(
	ctx context.Context,
	deliverPolicy options.DeliverPolicy,
) (walimpls.ScannerImpls, error) {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		innerScanner, err := s.innerWAL.Read(ctx, walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       deliverPolicy,
			ReadAheadBufferSize: getWALReadAheadBufferSize(),
		})
		if err == nil {
			return innerScanner, nil
		}
		if ctx.Err() != nil {
			// The scanner is closing, so stop the backoff.
			return nil, ctx.Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn(
			ctx, "create inner WAL scanner failed, start a backoff",
			mlog.Stringer("walName", s.innerWAL.WALName()),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waker:
		}
	}
}

func getWALReadAheadBufferSize() int {
	bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
	if bufSize < 0 {
		return 0
	}
	return bufSize
}

func newScannerReadBackoffTimer() *typeutil.BackoffTimer {
	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()
	return backoffTimer
}

// tailingScanner is used to perform a tailing read from the writeaheadbuffer of wal.
type tailingScanner struct {
	switchableScannerImpl
	reader              *wab.WriteAheadBufferReader
	lastConsumedMessage message.ImmutableMessage
}

func (s *tailingScanner) Do(ctx context.Context) (switchableScanner, error) {
	for {
		msg, err := s.reader.Next(ctx)
		if errors.Is(err, wab.ErrEvicted) {
			// The tailing read is failure, switch into catchup mode.
			s.logger.Info(
				ctx, "scanner consuming was interrpted because tailing eviction",
				mlog.Uint64("timetick", s.lastConsumedMessage.TimeTick()),
				mlog.Stringer("messageID", s.lastConsumedMessage.MessageID()),
				mlog.Stringer("lastConfirmedMessageID", s.lastConsumedMessage.LastConfirmedMessageID()),
			)
			return newCatchupScanner(
				s.switchableScannerImpl,
				options.DeliverPolicyStartFrom(s.lastConsumedMessage.LastConfirmedMessageID()),
				s.lastConsumedMessage.TimeTick(),
			), nil
		}
		if err != nil {
			return nil, err
		}
		// Do not start wal.catchup_consume or overwrite _tc in tailing mode.
		// WriteAheadBuffer readers share the same immutable message instance,
		// including its properties map, across all tailing consumers on this
		// pchannel. Mutating trace context here would race with other readers.
		if err := s.HandleMessage(ctx, tailingImmutableMesasge{msg}); err != nil {
			return nil, err
		}
		s.lastConsumedMessage = msg
	}
}

// getScannerModel returns the scanner model.
func getScannerModel(scanner switchableScanner) string {
	if _, ok := scanner.(*tailingScanner); ok {
		return metrics.WALScannerModelTailing
	}
	return metrics.WALScannerModelCatchup
}

type tailingImmutableMesasge struct {
	message.ImmutableMessage
}

// isTailingScanImmutableMessage check whether the message is a tailing message.
func isTailingScanImmutableMessage(msg message.ImmutableMessage) (message.ImmutableMessage, bool) {
	if msg, ok := msg.(tailingImmutableMesasge); ok {
		return msg.ImmutableMessage, true
	}
	return msg, false
}

func shouldStartConsumeSpan(msg message.ImmutableMessage) bool {
	if msg.TxnContext() == nil {
		return true
	}
	return msg.MessageType() == message.MessageTypeCommitTxn
}

func startConsumeSpanForMessage(ctx context.Context, msg message.ImmutableMessage) {
	ctx = message.ExtractTraceContext(ctx, msg)
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALCatchupConsume)
	message.OverwriteTraceContext(ctx, msg)
	span.End()
}
