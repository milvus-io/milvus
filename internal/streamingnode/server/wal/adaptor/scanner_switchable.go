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
	_ switchableScanner = (*historicalScanner)(nil)
)

type switchableScannerOptions struct {
	historicalWALOpener               historicalWALOpener
	historicalWALFallbackTimeout      time.Duration
	historicalStartExclusiveMessageID message.MessageID
}

// newSwitchableScanner creates a new switchable scanner.
func newSwithableScanner(
	scannerName string,
	logger *mlog.Logger,
	currentWAL walimpls.ROWALImpls,
	initialWAL walimpls.ROWALImpls,
	writeAheadBuffer wab.ROWriteAheadBuffer,
	deliverPolicy options.DeliverPolicy,
	msgChan chan<- message.ImmutableMessage,
	options switchableScannerOptions,
	onReaderSwitch func(message.WALName, string),
) switchableScanner {
	impl := switchableScannerImpl{
		scannerName:                  scannerName,
		logger:                       logger,
		innerWAL:                     currentWAL,
		msgChan:                      msgChan,
		writeAheadBuffer:             writeAheadBuffer,
		historicalWALOpener:          options.historicalWALOpener,
		historicalWALFallbackTimeout: options.historicalWALFallbackTimeout,
		onReaderSwitch:               onReaderSwitch,
	}
	if initialWAL.WALName() != currentWAL.WALName() {
		return &historicalScanner{
			switchableScannerImpl:   impl,
			historicalWAL:           initialWAL,
			deliverPolicy:           deliverPolicy,
			exclusiveStartMessageID: options.historicalStartExclusiveMessageID,
		}
	}
	return &catchupScanner{
		switchableScannerImpl:  impl,
		deliverPolicy:          deliverPolicy,
		exclusiveStartTimeTick: 0,
	}
}

// switchableScanner is a scanner that can switch between Catchup and Tailing mode
type switchableScanner interface {
	// Execute make a scanner work at background.
	// When the scanner want to change the mode, it will return a new scanner with the new mode.
	// When error is returned, the scanner is canceled and unrecoverable forever.
	Do(ctx context.Context) (switchableScanner, error)
}

type switchableScannerImpl struct {
	scannerName                  string
	logger                       *mlog.Logger
	innerWAL                     walimpls.ROWALImpls
	msgChan                      chan<- message.ImmutableMessage
	writeAheadBuffer             wab.ROWriteAheadBuffer
	historicalWALOpener          historicalWALOpener
	historicalWALFallbackTimeout time.Duration
	onReaderSwitch               func(message.WALName, string)
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

// historicalScanner consumes from the WAL backend encoded in an existing
// consumer position. It switches to the current WAL only after the AlterWAL
// message is covered by a TimeTick visibility barrier.
type historicalScanner struct {
	switchableScannerImpl
	historicalWAL                  walimpls.ROWALImpls
	deliverPolicy                  options.DeliverPolicy
	exclusiveStartTimeTick         uint64
	exclusiveStartMessageID        message.MessageID
	oldVersionLastConfirmedTracker *oldVersionLastConfirmedTracker
}

func (s *historicalScanner) Do(ctx context.Context) (switchableScanner, error) {
	defer s.historicalWAL.Close()
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		scanner, fallback, err := s.createHistoricalReaderWithFallback(ctx)
		if err != nil {
			return nil, err
		}
		if fallback {
			return s.newCurrentCatchupScanner(ctx, s.exclusiveStartTimeTick, "historical reader unavailable"), nil
		}
		switchedScanner, err := s.consumeWithScanner(ctx, scanner)
		if err != nil {
			s.logger.Warn(ctx, "historical scanner consumption was interrupted, retrying", mlog.Err(err))
			continue
		}
		return switchedScanner, nil
	}
}

func (s *historicalScanner) consumeWithScanner(
	ctx context.Context,
	scanner walimpls.ScannerImpls,
) (switchableScanner, error) {
	defer scanner.Close()
	var alterWALTimeTick uint64
	var targetWALName message.WALName
	idleTimeout := s.getHistoricalWALFallbackTimeout()
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-idleTimer.C:
			s.logger.Warn(ctx, "historical reader made no progress, falling back to current WAL",
				mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
				mlog.Stringer("currentWALName", s.innerWAL.WALName()),
				mlog.Uint64("alterWALTimeTick", alterWALTimeTick),
				mlog.Duration("idleTimeout", idleTimeout))
			return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "historical reader idle timeout"), nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				if err := scanner.Error(); err != nil {
					return nil, err
				}
				s.logger.Info(ctx, "historical reader reached end of stream, falling back to current WAL",
					mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
					mlog.Stringer("currentWALName", s.innerWAL.WALName()))
				return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "historical reader exhausted"), nil
			}
			resetTimer(idleTimer, idleTimeout)

			if msg.Version() == message.VersionOld {
				if s.oldVersionLastConfirmedTracker == nil {
					windowSize := paramtable.Get().StreamingCfg.OldVersionLastConfirmedWindowSize.GetAsInt()
					s.oldVersionLastConfirmedTracker = newOldVersionLastConfirmedTracker(windowSize)
				}
				lastConfirmedMessageID := s.oldVersionLastConfirmedTracker.Track(msg.MessageID())
				messageID := msg.MessageID()
				var err error
				msg, err = newOldVersionImmutableMessage(ctx, s.historicalWAL.Channel().Name, lastConfirmedMessageID, msg)
				if errors.Is(err, vchantempstore.ErrNotFound) {
					s.logger.Info(ctx, "skip the old version message because vchannel not found", mlog.Stringer("messageID", messageID))
					continue
				}
				if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
					return nil, err
				}
				if err != nil {
					panic("unreachable: unexpected error found: " + err.Error())
				}
			}

			if msg.TimeTick() <= s.exclusiveStartTimeTick {
				continue
			}

			shouldDeliver := s.exclusiveStartMessageID == nil || !msg.MessageID().EQ(s.exclusiveStartMessageID)
			if shouldDeliver {
				if shouldStartConsumeSpan(msg) {
					startConsumeSpanForMessage(ctx, msg)
				}
				if err := s.HandleMessage(ctx, msg); err != nil {
					return nil, err
				}
			}

			if msg.MessageType() == message.MessageTypeAlterWAL {
				alterWAL := message.MustAsImmutableAlterWALMessageV2(msg)
				targetWALName = message.WALName(alterWAL.Header().TargetWalName)
				alterWALTimeTick = msg.TimeTick()
				s.logger.Info(ctx, "historical reader found AlterWAL boundary",
					mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
					mlog.Stringer("targetWALName", targetWALName),
					mlog.Uint64("timeTick", alterWALTimeTick))
				continue
			}

			if alterWALTimeTick != 0 && msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() >= alterWALTimeTick {
				s.logger.Info(ctx, "historical reader reached AlterWAL visibility barrier",
					mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
					mlog.Stringer("currentWALName", s.innerWAL.WALName()),
					mlog.Uint64("timeTick", alterWALTimeTick))
				return s.newScannerAfterAlterWAL(ctx, targetWALName, alterWALTimeTick)
			}
		}
	}
}

func (s *historicalScanner) createHistoricalReaderWithFallback(ctx context.Context) (walimpls.ScannerImpls, bool, error) {
	timeout := s.getHistoricalWALFallbackTimeout()
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	backoffTimer := newScannerReadBackoffTimer()

	for {
		bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
		if bufSize < 0 {
			bufSize = 0
		}
		innerScanner, err := s.historicalWAL.Read(ctx, walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       s.deliverPolicy,
			ReadAheadBufferSize: bufSize,
		})
		if err == nil {
			return innerScanner, false, nil
		}
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if isHistoricalWALUnavailable(err) {
			s.logger.Warn(ctx, "historical WAL reader is permanently unavailable, falling back to current WAL",
				mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
				mlog.Stringer("currentWALName", s.innerWAL.WALName()),
				mlog.Err(err))
			return nil, true, nil
		}

		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn(ctx, "create historical WAL reader failed, retrying before fallback",
			mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err))
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-timeoutTimer.C:
			s.logger.Warn(ctx, "historical WAL reader creation timed out, falling back to current WAL",
				mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
				mlog.Stringer("currentWALName", s.innerWAL.WALName()),
				mlog.Duration("timeout", timeout),
				mlog.Err(err))
			return nil, true, nil
		case <-waker:
		}
	}
}

func (s *historicalScanner) newScannerAfterAlterWAL(
	ctx context.Context,
	targetWALName message.WALName,
	alterWALTimeTick uint64,
) (switchableScanner, error) {
	if targetWALName == s.innerWAL.WALName() {
		return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "historical WAL reached current WAL"), nil
	}
	if targetWALName == message.WALNameUnknown || targetWALName.String() == "" {
		s.logger.Warn(ctx, "AlterWAL target is invalid, falling back to current WAL",
			mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
			mlog.Stringer("currentWALName", s.innerWAL.WALName()))
		return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "invalid AlterWAL target"), nil
	}
	if s.historicalWALOpener == nil {
		s.logger.Warn(ctx, "historical WAL opener is unavailable for migration chain, falling back to current WAL",
			mlog.Stringer("targetWALName", targetWALName),
			mlog.Stringer("currentWALName", s.innerWAL.WALName()))
		return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "historical WAL opener unavailable"), nil
	}

	// A migration may legitimately return to a previously used WAL type, for
	// example rocksmq -> woodpecker -> rocksmq. Reopen it from the beginning and
	// use the strictly newer AlterWAL timetick as the exclusive boundary instead
	// of treating a repeated WAL name as a cycle.
	nextWAL, err := s.historicalWALOpener(ctx, targetWALName, s.innerWAL.Channel())
	if err != nil {
		if status.AsStreamingError(err).IsWALNameMismatch() {
			s.logger.Warn(ctx, "next historical WAL is unavailable, falling back to current WAL",
				mlog.Stringer("targetWALName", targetWALName),
				mlog.Stringer("currentWALName", s.innerWAL.WALName()),
				mlog.Err(err))
			return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "next historical WAL unavailable"), nil
		}
		return nil, err
	}
	actualWALName := nextWAL.WALName()
	if actualWALName != targetWALName {
		nextWAL.Close()
		s.logger.Warn(ctx, "historical WAL opener returned unexpected WAL, falling back to current WAL",
			mlog.Stringer("expectedWALName", targetWALName),
			mlog.Stringer("actualWALName", actualWALName),
			mlog.Stringer("currentWALName", s.innerWAL.WALName()))
		return s.newCurrentCatchupScanner(ctx, alterWALTimeTick, "unexpected historical WAL"), nil
	}

	if s.onReaderSwitch != nil {
		s.onReaderSwitch(targetWALName, metrics.WALReaderRoleHistorical)
	}
	return &historicalScanner{
		switchableScannerImpl:  s.switchableScannerImpl,
		historicalWAL:          nextWAL,
		deliverPolicy:          options.DeliverPolicyAll(),
		exclusiveStartTimeTick: alterWALTimeTick,
	}, nil
}

func (s *historicalScanner) newCurrentCatchupScanner(ctx context.Context, exclusiveStartTimeTick uint64, reason string) switchableScanner {
	s.logger.Info(ctx, "historical reader switching to current WAL",
		mlog.Stringer("historicalWALName", s.historicalWAL.WALName()),
		mlog.Stringer("currentWALName", s.innerWAL.WALName()),
		mlog.Uint64("exclusiveStartTimeTick", exclusiveStartTimeTick),
		mlog.String("reason", reason))
	if s.onReaderSwitch != nil {
		s.onReaderSwitch(s.innerWAL.WALName(), metrics.WALReaderRoleCurrent)
	}
	return &catchupScanner{
		switchableScannerImpl:  s.switchableScannerImpl,
		deliverPolicy:          options.DeliverPolicyAll(),
		exclusiveStartTimeTick: exclusiveStartTimeTick,
	}
}

func (s *historicalScanner) getHistoricalWALFallbackTimeout() time.Duration {
	if s.historicalWALFallbackTimeout > 0 {
		return s.historicalWALFallbackTimeout
	}
	return defaultHistoricalWALFallbackTimeout
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(timeout)
}

// catchupScanner is a scanner that make a read at underlying wal, and try to catchup the writeahead buffer then switch to tailing mode.
type catchupScanner struct {
	switchableScannerImpl
	deliverPolicy                  options.DeliverPolicy
	exclusiveStartTimeTick         uint64 // scanner should filter out the message that less than or equal to this time tick.
	oldVersionLastConfirmedTracker *oldVersionLastConfirmedTracker
}

func (s *catchupScanner) Do(ctx context.Context) (switchableScanner, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		scanner, err := s.createReaderWithBackoff(ctx, s.innerWAL, s.deliverPolicy)
		if err != nil {
			// Only the cancellation error will be returned, other error will keep backoff.
			return nil, err
		}
		switchedScanner, err := s.consumeWithScanner(ctx, scanner)
		if err != nil {
			s.logger.Warn(ctx, "scanner consuming was interrpurted with error, start a backoff", mlog.Err(err))
			continue
		}
		return switchedScanner, nil
	}
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
				s.logger.Info(ctx, "scanner consuming was interrpted because catup done",
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

func (s *switchableScannerImpl) createReaderWithBackoff(
	ctx context.Context,
	readWAL walimpls.ROWALImpls,
	deliverPolicy options.DeliverPolicy,
) (walimpls.ScannerImpls, error) {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
		if bufSize < 0 {
			bufSize = 0
		}
		innerScanner, err := readWAL.Read(ctx, walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       deliverPolicy,
			ReadAheadBufferSize: bufSize,
		})
		if err == nil {
			return innerScanner, nil
		}
		if ctx.Err() != nil {
			// The scanner is closing, so stop the backoff.
			return nil, ctx.Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn(ctx, "create underlying scanner for wal scanner, start a backoff",
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
			s.logger.Info(ctx, "scanner consuming was interrpted because tailing eviction",
				mlog.Uint64("timetick", s.lastConsumedMessage.TimeTick()),
				mlog.Stringer("messageID", s.lastConsumedMessage.MessageID()),
				mlog.Stringer("lastConfirmedMessageID", s.lastConsumedMessage.LastConfirmedMessageID()),
			)
			return &catchupScanner{
				switchableScannerImpl:  s.switchableScannerImpl,
				deliverPolicy:          options.DeliverPolicyStartFrom(s.lastConsumedMessage.LastConfirmedMessageID()),
				exclusiveStartTimeTick: s.lastConsumedMessage.TimeTick(),
			}, nil
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
