package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchantempstore"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	_ switchableScanner = (*tailingScanner)(nil)
	_ switchableScanner = (*catchupScanner)(nil)
	_ switchableScanner = (*switchableScannerAdaptorImpl)(nil)
)

type underlyingROWALImplsOpener func(
	ctx context.Context,
	walName message.WALName,
	channel types.PChannelInfo,
) (walimpls.ROWALImpls, error)

// newSwitchableScanner creates a new switchable scanner.
func newSwitchableScanner(
	scannerName string,
	logger *mlog.Logger,
	currentWAL walimpls.ROWALImpls,
	writeAheadBuffer wab.ROWriteAheadBuffer,
	deliverPolicy options.DeliverPolicy,
	msgChan chan<- message.ImmutableMessage,
	underlyingROWALImplsOpener underlyingROWALImplsOpener,
	onReaderSwitch func(message.WALName, string),
) switchableScanner {
	impl := switchableScannerImpl{
		scannerName:      scannerName,
		logger:           logger,
		innerWAL:         currentWAL,
		msgChan:          msgChan,
		writeAheadBuffer: writeAheadBuffer,
		onReaderSwitch:   onReaderSwitch,
	}
	startWALName, ok := getDeliverPolicyWALName(deliverPolicy)
	if !ok || startWALName == currentWAL.WALName() {
		return newCatchupScanner(impl, deliverPolicy, 0)
	}

	normalizedPolicy, excludedMessageID, err := normalizeSwitchableDeliverPolicy(deliverPolicy)
	if err != nil {
		logger.Warn(context.TODO(), "invalid cross-WAL start position, falling back to current WAL",
			mlog.Stringer("startWALName", startWALName),
			mlog.Stringer("currentWALName", currentWAL.WALName()),
			mlog.Err(err))
		return newCatchupScanner(impl, options.DeliverPolicyAll(), 0)
	}

	return &switchableScannerAdaptorImpl{
		switchableScannerImpl:      impl,
		underlyingROWALImplsOpener: underlyingROWALImplsOpener,
		walName:                    startWALName,
		deliverPolicy:              normalizedPolicy,
		excludedMessageID:          excludedMessageID,
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
	scannerName      string
	logger           *mlog.Logger
	innerWAL         walimpls.ROWALImpls
	msgChan          chan<- message.ImmutableMessage
	writeAheadBuffer wab.ROWriteAheadBuffer
	onReaderSwitch   func(message.WALName, string)
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

// switchableScannerAdaptorImpl transparently follows AlterWAL messages from
// the WAL encoded in the initial consumer position to the current WAL.
type switchableScannerAdaptorImpl struct {
	switchableScannerImpl
	underlyingROWALImplsOpener     underlyingROWALImplsOpener
	walName                        message.WALName
	deliverPolicy                  options.DeliverPolicy
	cutTs                          uint64
	excludedMessageID              message.MessageID
	oldVersionLastConfirmedTracker *oldVersionLastConfirmedTracker
}

func (s *switchableScannerAdaptorImpl) Do(ctx context.Context) (switchableScanner, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		underlyingWAL, err := s.openUnderlyingROWALImpls(ctx)
		if err != nil {
			if isReadWALUnavailable(err) {
				return s.newCurrentCatchupScanner(ctx, "WAL reader unavailable"), nil
			}
			return nil, err
		}

		switchedScanner, err := s.consumeWAL(ctx, underlyingWAL)
		underlyingWAL.Close()
		if err == nil {
			if switchedScanner != nil {
				return switchedScanner, nil
			}
			continue
		}
		if isReadWALUnavailable(err) {
			return s.newCurrentCatchupScanner(ctx, "WAL reader unavailable"), nil
		}
		s.logger.Warn(ctx, "switchable scanner consumption was interrupted, retrying",
			mlog.Stringer("walName", s.walName),
			mlog.Err(err))
	}
}

func (s *switchableScannerAdaptorImpl) consumeWAL(
	ctx context.Context,
	underlyingWAL walimpls.ROWALImpls,
) (switchableScanner, error) {
	scanner, err := s.createReaderWithBackoff(ctx, underlyingWAL, s.deliverPolicy, true)
	if err != nil {
		return nil, err
	}
	defer scanner.Close()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				if err := scanner.Error(); err != nil {
					return nil, err
				}
				s.logger.Info(ctx, "WAL reader reached end of stream, switching to current WAL",
					mlog.Stringer("walName", s.walName),
					mlog.Stringer("currentWALName", s.innerWAL.WALName()))
				return s.newCurrentCatchupScanner(ctx, "WAL reader exhausted"), nil
			}

			if msg.Version() == message.VersionOld {
				if s.oldVersionLastConfirmedTracker == nil {
					windowSize := paramtable.Get().StreamingCfg.OldVersionLastConfirmedWindowSize.GetAsInt()
					s.oldVersionLastConfirmedTracker = newOldVersionLastConfirmedTracker(windowSize)
				}
				lastConfirmedMessageID := s.oldVersionLastConfirmedTracker.Track(msg.MessageID())
				messageID := msg.MessageID()
				var err error
				msg, err = newOldVersionImmutableMessage(ctx, underlyingWAL.Channel().Name, lastConfirmedMessageID, msg)
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

			if msg.TimeTick() <= s.cutTs {
				continue
			}

			shouldDeliver := s.excludedMessageID == nil || !msg.MessageID().EQ(s.excludedMessageID)
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
				targetWALName := message.WALName(alterWAL.Header().TargetWalName)
				s.logger.Info(ctx, "switchable reader found AlterWAL boundary",
					mlog.Stringer("sourceWALName", s.walName),
					mlog.Stringer("targetWALName", targetWALName),
					mlog.Uint64("timeTick", msg.TimeTick()))
				return s.switchTo(ctx, targetWALName, msg.TimeTick()), nil
			}
		}
	}
}

func (s *switchableScannerAdaptorImpl) switchTo(
	ctx context.Context,
	targetWALName message.WALName,
	cutTs uint64,
) switchableScanner {
	if targetWALName == message.WALNameUnknown || targetWALName.String() == "" {
		s.logger.Warn(ctx, "AlterWAL target is invalid, switching to current WAL",
			mlog.Stringer("sourceWALName", s.walName),
			mlog.Stringer("currentWALName", s.innerWAL.WALName()))
		s.cutTs = cutTs
		return s.newCurrentCatchupScanner(ctx, "invalid AlterWAL target")
	}

	s.walName = targetWALName
	s.deliverPolicy = options.DeliverPolicyAll()
	s.cutTs = cutTs
	s.excludedMessageID = nil
	s.oldVersionLastConfirmedTracker = nil
	if targetWALName == s.innerWAL.WALName() {
		return s.newCurrentCatchupScanner(ctx, "AlterWAL reached current WAL")
	}
	if s.onReaderSwitch != nil {
		s.onReaderSwitch(targetWALName, metrics.WALReaderRoleHistorical)
	}
	return nil
}

func (s *switchableScannerAdaptorImpl) openUnderlyingROWALImpls(ctx context.Context) (walimpls.ROWALImpls, error) {
	if s.underlyingROWALImplsOpener == nil {
		return nil, status.NewWALNameMismatchError(s.innerWAL.WALName().String(), s.walName.String())
	}
	underlyingWAL, err := s.underlyingROWALImplsOpener(ctx, s.walName, s.innerWAL.Channel())
	if err != nil {
		return nil, err
	}
	if underlyingWAL.WALName() != s.walName {
		actualWALName := underlyingWAL.WALName()
		underlyingWAL.Close()
		return nil, status.NewWALNameMismatchError(actualWALName.String(), s.walName.String())
	}
	return underlyingWAL, nil
}

func (s *switchableScannerAdaptorImpl) newCurrentCatchupScanner(ctx context.Context, reason string) switchableScanner {
	s.logger.Info(ctx, "switchable reader switching to current WAL",
		mlog.Stringer("sourceWALName", s.walName),
		mlog.Stringer("currentWALName", s.innerWAL.WALName()),
		mlog.Uint64("exclusiveStartTimeTick", s.cutTs),
		mlog.String("reason", reason))
	if s.onReaderSwitch != nil {
		s.onReaderSwitch(s.innerWAL.WALName(), metrics.WALReaderRoleCurrent)
	}
	return newCatchupScanner(s.switchableScannerImpl, options.DeliverPolicyAll(), s.cutTs)
}

func normalizeSwitchableDeliverPolicy(deliverPolicy options.DeliverPolicy) (options.DeliverPolicy, message.MessageID, error) {
	switch policy := deliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		_, err := message.UnmarshalMessageID(policy.StartFrom)
		return deliverPolicy, nil, err
	case *streamingpb.DeliverPolicy_StartAfter:
		messageID, err := message.UnmarshalMessageID(policy.StartAfter)
		if err != nil {
			return nil, nil, err
		}
		return options.DeliverPolicyStartFrom(messageID), messageID, nil
	default:
		return deliverPolicy, nil, nil
	}
}

func getDeliverPolicyWALName(deliverPolicy options.DeliverPolicy) (message.WALName, bool) {
	if deliverPolicy == nil {
		return message.WALNameUnknown, false
	}
	var msgID *commonpb.MessageID
	switch policy := deliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		msgID = policy.StartFrom
	case *streamingpb.DeliverPolicy_StartAfter:
		msgID = policy.StartAfter
	default:
		return message.WALNameUnknown, false
	}
	if msgID == nil {
		return message.WALNameUnknown, false
	}
	return message.WALName(msgID.WALName), true
}

func isReadWALUnavailable(err error) bool {
	return status.AsStreamingError(err).IsWALNameMismatch() || errors.Is(err, merr.ErrMqTopicNotFound)
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
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		scanner, err := s.createReaderWithBackoff(ctx, s.innerWAL, s.deliverPolicy, false)
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
	underlyingWAL walimpls.ROWALImpls,
	deliverPolicy options.DeliverPolicy,
	stopOnUnavailable bool,
) (walimpls.ScannerImpls, error) {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
		if bufSize < 0 {
			bufSize = 0
		}
		innerScanner, err := underlyingWAL.Read(ctx, walimpls.ReadOption{
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
		if stopOnUnavailable && isReadWALUnavailable(err) {
			return nil, err
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn(ctx, "create underlying scanner for wal scanner, start a backoff",
			mlog.Stringer("walName", underlyingWAL.WALName()),
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
