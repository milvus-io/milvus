package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ walimpls.ScannerImpls = (*underlyingWALScannerAdaptor)(nil)

type underlyingROWALImplsOpener func(
	ctx context.Context,
	walName message.WALName,
	channel types.PChannelInfo,
) (walimpls.ROWALImpls, error)

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

// underlyingWALScannerAdaptor exposes one continuous ScannerImpls stream backed
// by a sequence of underlying WAL scanners. An AlterWAL message replaces the
// active underlying WAL and scanner without involving the caller.
//
// This adaptor only joins persisted WAL streams. It does not manage the
// catchup/tailing transition or access the write-ahead buffer.
type underlyingWALScannerAdaptor struct {
	*helper.ScannerHelper

	logger                     *mlog.Logger
	channel                    types.PChannelInfo
	underlyingROWALImplsOpener underlyingROWALImplsOpener
	onUnderlyingScannerChanged func(message.WALName)

	underlyingWALName      message.WALName
	underlyingReadOption   walimpls.ReadOption
	cutTs                  uint64
	excludedMessageID      message.MessageID
	waitingForSwitchTarget bool
	messageCh              chan message.ImmutableMessage
}

func newUnderlyingWALScannerAdaptor(
	logger *mlog.Logger,
	channel types.PChannelInfo,
	readOption walimpls.ReadOption,
	underlyingROWALImplsOpener underlyingROWALImplsOpener,
	onUnderlyingScannerChanged func(message.WALName),
) (walimpls.ScannerImpls, error) {
	walName, ok := getDeliverPolicyWALName(readOption.DeliverPolicy)
	if !ok {
		return nil, status.NewUnrecoverableError("underlying WAL scanner requires a WAL-specific start position")
	}
	if underlyingROWALImplsOpener == nil {
		return nil, status.NewUnrecoverableError("underlying WAL opener is unavailable for %s", walName)
	}

	normalizedPolicy, excludedMessageID, err := normalizeUnderlyingWALDeliverPolicy(readOption.DeliverPolicy)
	if err != nil {
		return nil, status.NewUnrecoverableError(
			"invalid underlying WAL start position for %s: %s", walName, err.Error(),
		)
	}
	readOption.DeliverPolicy = normalizedPolicy

	adaptor := &underlyingWALScannerAdaptor{
		ScannerHelper:              helper.NewScannerHelper(readOption.Name),
		logger:                     logger,
		channel:                    channel,
		underlyingROWALImplsOpener: underlyingROWALImplsOpener,
		onUnderlyingScannerChanged: onUnderlyingScannerChanged,
		underlyingWALName:          walName,
		underlyingReadOption:       readOption,
		excludedMessageID:          excludedMessageID,
		messageCh:                  make(chan message.ImmutableMessage),
	}
	go adaptor.execute()
	return adaptor, nil
}

func (a *underlyingWALScannerAdaptor) Chan() <-chan message.ImmutableMessage {
	return a.messageCh
}

func (a *underlyingWALScannerAdaptor) Close() error {
	return a.ScannerHelper.Close()
}

func (a *underlyingWALScannerAdaptor) execute() {
	var executeErr error
	defer func() {
		close(a.messageCh)
		a.Finish(executeErr)
	}()

	executeErr = a.consume()
	if a.Context().Err() != nil && errors.Is(executeErr, context.Canceled) {
		executeErr = nil
	}
}

func (a *underlyingWALScannerAdaptor) consume() error {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		if a.Context().Err() != nil {
			return a.Context().Err()
		}

		underlyingWAL, err := a.openUnderlyingWAL(a.Context())
		if err != nil {
			if a.isUnderlyingWALUnavailable(err) {
				return newUnderlyingWALUnavailableError(a.underlyingWALName, err)
			}
			if !a.shouldRetrySwitchTarget(err) {
				return err
			}
			if a.Context().Err() != nil {
				return a.Context().Err()
			}
			waker, nextInterval := backoffTimer.NextTimer()
			a.logger.Warn(a.Context(), "open underlying WAL failed, start a backoff",
				mlog.Stringer("walName", a.underlyingWALName),
				mlog.Duration("nextInterval", nextInterval),
				mlog.Err(err))
			select {
			case <-a.Context().Done():
				return a.Context().Err()
			case <-waker:
			}
			continue
		}

		err = a.consumeUnderlying(a.Context(), underlyingWAL)
		underlyingWAL.Close()
		if err == nil {
			backoffTimer = newScannerReadBackoffTimer()
			continue
		}
		if a.isUnderlyingWALUnavailable(err) {
			return newUnderlyingWALUnavailableError(a.underlyingWALName, err)
		}
		if status.AsStreamingError(err).IsUnrecoverable() {
			return err
		}
		if a.Context().Err() != nil {
			return a.Context().Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		a.logger.Warn(a.Context(), "underlying WAL scanner was interrupted, start a backoff",
			mlog.Stringer("walName", a.underlyingWALName),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err))
		select {
		case <-a.Context().Done():
			return a.Context().Err()
		case <-waker:
		}
	}
}

func (a *underlyingWALScannerAdaptor) consumeUnderlying(
	ctx context.Context,
	underlyingWAL walimpls.ROWALImpls,
) error {
	underlyingScanner, err := a.createUnderlyingScannerWithBackoff(ctx, underlyingWAL)
	if err != nil {
		return err
	}
	a.waitingForSwitchTarget = false
	defer underlyingScanner.Close()
	if a.onUnderlyingScannerChanged != nil {
		a.onUnderlyingScannerChanged(a.underlyingWALName)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-underlyingScanner.Chan():
			if !ok {
				if err := underlyingScanner.Error(); err != nil {
					return err
				}
				return status.NewUnrecoverableError(
					"underlying WAL %s reached end of stream before an AlterWAL boundary", a.underlyingWALName,
				)
			}

			// Raw V0 messages do not carry the streaming `_tt` property. They must
			// reach catchupScanner, which converts them to V1 before inspecting
			// TimeTick or MessageType. AlterWAL is never encoded as a V0 message.
			if msg.Version() == message.VersionOld {
				// V0 messages can only belong to the history before the first WAL
				// switch. Once cutTs is set, a V0 message from the next WAL is stale
				// residue from reusing a backend and must not regress the TimeTick.
				if a.cutTs > 0 {
					continue
				}
				if a.excludedMessageID == nil || !msg.MessageID().EQ(a.excludedMessageID) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case a.messageCh <- msg:
					}
				}
				continue
			}

			if msg.TimeTick() <= a.cutTs {
				continue
			}

			messageType := msg.MessageType()
			messageTimeTick := msg.TimeTick()
			var targetWALName message.WALName
			if messageType == message.MessageTypeAlterWAL {
				alterWAL := message.MustAsImmutableAlterWALMessageV2(msg)
				targetWALName = message.WALName(alterWAL.Header().TargetWalName)
				if targetWALName == message.WALNameUnknown || targetWALName.String() == "" {
					return status.NewUnrecoverableError(
						"underlying WAL %s contains an AlterWAL boundary with invalid target %d",
						a.underlyingWALName,
						targetWALName,
					)
				}
			}

			shouldDeliver := a.excludedMessageID == nil || !msg.MessageID().EQ(a.excludedMessageID)
			if shouldDeliver {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case a.messageCh <- msg:
				}
			}

			// The receiver may attach trace context to the message as soon as it is
			// delivered, so do not access msg below this point.
			if messageType != message.MessageTypeAlterWAL {
				continue
			}

			a.logger.Info(ctx, "underlying WAL scanner found AlterWAL boundary",
				mlog.Stringer("sourceWALName", a.underlyingWALName),
				mlog.Stringer("targetWALName", targetWALName),
				mlog.Uint64("timeTick", messageTimeTick))

			a.underlyingWALName = targetWALName
			a.underlyingReadOption.DeliverPolicy = options.DeliverPolicyAll()
			a.cutTs = messageTimeTick
			a.excludedMessageID = nil
			a.waitingForSwitchTarget = true
			return nil
		}
	}
}

func (a *underlyingWALScannerAdaptor) openUnderlyingWAL(
	ctx context.Context,
) (walimpls.ROWALImpls, error) {
	underlyingWAL, err := a.underlyingROWALImplsOpener(ctx, a.underlyingWALName, a.channel)
	if err != nil {
		return nil, err
	}
	if underlyingWAL.WALName() != a.underlyingWALName {
		actualWALName := underlyingWAL.WALName()
		underlyingWAL.Close()
		return nil, status.NewWALNameMismatchError(actualWALName.String(), a.underlyingWALName.String())
	}
	return underlyingWAL, nil
}

func (a *underlyingWALScannerAdaptor) createUnderlyingScannerWithBackoff(
	ctx context.Context,
	underlyingWAL walimpls.ROWALImpls,
) (walimpls.ScannerImpls, error) {
	backoffTimer := newScannerReadBackoffTimer()
	for {
		underlyingScanner, err := underlyingWAL.Read(ctx, a.underlyingReadOption)
		if err == nil {
			return underlyingScanner, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if isReadWALUnavailable(err) {
			return nil, err
		}
		if status.AsStreamingError(err).IsUnrecoverable() {
			return nil, err
		}

		waker, nextInterval := backoffTimer.NextTimer()
		a.logger.Warn(ctx, "create underlying WAL scanner failed, start a backoff",
			mlog.Stringer("walName", underlyingWAL.WALName()),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waker:
		}
	}
}

func normalizeUnderlyingWALDeliverPolicy(
	deliverPolicy options.DeliverPolicy,
) (options.DeliverPolicy, message.MessageID, error) {
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

func isReadWALUnavailable(err error) bool {
	return status.AsStreamingError(err).IsWALNameMismatch() || errors.Is(err, merr.ErrMqTopicNotFound)
}

func (a *underlyingWALScannerAdaptor) isUnderlyingWALUnavailable(err error) bool {
	if status.AsStreamingError(err).IsWALNameMismatch() {
		return true
	}
	return !a.waitingForSwitchTarget && errors.Is(err, merr.ErrMqTopicNotFound)
}

func (a *underlyingWALScannerAdaptor) shouldRetrySwitchTarget(err error) bool {
	return a.waitingForSwitchTarget && errors.Is(err, merr.ErrMqTopicNotFound)
}

func newUnderlyingWALUnavailableError(walName message.WALName, err error) error {
	return status.NewUnrecoverableError("underlying WAL %s is unavailable: %s", walName, err.Error())
}
