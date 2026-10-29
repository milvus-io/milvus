package utility

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchantempstore"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	walCatchUpSlots       = make(chan struct{}, 4)
	walCatchUpReadLimiter = rate.NewLimiter(16<<20, 1<<20)
	walCatchUpScannerID   atomic.Int64
)

// WALSwitchCatchUpOptions selects the old WAL position and messages to replay after a switch.
type WALSwitchCatchUpOptions struct {
	PChannel               string
	VChannel               string
	DeliverPolicy          options.DeliverPolicy
	DeliverFilters         []options.DeliverFilter
	MessageHandler         message.Handler
	IgnorePauseConsumption bool
	ResolveVChannel        func(context.Context, int64, string) (string, error)
}

// CatchUpWALAfterSwitch replays the old WAL read-only through AlterWAL and returns
// its TimeTick after selected messages are handled. It never closes MessageHandler.
func CatchUpWALAfterSwitch(ctx context.Context, opts WALSwitchCatchUpOptions) (uint64, error) {
	var startMessageID, startAfterMessageID message.MessageID
	var err error
	switch policy := opts.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		startMessageID, err = message.UnmarshalMessageID(policy.StartFrom)
	case *streamingpb.DeliverPolicy_StartAfter:
		startMessageID, err = message.UnmarshalMessageID(policy.StartAfter)
		startAfterMessageID = startMessageID
	default:
		return 0, status.NewUnrecoverableError("historical catch-up requires a WAL-specific position")
	}
	if err != nil {
		return 0, merr.Wrap(err, "decode historical WAL position")
	}
	// Open inclusively so StartAfter on AlterWAL cannot hide the stop marker.
	opts.DeliverPolicy = options.DeliverPolicyStartFrom(startMessageID)
	select {
	case walCatchUpSlots <- struct{}{}:
		defer func() { <-walCatchUpSlots }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	// RocksMQ shares the existing standalone server during this switch.
	if startMessageID.WALName() == message.WALNameRocksmq && (!paramtable.IsStandalone() || server.Rmq == nil) {
		return 0, status.NewUnrecoverableError("historical RocksMQ requires the existing standalone server")
	}
	// Backend init registers its ID unmarshaler and builder together, so a
	// successfully decoded position already identifies a registered backend.
	opener, err := registry.MustGetBuilder(startMessageID.WALName()).Build()
	if err != nil {
		return 0, err
	}
	defer opener.Close()
	return catchUpWALWithRetry(ctx, opts, startAfterMessageID, opener)
}

// Once any output has been handled, retry here with its checkpoint instead of
// returning stale construction options to Resuming. All state is call-local.
func catchUpWALWithRetry(ctx context.Context, opts WALSwitchCatchUpOptions, startAfterMessageID message.MessageID, opener walimpls.OpenerImpls) (uint64, error) {
	logger := mlog.With(mlog.FieldPChannel(opts.PChannel), mlog.FieldVChannel(opts.VChannel))
	channel := types.PChannelInfo{Name: opts.PChannel, AccessMode: types.AccessModeRO}
	metrics := metricsutil.NewScanMetrics(channel).NewScannerMetrics()
	// Standalone may share these metric series with SN; release this scanner's
	// contribution without deleting the shared series.
	defer metrics.Close()
	progress := &catchUpProgressHandler{Handler: opts.MessageHandler}
	readRetryBackoff := 100 * time.Millisecond
	for {
		for !opts.IgnorePauseConsumption && paramtable.Get().StreamingCfg.WALScannerPauseConsumption.GetAsBool() {
			if err := waitForCatchUpRetry(ctx, 100*time.Millisecond); err != nil {
				return 0, err
			}
		}
		alterWALTimeTick, err := func() (uint64, error) {
			w, err := opener.Open(ctx, &walimpls.OpenOption{Channel: channel})
			if err != nil {
				return 0, err
			}
			defer w.Close()
			scanner, err := w.Read(ctx, walimpls.ReadOption{
				Name:          fmt.Sprintf("wal-switch-catchup-%d-%d", paramtable.GetNodeID(), walCatchUpScannerID.Add(1)),
				DeliverPolicy: opts.DeliverPolicy, ReadAheadBufferSize: 1,
			})
			if err != nil {
				return 0, err
			}
			defer scanner.Close()
			r := newWALSwitchCatchUpReader(opts.PChannel, opts.VChannel, logger, metrics, opts.DeliverFilters)
			r.ResolveVChannel, r.WaitForReadBudget, r.StartAfterMessageID = opts.ResolveVChannel, waitForCatchUpReadBudget, startAfterMessageID
			return r.Read(ctx, scanner, progress)
		}()
		if err == nil {
			return alterWALTimeTick, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || progress.lastHandledMessage == nil {
			return 0, err
		}
		if lastConfirmedMessageID := progress.lastHandledMessage.LastConfirmedMessageID(); lastConfirmedMessageID != nil {
			opts.DeliverPolicy = options.DeliverPolicyStartFrom(lastConfirmedMessageID)
			startAfterMessageID = nil
		}
		filters := make([]options.DeliverFilter, 0, len(opts.DeliverFilters)+1)
		for _, filter := range opts.DeliverFilters {
			if !options.IsDeliverFilterTimeTick(filter) {
				filters = append(filters, filter)
			}
		}
		if progress.lastHandledMessage.Version() == message.VersionOld {
			opts.DeliverFilters = append(filters, options.DeliverFilterTimeTickGTE(progress.lastHandledMessage.TimeTick()))
		} else {
			opts.DeliverFilters = append(filters, options.DeliverFilterTimeTickGT(progress.lastHandledMessage.TimeTick()))
		}
		logger.RatedWarn(ctx, 1, "retry historical WAL from successfully handled progress", mlog.Err(err))
		if err := waitForCatchUpRetry(ctx, readRetryBackoff); err != nil {
			return 0, err
		}
		readRetryBackoff = min(readRetryBackoff*2, 5*time.Second)
	}
}

func waitForCatchUpRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitForCatchUpReadBudget(ctx context.Context, size int) error {
	for size > 0 {
		n := min(size, walCatchUpReadLimiter.Burst())
		if err := walCatchUpReadLimiter.WaitN(ctx, n); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return merr.WrapErrServiceUnavailableErr(err, "historical WAL read rate limit")
		}
		size -= n
	}
	return nil
}

type catchUpProgressHandler struct {
	message.Handler
	lastHandledMessage message.ImmutableMessage
}

func (p *catchUpProgressHandler) Handle(param message.HandleParam) message.HandleResult {
	result := p.Handler.Handle(param)
	if result.MessageHandled && param.Message != nil {
		p.lastHandledMessage = param.Message
	}
	return result
}

func (*catchUpProgressHandler) Close() {}

// walSwitchCatchUpReader directly composes the existing low-level SN buffers. It has
// no WAB, scanner lifecycle, recovery catalog, or normal-reader mode to manage.
type walSwitchCatchUpReader struct {
	legacyConfirmWindowSize int
	PChannel                string
	VChannel                string
	Metrics                 *metricsutil.ScannerMetrics
	Filter                  func(message.ImmutableMessage) bool
	ReorderBuffer           *ReOrderByTimeTickBuffer
	TxnBuffer               *TxnBuffer
	PendingQueue            *PendingQueue
	ResolveVChannel         func(context.Context, int64, string) (string, error)
	WaitForReadBudget       func(context.Context, int) error
	StartAfterMessageID     message.MessageID
}

func newWALSwitchCatchUpReader(pchannel, vchannel string, logger *mlog.Logger, metrics *metricsutil.ScannerMetrics, filters []options.DeliverFilter) *walSwitchCatchUpReader {
	legacyConfirmWindowSize := paramtable.Get().StreamingCfg.OldVersionLastConfirmedWindowSize.GetAsInt()
	return &walSwitchCatchUpReader{
		legacyConfirmWindowSize: legacyConfirmWindowSize,
		PChannel:                pchannel, VChannel: vchannel, Metrics: metrics,
		Filter: options.GetFilterFunc(filters), ReorderBuffer: NewReOrderBuffer(),
		TxnBuffer: NewTxnBuffer(logger, metrics), PendingQueue: NewPendingQueue(),
	}
}

func (r *walSwitchCatchUpReader) Read(ctx context.Context, scanner walimpls.ScannerImpls, handler message.Handler) (uint64, error) {
	var legacyMessageIDs []message.MessageID
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				if err := scanner.Error(); err != nil {
					return 0, err
				}
				return 0, status.NewUnrecoverableError("historical WAL ended before AlterWAL on %s", r.PChannel)
			}
			if r.WaitForReadBudget != nil {
				if err := r.WaitForReadBudget(ctx, msg.EstimateSize()); err != nil {
					return 0, err
				}
			}
			if msg.Version() == message.VersionOld {
				// Mirror the existing SN compatibility window locally; no normal
				// scanner or tracker needs to change for this maintenance path.
				legacyMessageIDs = append(legacyMessageIDs, msg.MessageID())
				lastConfirmedMessageID := legacyMessageIDs[0]
				if len(legacyMessageIDs) > r.legacyConfirmWindowSize {
					legacyMessageIDs = legacyMessageIDs[1:]
				}
				var err error
				msg, err = ConvertOldVersionMessage(ctx, r.PChannel, lastConfirmedMessageID, msg, r.ResolveVChannel)
				if errors.Is(err, vchantempstore.ErrNotFound) {
					continue
				}
				if err != nil {
					return 0, err
				}
			}
			isAlterWAL := msg.MessageType() == message.MessageTypeAlterWAL
			if err := r.bufferMessage(msg, isAlterWAL); err != nil {
				return 0, err
			}
			for r.PendingQueue.Len() > 0 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				pending := r.PendingQueue.Next()
				if pending.MessageType() == message.MessageTypeAlterWAL {
					r.PendingQueue.UnsafeAdvance()
					r.Metrics.UpdatePendingQueueSize(r.PendingQueue.Bytes())
					return pending.TimeTick(), nil
				}
				if r.StartAfterMessageID == nil || !pending.MessageID().EQ(r.StartAfterMessageID) {
					result := handler.Handle(message.HandleParam{Ctx: ctx, Message: pending})
					if result.Error != nil {
						return 0, result.Error
					}
					if !result.MessageHandled {
						continue
					}
				}
				r.PendingQueue.UnsafeAdvance()
				r.Metrics.UpdatePendingQueueSize(r.PendingQueue.Bytes())
			}
		}
	}
}

func (r *walSwitchCatchUpReader) bufferMessage(msg message.ImmutableMessage, isAlterWAL bool) error {
	if !isAlterWAL && !r.Filter(msg) {
		return nil
	}
	r.Metrics.ObserveMessage(false, msg.MessageType(), msg.EstimateSize())
	if isAlterWAL || msg.MessageType() == message.MessageTypeTimeTick {
		ordered := r.ReorderBuffer.PopUtilTimeTick(msg.TimeTick())
		if isAlterWAL {
			ordered = append(ordered, msg)
		}
		output := r.TxnBuffer.HandleImmutableMessages(ordered, msg.TimeTick())
		if !isAlterWAL && (msg.MessageType() != message.MessageTypeTimeTick || msg.IsPersisted() || len(output) == 0) {
			output = append(output, msg)
		}
		r.PendingQueue.Add(output)
		r.Metrics.UpdateTimeTickBufSize(r.ReorderBuffer.Bytes())
		r.Metrics.UpdateTxnBufSize(r.TxnBuffer.Bytes())
		r.Metrics.UpdatePendingQueueSize(r.PendingQueue.Bytes())
		return nil
	}
	if msg.VChannel() != "" && r.VChannel != "" && msg.VChannel() != r.VChannel {
		return nil
	}
	err := r.ReorderBuffer.Push(msg)
	if errors.Is(err, ErrTimeTickVoilation) {
		return errors.WithSecondaryError(status.NewUnrecoverableError("historical WAL violates confirmed message ordering"), err)
	}
	// Other Push outcomes are physical message-ID/TimeTick duplicates, handled
	// by the existing buffer. Only this reader's metrics contribution is updated.
	r.Metrics.UpdateTimeTickBufSize(r.ReorderBuffer.Bytes())
	return nil
}
