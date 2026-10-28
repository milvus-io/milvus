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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	historicalReadSlots = make(chan struct{}, 4)
	historicalReadRate  = rate.NewLimiter(16<<20, 1<<20)
	historicalReaderID  atomic.Int64
)

// HistoricalReadOptions describes one maintenance read of the previous WAL.
type HistoricalReadOptions struct {
	PChannel               string
	VChannel               string
	DeliverPolicy          options.DeliverPolicy
	DeliverFilters         []options.DeliverFilter
	MessageHandler         message.Handler
	IgnorePauseConsumption bool
	ResolveVChannel        func(context.Context, int64, string) (string, error)
}

// ReadHistoricalWAL opens the explicitly named old backend read-only and drains
// selected messages through AlterWAL. It never owns or closes MessageHandler.
func ReadHistoricalWAL(ctx context.Context, opts HistoricalReadOptions) (uint64, error) {
	var id, excluded message.MessageID
	var err error
	switch policy := opts.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err = message.UnmarshalMessageID(policy.StartFrom)
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err = message.UnmarshalMessageID(policy.StartAfter)
		excluded = id
	default:
		return 0, status.NewUnrecoverableError("historical catch-up requires a WAL-specific position")
	}
	if err != nil {
		return 0, merr.Wrap(err, "decode historical WAL position")
	}
	// Open inclusively so StartAfter on AlterWAL cannot hide the stop marker.
	opts.DeliverPolicy = options.DeliverPolicyStartFrom(id)
	select {
	case historicalReadSlots <- struct{}{}:
		defer func() { <-historicalReadSlots }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	// RocksMQ shares the existing standalone server during this switch.
	if id.WALName() == message.WALNameRocksmq && (!paramtable.IsStandalone() || server.Rmq == nil) {
		return 0, status.NewUnrecoverableError("historical RocksMQ requires the existing standalone server")
	}
	// Backend init registers its ID unmarshaler and builder together, so a
	// successfully decoded position already identifies a registered backend.
	opener, err := registry.MustGetBuilder(id.WALName()).Build()
	if err != nil {
		return 0, err
	}
	defer opener.Close()
	return readHistoricalWAL(ctx, opts, excluded, opener)
}

// Once any output has been handled, retry here with its checkpoint instead of
// returning stale construction options to Resuming. All state is call-local.
func readHistoricalWAL(ctx context.Context, opts HistoricalReadOptions, excluded message.MessageID, opener walimpls.OpenerImpls) (uint64, error) {
	logger := mlog.With(mlog.FieldPChannel(opts.PChannel), mlog.FieldVChannel(opts.VChannel))
	channel := types.PChannelInfo{Name: opts.PChannel, AccessMode: types.AccessModeRO}
	metrics := metricsutil.NewScanMetrics(channel).NewScannerMetrics()
	// Standalone may share these metric series with SN; release this scanner's
	// contribution without deleting the shared series.
	defer metrics.Close()
	progress := &historicalProgress{Handler: opts.MessageHandler}
	backoff := 100 * time.Millisecond
	for {
		for !opts.IgnorePauseConsumption && paramtable.Get().StreamingCfg.WALScannerPauseConsumption.GetAsBool() {
			if err := waitHistoricalRetry(ctx, 100*time.Millisecond); err != nil {
				return 0, err
			}
		}
		cut, err := func() (uint64, error) {
			w, err := opener.Open(ctx, &walimpls.OpenOption{Channel: channel})
			if err != nil {
				return 0, err
			}
			defer w.Close()
			scanner, err := w.Read(ctx, walimpls.ReadOption{
				Name:          fmt.Sprintf("historical-%d-%d", paramtable.GetNodeID(), historicalReaderID.Add(1)),
				DeliverPolicy: opts.DeliverPolicy, ReadAheadBufferSize: 1,
			})
			if err != nil {
				return 0, err
			}
			defer scanner.Close()
			r := newHistoricalReader(opts.PChannel, opts.VChannel, logger, metrics, opts.DeliverFilters)
			r.ResolveVChannel, r.WaitRead, r.ExcludeMessageID = opts.ResolveVChannel, waitHistoricalRead, excluded
			return r.Read(ctx, scanner, progress)
		}()
		if err == nil {
			return cut, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || progress.last == nil {
			return 0, err
		}
		if confirmed := progress.last.LastConfirmedMessageID(); confirmed != nil {
			opts.DeliverPolicy = options.DeliverPolicyStartFrom(confirmed)
			excluded = nil
		}
		filters := make([]options.DeliverFilter, 0, len(opts.DeliverFilters)+1)
		for _, filter := range opts.DeliverFilters {
			if !options.IsDeliverFilterTimeTick(filter) {
				filters = append(filters, filter)
			}
		}
		if progress.last.Version() == message.VersionOld {
			opts.DeliverFilters = append(filters, options.DeliverFilterTimeTickGTE(progress.last.TimeTick()))
		} else {
			opts.DeliverFilters = append(filters, options.DeliverFilterTimeTickGT(progress.last.TimeTick()))
		}
		logger.RatedWarn(ctx, 1, "retry historical WAL from successfully handled progress", mlog.Err(err))
		if err := waitHistoricalRetry(ctx, backoff); err != nil {
			return 0, err
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func waitHistoricalRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitHistoricalRead(ctx context.Context, size int) error {
	for size > 0 {
		n := min(size, historicalReadRate.Burst())
		if err := historicalReadRate.WaitN(ctx, n); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return merr.WrapErrServiceUnavailableErr(err, "historical WAL read rate limit")
		}
		size -= n
	}
	return nil
}

type historicalProgress struct {
	message.Handler
	last message.ImmutableMessage
}

func (p *historicalProgress) Handle(param message.HandleParam) message.HandleResult {
	result := p.Handler.Handle(param)
	if result.MessageHandled && param.Message != nil {
		p.last = param.Message
	}
	return result
}

func (*historicalProgress) Close() {}

// historicalReader directly composes the existing low-level SN buffers. It has
// no WAB, scanner lifecycle, recovery catalog, or normal-reader mode to manage.
type historicalReader struct {
	legacyWindow     int
	PChannel         string
	VChannel         string
	Metrics          *metricsutil.ScannerMetrics
	Filter           func(message.ImmutableMessage) bool
	ReorderBuffer    *ReOrderByTimeTickBuffer
	TxnBuffer        *TxnBuffer
	PendingQueue     *PendingQueue
	ResolveVChannel  func(context.Context, int64, string) (string, error)
	WaitRead         func(context.Context, int) error
	ExcludeMessageID message.MessageID
}

func newHistoricalReader(pchannel, vchannel string, logger *mlog.Logger, metrics *metricsutil.ScannerMetrics, filters []options.DeliverFilter) *historicalReader {
	window := paramtable.Get().StreamingCfg.OldVersionLastConfirmedWindowSize.GetAsInt()
	return &historicalReader{
		legacyWindow: window,
		PChannel:     pchannel, VChannel: vchannel, Metrics: metrics,
		Filter: options.GetFilterFunc(filters), ReorderBuffer: NewReOrderBuffer(),
		TxnBuffer: NewTxnBuffer(logger, metrics), PendingQueue: NewPendingQueue(),
	}
}

func (r *historicalReader) Read(ctx context.Context, scanner walimpls.ScannerImpls, handler message.Handler) (uint64, error) {
	var chunks message.ChunkAssembler
	var legacyIDs []message.MessageID
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
			if r.WaitRead != nil {
				if err := r.WaitRead(ctx, msg.EstimateSize()); err != nil {
					return 0, err
				}
			}
			assembled, handled, err := chunks.Push(msg)
			if err != nil {
				return 0, errors.WithSecondaryError(status.NewUnrecoverableError("historical WAL contains malformed chunks"), err)
			}
			if handled {
				if assembled == nil {
					continue
				}
				msg = assembled
			}
			if msg.Version() == message.VersionOld {
				// Mirror the existing SN compatibility window locally; no normal
				// scanner or tracker needs to change for this maintenance path.
				legacyIDs = append(legacyIDs, msg.MessageID())
				confirmed := legacyIDs[0]
				if len(legacyIDs) > r.legacyWindow {
					legacyIDs = legacyIDs[1:]
				}
				msg, err = ConvertOldVersionMessage(ctx, r.PChannel, confirmed, msg, r.ResolveVChannel)
				if errors.Is(err, vchantempstore.ErrNotFound) {
					continue
				}
				if err != nil {
					return 0, err
				}
				if msg.MessageType() == message.MessageTypeTimeTick {
					chunks.AdvanceTimeTick(msg.TimeTick())
				}
			}
			end := msg.MessageType() == message.MessageTypeAlterWAL
			if err := r.buffer(msg, end); err != nil {
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
				if r.ExcludeMessageID == nil || !pending.MessageID().EQ(r.ExcludeMessageID) {
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

func (r *historicalReader) buffer(msg message.ImmutableMessage, end bool) error {
	if !end && !r.Filter(msg) {
		return nil
	}
	r.Metrics.ObserveMessage(false, msg.MessageType(), msg.EstimateSize())
	if end || messageutil.IsTimeTickConfirmBarrier(msg.MessageType()) {
		ordered := r.ReorderBuffer.PopUtilTimeTick(msg.TimeTick())
		if end {
			ordered = append(ordered, msg)
		}
		output := r.TxnBuffer.HandleImmutableMessages(ordered, msg.TimeTick())
		if !end && (msg.MessageType() != message.MessageTypeTimeTick || msg.IsPersisted() || len(output) == 0) {
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
	_, err := r.ReorderBuffer.Push(msg)
	if errors.Is(err, ErrTimeTickVoilation) {
		return errors.WithSecondaryError(status.NewUnrecoverableError("historical WAL violates confirmed message ordering"), err)
	}
	// Other Push outcomes are physical message-ID/TimeTick duplicates, handled
	// by the existing buffer. Only this reader's metrics contribution is updated.
	r.Metrics.UpdateTimeTickBufSize(r.ReorderBuffer.Bytes())
	return nil
}
