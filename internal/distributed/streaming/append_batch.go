package streaming

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type appendBatchConfig struct {
	SmallMessageThreshold int
	MaxBatchSize          int
	MaxDelay              time.Duration
	CooldownThreshold     int
	CooldownDuration      time.Duration
}

func newAppendBatchConfigFromParams() appendBatchConfig {
	params := paramtable.Get().StreamingCfg
	return appendBatchConfig{
		SmallMessageThreshold: int(params.WALAppendBatchSmallMessageThreshold.GetAsSize()),
		MaxBatchSize:          int(params.WALAppendBatchMaxSize.GetAsSize()),
		MaxDelay:              params.WALAppendBatchMaxDelay.GetAsDurationByParse(),
		CooldownThreshold:     params.WALAppendBatchCooldownThreshold.GetAsInt(),
		CooldownDuration:      params.WALAppendBatchCooldownDuration.GetAsDurationByParse(),
	}
}

func (c appendBatchConfig) enabled() bool {
	return c.SmallMessageThreshold > 0 && c.MaxBatchSize > 0 && c.MaxDelay > 0
}

type appendBatchRequest struct {
	ctx    context.Context
	msgs   []message.MutableMessage
	size   int
	respCh chan types.AppendResponse
}

type appendBatcher struct {
	mu       sync.Mutex
	vchannel string
	cfg      appendBatchConfig
	appendFn func(context.Context, ...message.MutableMessage) types.AppendResponse

	pending   []*appendBatchRequest
	totalSize int
	timer     *time.Timer
	closed    bool

	consecutiveSingleFlushes int
	cooldownUntil            time.Time
}

func newAppendBatcher(
	vchannel string,
	cfg appendBatchConfig,
	appendFn func(context.Context, ...message.MutableMessage) types.AppendResponse,
) *appendBatcher {
	return &appendBatcher{
		vchannel: vchannel,
		cfg:      cfg,
		appendFn: appendFn,
	}
}

func (b *appendBatcher) submit(ctx context.Context, msgs ...message.MutableMessage) <-chan types.AppendResponse {
	respCh := make(chan types.AppendResponse, 1)
	req := &appendBatchRequest{
		ctx:    ctx,
		msgs:   msgs,
		size:   messagesEstimateSize(msgs...),
		respCh: respCh,
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		respCh <- types.AppendResponse{Error: ErrWALAccesserClosed}
		return respCh
	}
	if err := ctx.Err(); err != nil {
		respCh <- types.AppendResponse{Error: err}
		return respCh
	}

	b.pending = append(b.pending, req)
	b.totalSize += req.size
	if b.totalSize >= b.cfg.MaxBatchSize {
		reqs := b.popPendingLocked()
		go b.flush(reqs, false)
		return respCh
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(b.cfg.MaxDelay, b.flushByTimer)
	}
	return respCh
}

func (b *appendBatcher) inCooldown(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return now.Before(b.cooldownUntil)
}

func (b *appendBatcher) close() {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	reqs := b.popPendingLocked()
	b.closed = true
	b.mu.Unlock()

	for _, req := range reqs {
		req.respCh <- types.AppendResponse{Error: ErrWALAccesserClosed}
	}
}

func (b *appendBatcher) flushByTimer() {
	b.mu.Lock()
	reqs := b.popPendingLocked()
	b.mu.Unlock()
	b.flush(reqs, true)
}

func (b *appendBatcher) popPendingLocked() []*appendBatchRequest {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	reqs := b.pending
	b.pending = nil
	b.totalSize = 0
	return reqs
}

func (b *appendBatcher) flush(reqs []*appendBatchRequest, fromTimer bool) {
	if len(reqs) == 0 {
		return
	}

	active := make([]*appendBatchRequest, 0, len(reqs))
	msgs := make([]message.MutableMessage, 0, len(reqs))
	size := 0
	for _, req := range reqs {
		if err := req.ctx.Err(); err != nil {
			req.respCh <- types.AppendResponse{Error: err}
			continue
		}
		active = append(active, req)
		msgs = append(msgs, req.msgs...)
		size += req.size
	}
	if len(active) == 0 {
		return
	}

	b.observeFlush(fromTimer, len(active), len(msgs))
	ctx, cancel := batchContext(active)
	trigger := "size"
	if fromTimer {
		trigger = "timer"
	}
	log.Ctx(ctx).Debug("wal append batch flush",
		zap.String("vchannel", b.vchannel),
		zap.String("trigger", trigger),
		zap.Int("requestCount", len(active)),
		zap.Int("messageCount", len(msgs)),
		zap.Int("estimatedSize", size))
	resp := b.appendFn(ctx, msgs...)
	cancel()
	for _, req := range active {
		req.respCh <- resp
	}
}

func (b *appendBatcher) observeFlush(fromTimer bool, reqCount int, msgCount int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !fromTimer || reqCount > 1 || msgCount > 1 {
		b.consecutiveSingleFlushes = 0
		return
	}

	b.consecutiveSingleFlushes++
	if b.cfg.CooldownThreshold > 0 && b.consecutiveSingleFlushes >= b.cfg.CooldownThreshold {
		b.cooldownUntil = time.Now().Add(b.cfg.CooldownDuration)
		b.consecutiveSingleFlushes = 0
	}
}

func batchContext(reqs []*appendBatchRequest) (context.Context, context.CancelFunc) {
	var earliest time.Time
	hasDeadline := false
	for _, req := range reqs {
		deadline, ok := req.ctx.Deadline()
		if !ok {
			continue
		}
		if !hasDeadline || deadline.Before(earliest) {
			earliest = deadline
			hasDeadline = true
		}
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if !hasDeadline {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), earliest)
	}
	for _, req := range reqs {
		reqCtx := req.ctx
		go func() {
			select {
			case <-reqCtx.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

func messagesEstimateSize(msgs ...message.MutableMessage) int {
	var size int
	for _, msg := range msgs {
		size += msg.EstimateSize()
	}
	return size
}
