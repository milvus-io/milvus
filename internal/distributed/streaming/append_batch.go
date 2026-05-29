package streaming

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type appendBatchConfig struct {
	SmallMessageThreshold int
	MaxBatchSize          int
	MaxMessageCount       int
	MaxDelay              time.Duration
	CooldownThreshold     int
	CooldownDuration      time.Duration
}

func newAppendBatchConfigFromParams() appendBatchConfig {
	params := paramtable.Get()
	return appendBatchConfig{
		SmallMessageThreshold: int(params.StreamingCfg.WALAppendBatchSmallMessageThreshold.GetAsSize()),
		MaxBatchSize:          int(params.StreamingCfg.WALAppendBatchMaxSize.GetAsSize()),
		MaxMessageCount:       params.StreamingCfg.WALAppendBatchMaxMessageCount.GetAsInt(),
		MaxDelay:              params.StreamingCfg.WALAppendBatchMaxDelay.GetAsDurationByParse(),
		CooldownThreshold:     params.StreamingCfg.WALAppendBatchCooldownThreshold.GetAsInt(),
		CooldownDuration:      params.StreamingCfg.WALAppendBatchCooldownDuration.GetAsDurationByParse(),
	}
}

func (c appendBatchConfig) enabled() bool {
	return c.SmallMessageThreshold > 0 && c.MaxBatchSize > 0 && c.MaxMessageCount > 0 && c.MaxDelay > 0
}

type appendBatchConfigProvider struct {
	load       func() appendBatchConfig
	unregister func()
	current    atomic.Pointer[appendBatchConfig]
}

func newAppendBatchConfigProvider(
	load func() appendBatchConfig,
	register func(paramtable.ParamChangeCallback) func(),
) *appendBatchConfigProvider {
	p := &appendBatchConfigProvider{load: load}
	p.refresh()
	if register != nil {
		p.unregister = register(p.update)
	}
	return p
}

func newAppendBatchConfigProviderFromParams() *appendBatchConfigProvider {
	return newAppendBatchConfigProvider(newAppendBatchConfigFromParams, registerAppendBatchConfigCallbacks)
}

func registerAppendBatchConfigCallbacks(callback paramtable.ParamChangeCallback) func() {
	params := paramtable.Get()
	handler := config.NewHandler("wal-append-batch-config-provider", func(event *config.Event) {
		_ = callback(context.Background(), event.Key, "", event.Value)
	})
	params.WatchKeyPrefix("streaming.walAppendBatch", handler)
	return func() {
		params.Unwatch("streaming.walAppendBatch", handler)
	}
}

func (p *appendBatchConfigProvider) get() appendBatchConfig {
	if cfg := p.current.Load(); cfg != nil {
		return *cfg
	}
	return appendBatchConfig{}
}

func (p *appendBatchConfigProvider) update(ctx context.Context, key, oldValue, newValue string) error {
	oldConfig := p.get()
	newConfig := p.load()
	p.current.Store(&newConfig)
	if oldConfig != newConfig {
		log.Info("wal append batch config updated",
			zap.Any("oldConfig", oldConfig),
			zap.Any("newConfig", newConfig))
	}
	return nil
}

func (p *appendBatchConfigProvider) refresh() {
	cfg := p.load()
	p.current.Store(&cfg)
}

func (p *appendBatchConfigProvider) close() {
	if p.unregister != nil {
		p.unregister()
		p.unregister = nil
	}
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
	cfg      func() appendBatchConfig
	appendFn func(context.Context, ...message.MutableMessage) types.AppendResponse

	pending       []*appendBatchRequest
	totalSize     int
	totalMessages int
	timer         *time.Timer
	timerDelay    time.Duration
	closed        bool

	consecutiveSingleFlushes int
	cooldownUntil            time.Time
}

func newAppendBatcher(
	vchannel string,
	cfg func() appendBatchConfig,
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

	cfg := b.cfg()
	b.pending = append(b.pending, req)
	b.totalSize += req.size
	b.totalMessages += len(req.msgs)
	if b.totalSize >= cfg.MaxBatchSize || b.totalMessages >= cfg.MaxMessageCount {
		reqs := b.popPendingLocked()
		go b.flush(reqs)
		return respCh
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(cfg.MaxDelay, b.flushByTimer)
		b.timerDelay = cfg.MaxDelay
	} else if b.timerDelay != cfg.MaxDelay {
		b.timer.Stop()
		b.timer = time.AfterFunc(cfg.MaxDelay, b.flushByTimer)
		b.timerDelay = cfg.MaxDelay
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
		b.timerDelay = 0
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
	b.flush(reqs)
}

func (b *appendBatcher) popPendingLocked() []*appendBatchRequest {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
		b.timerDelay = 0
	}
	reqs := b.pending
	b.pending = nil
	b.totalSize = 0
	b.totalMessages = 0
	return reqs
}

func (b *appendBatcher) flush(reqs []*appendBatchRequest) {
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

	b.observeFlush(len(active))
	ctx, cancel := batchContext(active)
	log.Ctx(ctx).Debug("wal append batch flush",
		zap.String("vchannel", b.vchannel),
		zap.Int("requestCount", len(active)),
		zap.Int("messageCount", len(msgs)),
		zap.Int("estimatedSize", size))
	resp := b.appendFn(ctx, msgs...)
	cancel()
	for _, req := range active {
		req.respCh <- resp
	}
}

func (b *appendBatcher) observeFlush(reqCount int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if reqCount > 1 {
		b.consecutiveSingleFlushes = 0
		return
	}

	b.consecutiveSingleFlushes++
	cfg := b.cfg()
	if cfg.CooldownThreshold > 0 && b.consecutiveSingleFlushes >= cfg.CooldownThreshold {
		b.cooldownUntil = time.Now().Add(cfg.CooldownDuration)
		b.consecutiveSingleFlushes = 0
	}
}

func batchContext(reqs []*appendBatchRequest) (context.Context, context.CancelFunc) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	active := len(reqs)
	mu := sync.Mutex{}
	stops := make([]func() bool, 0, len(reqs))
	for _, req := range reqs {
		stops = append(stops, context.AfterFunc(req.ctx, func() {
			mu.Lock()
			defer mu.Unlock()
			active--
			if active == 0 {
				cancelCtx()
			}
		}))
	}
	return ctx, func() {
		for _, stop := range stops {
			stop()
		}
		cancelCtx()
	}
}

func messagesEstimateSize(msgs ...message.MutableMessage) int {
	var size int
	for _, msg := range msgs {
		size += msg.EstimateSize()
	}
	return size
}
