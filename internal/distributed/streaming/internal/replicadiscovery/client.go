package replicadiscovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Client shares one logical watcher per PChannel across all its subscriptions.
// After idle retirement, streams retain only parked control loops.
type Client struct {
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	watchers  map[string]*resumableStream
	factory   func(context.Context, string) (Stream, error)
}

func NewResumingClient(factory func(context.Context, string) (Stream, error)) *Client {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Client.Close owns cancellation.
	return &Client{ctx: ctx, cancel: cancel, watchers: make(map[string]*resumableStream), factory: factory}
}

func (m *Client) Subscribe(vchannel string, handler Handler) (replicadiscovery.Subscription, error) {
	if vchannel == "" || handler == nil {
		return nil, status.NewInvalidRequestSeq("replica subscription requires VChannel and handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ctx.Err(); err != nil {
		return nil, err
	}
	pc := funcutil.ToPhysicalChannel(vchannel)
	w := m.watchers[pc]
	if w == nil {
		w = newResumableStream(m.ctx, pc, func(ctx context.Context) (Stream, error) { return m.factory(ctx, pc) })
		m.watchers[pc] = w
	}
	return w.subscribe(vchannel, handler)
}

func (m *Client) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.cancel()
		watchers := make([]*resumableStream, 0, len(m.watchers))
		for _, w := range m.watchers {
			watchers = append(watchers, w)
		}
		m.mu.Unlock()
		for _, w := range watchers {
			w.Close()
		}
		m.mu.Lock()
		m.watchers = nil
		m.mu.Unlock()
	})
}

type (
	Snapshot = replicadiscovery.Snapshot
	Replica  = replicadiscovery.Replica
	Handler  = replicadiscovery.Handler
	Stream   = replicadiscovery.Stream
)
