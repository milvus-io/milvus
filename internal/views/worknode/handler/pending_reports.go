package handler

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// pendingReports tracks the latest pending report state for each query view key.
// OnReport callbacks update the latest state; the send loop drains and sends them.
// Thread-safe.
type pendingReports struct {
	mu      sync.Mutex
	reports map[qviews.QueryViewKey]qviews.QueryViewAtWorkNode
	closing bool          // true after SetCloseResponse is called
	stopped bool          // true after Close is called
	notify  chan struct{} // cap 1, signaled when new reports are available; closed on Close()
}

func newPendingReports() *pendingReports {
	return &pendingReports{
		reports: make(map[qviews.QueryViewKey]qviews.QueryViewAtWorkNode),
		notify:  make(chan struct{}, 1),
	}
}

// Update stores the latest report for a view key, overwriting any previous
// pending state, and signals the send loop.
func (p *pendingReports) Update(report qviews.QueryViewAtWorkNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.reports[report.QueryViewKey()] = report
	p.signalLocked()
}

// SetCloseResponse marks that a close response should be sent during the next drain.
func (p *pendingReports) SetCloseResponse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.closing = true
	p.signalLocked()
}

// Ready returns a channel that is signaled when new reports or a close response are available.
func (p *pendingReports) Ready() <-chan struct{} {
	return p.notify
}

// Drain atomically collects and removes all pending reports.
// Returns the report protos and whether a close response should be sent.
func (p *pendingReports) Drain() (protos []*viewpb.QueryViewOfShard, closing bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.reports) > 0 {
		protos = make([]*viewpb.QueryViewOfShard, 0, len(p.reports))
		for _, r := range p.reports {
			protos = append(protos, r.IntoProto())
		}
		p.reports = make(map[qviews.QueryViewKey]qviews.QueryViewAtWorkNode)
	}
	closing = p.closing
	return protos, closing
}

// Close closes the notify channel, causing the send loop to exit.
func (p *pendingReports) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped {
		p.stopped = true
		close(p.notify)
	}
}

// signalLocked wakes the send loop without blocking. Caller must hold p.mu.
func (p *pendingReports) signalLocked() {
	if p.stopped {
		return
	}
	// Non-blocking notify: if already signaled, send loop will drain all.
	select {
	case p.notify <- struct{}{}:
	default:
	}
}
