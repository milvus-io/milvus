package queryresource

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Dispatcher struct {
	tasks              chan *QueryRuntime
	closed             chan struct{}
	once               sync.Once
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	workers            int
	concurrency        int
	stopped            bool
	changed            chan struct{}
	concurrencyWatcher config.EventHandler
}

func NewDispatcher(concurrency int) *Dispatcher {
	if concurrency <= 0 {
		panic("query resource dispatcher concurrency must be positive")
	}
	dispatcher := &Dispatcher{
		tasks:   make(chan *QueryRuntime, 1024),
		closed:  make(chan struct{}),
		changed: make(chan struct{}),
	}
	dispatcher.mu.Lock()
	dispatcher.resizeLocked(concurrency)
	dispatcher.mu.Unlock()
	return dispatcher
}

// NewConfiguredDispatcher creates a PChannel-local pool following live config.
func NewConfiguredDispatcher() *Dispatcher {
	params := paramtable.Get()
	d := NewDispatcher(params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.GetAsInt())
	refresh := func(*config.Event) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.stopped {
			d.resizeLocked(params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.GetAsInt())
		}
	}
	d.concurrencyWatcher = config.NewHandler(fmt.Sprintf("query-live-dispatch-%p", d), refresh)
	params.Watch(params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.Key, d.concurrencyWatcher)
	refresh(nil)
	return d
}

func (d *Dispatcher) resizeLocked(concurrency int) {
	d.concurrency = concurrency
	for d.workers < concurrency {
		d.workers++
		d.wg.Add(1)
		go d.worker()
	}
	close(d.changed)
	d.changed = make(chan struct{})
}

func (d *Dispatcher) Submit(runtime *QueryRuntime) bool {
	if d == nil || runtime == nil {
		return false
	}
	select {
	case d.tasks <- runtime:
		return true
	case <-d.closed:
		return false
	}
}

func (d *Dispatcher) Close() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		if d.concurrencyWatcher != nil {
			params := paramtable.Get()
			params.Unwatch(params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.Key, d.concurrencyWatcher)
		}
		d.mu.Lock()
		d.stopped = true
		close(d.closed)
		d.mu.Unlock()
		d.wg.Wait()
	})
}

func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		if d.stopped || d.workers > d.concurrency {
			d.workers--
			d.mu.Unlock()
			return
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case runtime := <-d.tasks:
			runtime.drainReady()
		case <-d.closed:
		case <-changed:
		}
	}
}
