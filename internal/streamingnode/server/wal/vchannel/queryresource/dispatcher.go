package queryresource

import "sync"

const defaultLiveEventDispatchConcurrency = 4

type Dispatcher struct {
	tasks  chan *QueryRuntime
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func NewDispatcher(concurrency int) *Dispatcher {
	if concurrency <= 0 {
		concurrency = 1
	}
	dispatcher := &Dispatcher{
		tasks:  make(chan *QueryRuntime, 1024),
		closed: make(chan struct{}),
	}
	for i := 0; i < concurrency; i++ {
		dispatcher.wg.Add(1)
		go func() {
			defer dispatcher.wg.Done()
			dispatcher.worker()
		}()
	}
	return dispatcher
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
		close(d.closed)
		d.wg.Wait()
	})
}

func (d *Dispatcher) worker() {
	for {
		select {
		case runtime := <-d.tasks:
			runtime.drainReady()
		case <-d.closed:
			return
		}
	}
}
