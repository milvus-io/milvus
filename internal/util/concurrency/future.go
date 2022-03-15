package concurrency

type Future struct {
	ch    chan struct{}
	value interface{}
	err   error
}

func newFuture() *Future {
	return &Future{
		ch: make(chan struct{}),
	}
}

func (future *Future) Await() (interface{}, error) {
	<-future.ch

	return future.value, future.err
}

func (future *Future) Value() interface{} {
	<-future.ch

	return future.value
}

func (future *Future) OK() bool {
	<-future.ch

	return future.err == nil
}

func (future *Future) Err() error {
	<-future.ch

	return future.err
}

func (future *Future) Inner() <-chan struct{} {
	return future.ch
}

func AwaitAll(futures []*Future) error {
	for i := range futures {
		if !futures[i].OK() {
			return futures[i].err
		}
	}

	return nil
}
