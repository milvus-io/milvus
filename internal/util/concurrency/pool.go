package concurrency

import "github.com/panjf2000/ants/v2"

type Pool struct {
	inner *ants.Pool
}

func NewPool(cap int, opts ...ants.Option) (*Pool, error) {
	pool, err := ants.NewPool(cap, opts...)
	if err != nil {
		return nil, err
	}

	return &Pool{
		inner: pool,
	}, nil
}

func (pool *Pool) Submit(method func() (interface{}, error)) *Future {
	future := newFuture()
	err := pool.inner.Submit(func() {
		defer close(future.ch)
		res, err := method()
		if err != nil {
			future.err = err
		} else {
			future.value = res
		}
	})
	if err != nil {
		future.err = err
		close(future.ch)
	}

	return future
}

func (pool *Pool) Cap() int {
	return pool.inner.Cap()
}

func (pool *Pool) Running() int {
	return pool.inner.Running()
}
