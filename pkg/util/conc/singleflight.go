package conc

import "golang.org/x/sync/singleflight"

// Singleflight wraps golang.org/x/sync/singleflight.Group into generic one.
type Singleflight[T any] struct {
	internal singleflight.Group
}

// SingleflightResult is a generic Result wrapper for DoChan.
type SingleflightResult[T any] struct {
	Val    T
	Err    error
	Shared bool
}

func (sf *Singleflight[T]) Do(key string, fn func() (T, error)) (T, error, bool) {
	raw, err, shared := sf.internal.Do(key, func() (any, error) {
		return fn()
	})
	var t T
	if raw != nil {
		t = raw.(T)
	}
	return t, err, shared
}

func (sf *Singleflight[T]) DoChan(key string, fn func() (T, error)) <-chan SingleflightResult[T] {
	ch := make(chan SingleflightResult[T], 1)
	go func() {
		val, err, shared := sf.Do(key, fn)
		ch <- SingleflightResult[T]{
			Val:    val,
			Err:    err,
			Shared: shared,
		}
	}()
	return ch
}

func (sf *Singleflight[T]) Forget(key string) {
	sf.internal.Forget(key)
}
