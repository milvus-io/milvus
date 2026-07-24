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

// SingleflightRawResult is the asynchronous result returned by DoChanRaw.
// Val contains a value of the Singleflight type parameter when it is non-nil.
type SingleflightRawResult = singleflight.Result

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

// DoChanRaw provides the native singleflight asynchronous path without an
// adapter goroutine for each caller.
func (sf *Singleflight[T]) DoChanRaw(key string, fn func() (T, error)) <-chan SingleflightRawResult {
	return sf.internal.DoChan(key, func() (any, error) {
		return fn()
	})
}

// UnwrapSingleflightRawResult converts a DoChanRaw result back to its generic value.
func UnwrapSingleflightRawResult[T any](result SingleflightRawResult) (T, error, bool) {
	var value T
	if result.Val != nil {
		value = result.Val.(T)
	}
	return value, result.Err, result.Shared
}

func (sf *Singleflight[T]) Forget(key string) {
	sf.internal.Forget(key)
}
