package conc

import "golang.org/x/sync/singleflight"

// Singleflight wraps golang.org/x/sync/singleflight.Group into generic one.
type Singleflight[T any] struct {
	internal singleflight.Group
}

// SingleflightResult is the asynchronous result returned by DoChan.
// Val contains a value of the Singleflight type parameter when it is non-nil.
type SingleflightResult = singleflight.Result

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

func (sf *Singleflight[T]) DoChan(key string, fn func() (T, error)) <-chan SingleflightResult {
	return sf.internal.DoChan(key, func() (any, error) {
		return fn()
	})
}

// UnwrapSingleflightResult converts a DoChan result back to its generic value.
func UnwrapSingleflightResult[T any](result SingleflightResult) (T, error, bool) {
	var value T
	if result.Val != nil {
		value = result.Val.(T)
	}
	return value, result.Err, result.Shared
}

func (sf *Singleflight[T]) Forget(key string) {
	sf.internal.Forget(key)
}
