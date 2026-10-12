package kv

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	tikverr "github.com/tikv/client-go/v2/error"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

var _ MetaKv = (*ReliableWriteMetaKv)(nil)

// NewReliableWriteMetaKv returns a new ReliableWriteMetaKv if the kv is not a ReliableWriteMetaKv.
func NewReliableWriteMetaKv(kv MetaKv) MetaKv {
	if _, ok := kv.(*ReliableWriteMetaKv); ok {
		return kv
	}
	return &ReliableWriteMetaKv{
		Binder: mlog.Binder{},
		MetaKv: kv,
	}
}

// ReliableWriteMetaKv is a wrapper of MetaKv that ensures the data is written reliably.
// It will retry the metawrite operation until the data is written successfully or the context is timeout.
// It's useful to promise the meta data is consistent in memory and underlying meta storage.
type ReliableWriteMetaKv struct {
	mlog.Binder
	MetaKv
}

func (kv *ReliableWriteMetaKv) Save(ctx context.Context, key, value string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.Save(ctx, key, value)
	}, true)
}

func (kv *ReliableWriteMetaKv) MultiSave(ctx context.Context, kvs map[string]string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSave(ctx, kvs)
	}, true)
}

func (kv *ReliableWriteMetaKv) Remove(ctx context.Context, key string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.Remove(ctx, key)
	}, true)
}

func (kv *ReliableWriteMetaKv) MultiRemove(ctx context.Context, keys []string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiRemove(ctx, keys)
	}, true)
}

// A guarded write is NOT retried. Its predicate is a snapshot of a value the
// attempt itself may already have changed: an etcd leader change or a timeout
// can apply the transaction and still report an error to the client, and
// re-sending the same guard then compares against a value that can never hold
// again -- the write loops until its context expires while the state it wanted
// is already in place. An unmet predicate is likewise not transient; retrying it
// only spins. Both cases belong to the caller, which holds the value it read and
// can read it again to decide. Unconditional writes keep the retry: re-running
// the identical key->value operation converges either way.
func (kv *ReliableWriteMetaKv) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if len(preds) > 0 {
		return kv.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
	}
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSaveAndRemove(ctx, saves, removals)
	}, true)
}

func (kv *ReliableWriteMetaKv) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if len(preds) > 0 {
		return kv.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
	}
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals)
	}, true)
}

func (kv *ReliableWriteMetaKv) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	var result bool
	err := kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		var err error
		result, err = kv.MetaKv.CompareVersionAndSwap(ctx, key, version, target)
		return err
	}, false)
	return result, err
}

// retryWithBackoff retries the function with backoff.
//
// A TiKV "undetermined" write result means the 2PC commit outcome is unknown:
// the operation may or may not have been applied. For an unconditional
// (predicate-free) write this is harmless — re-running the identical
// key→value operation converges to the same final state whether or not the
// first attempt committed, so it is retried like any other transient error.
// For a conditional write (predicates or CAS) the outcome ambiguity cannot be
// resolved by re-running it — the first attempt may already have consumed the
// condition being guarded — so undetermined results are surfaced to the caller
// immediately. Callers pass retryUndetermined accordingly.
func (kv *ReliableWriteMetaKv) retryWithBackoff(ctx context.Context, fn func(ctx context.Context) error, retryUndetermined bool) error {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if tikverr.IsErrorUndetermined(err) && !retryUndetermined {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		nextInterval := backoff.NextBackOff()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextInterval):
			kv.Logger().Warn(ctx, "failed to persist operation, wait for retry...", mlog.Duration("nextRetryInterval", nextInterval), mlog.Err(err))
		}
	}
}
