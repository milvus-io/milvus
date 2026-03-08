package kv

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

var _ MetaKv = (*ReliableWriteMetaKv)(nil)

// NewReliableWriteMetaKv returns a new ReliableWriteMetaKv if the kv is not a ReliableWriteMetaKv.
func NewReliableWriteMetaKv(kv MetaKv) MetaKv {
	if _, ok := kv.(*ReliableWriteMetaKv); ok {
		return kv
	}
	return &ReliableWriteMetaKv{
		Binder: log.Binder{},
		MetaKv: kv,
	}
}

// ReliableWriteMetaKv is a wrapper of MetaKv that ensures the data is written reliably.
// It will retry the metawrite operation until the data is written successfully or the context is timeout.
// It's useful to promise the meta data is consistent in memory and underlying meta storage.
type ReliableWriteMetaKv struct {
	log.Binder
	MetaKv
}

func (kv *ReliableWriteMetaKv) Save(ctx context.Context, key, value string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.Save(ctx, key, value)
	})
}

func (kv *ReliableWriteMetaKv) MultiSave(ctx context.Context, kvs map[string]string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSave(ctx, kvs)
	})
}

func (kv *ReliableWriteMetaKv) Remove(ctx context.Context, key string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.Remove(ctx, key)
	})
}

func (kv *ReliableWriteMetaKv) MultiRemove(ctx context.Context, keys []string) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiRemove(ctx, keys)
	})
}

func (kv *ReliableWriteMetaKv) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
	})
}

func (kv *ReliableWriteMetaKv) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	return kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		return kv.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
	})
}

func (kv *ReliableWriteMetaKv) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	var result bool
	err := kv.retryWithBackoff(ctx, func(ctx context.Context) error {
		var err error
		result, err = kv.MetaKv.CompareVersionAndSwap(ctx, key, version, target)
		return err
	})
	return result, err
}

// retryWithBackoff retries the function with backoff.
func (kv *ReliableWriteMetaKv) retryWithBackoff(ctx context.Context, fn func(ctx context.Context) error) error {
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		nextInterval := backoff.NextBackOff()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextInterval):
			kv.Logger().Warn("failed to persist operation, wait for retry...", zap.Duration("nextRetryInterval", nextInterval), zap.Error(err))
		}
	}
}
