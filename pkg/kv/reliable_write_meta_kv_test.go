package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mocks/mock_kv"
)

func TestReliableWriteMetaKv(t *testing.T) {
	kv := mock_kv.NewMockMetaKv(t)
	fail := atomic.NewBool(true)
	kv.EXPECT().Save(context.TODO(), mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s1, s2 string) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().MultiSave(context.TODO(), mock.Anything).RunAndReturn(func(ctx context.Context, kvs map[string]string) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().Remove(context.TODO(), mock.Anything).RunAndReturn(func(ctx context.Context, key string) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().MultiRemove(context.TODO(), mock.Anything).RunAndReturn(func(ctx context.Context, keys []string) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().MultiSaveAndRemove(context.TODO(), mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().MultiSaveAndRemoveWithPrefix(context.TODO(), mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
		if !fail.Load() {
			return nil
		}
		return errors.New("test")
	})
	kv.EXPECT().CompareVersionAndSwap(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key string, version int64, target string) (bool, error) {
		if !fail.Load() {
			return false, nil
		}
		return false, errors.New("test")
	})
	rkv := NewReliableWriteMetaKv(kv)
	wg := sync.WaitGroup{}
	wg.Add(7)
	success := atomic.NewInt32(0)
	go func() {
		defer wg.Done()
		err := rkv.Save(context.TODO(), "test", "test")
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		err := rkv.MultiSave(context.TODO(), map[string]string{"test": "test"})
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		err := rkv.Remove(context.TODO(), "test")
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		err := rkv.MultiRemove(context.TODO(), []string{"test"})
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		err := rkv.MultiSaveAndRemove(context.TODO(), map[string]string{"test": "test"}, []string{"test"})
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		err := rkv.MultiSaveAndRemoveWithPrefix(context.TODO(), map[string]string{"test": "test"}, []string{"test"})
		if err == nil {
			success.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		_, err := rkv.CompareVersionAndSwap(context.TODO(), "test", 0, "test")
		if err == nil {
			success.Add(1)
		}
	}()
	time.Sleep(1 * time.Second)
	fail.Store(false)
	wg.Wait()
	assert.Equal(t, int32(7), success.Load())

	fail.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := rkv.CompareVersionAndSwap(ctx, "test", 0, "test")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestReliableWriteMetaKvRetriesUndeterminedForPureSet verifies that an
// undetermined write result from an unconditional (predicate-free) Set
// operation is retried: re-running the identical key→value operation converges
// whether or not the first attempt committed.
func TestReliableWriteMetaKvRetriesUndeterminedForPureSet(t *testing.T) {
	metaKV := mock_kv.NewMockMetaKv(t)
	calls := atomic.NewInt32(0)
	metaKV.EXPECT().Save(mock.Anything, "k", "v").RunAndReturn(
		func(ctx context.Context, key, value string) error {
			if calls.Inc() == 1 {
				return errors.Wrap(tikverr.ErrResultUndetermined, "commit failed")
			}
			return nil
		}).Maybe()

	rkv := NewReliableWriteMetaKv(metaKV)
	err := rkv.Save(context.Background(), "k", "v")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

// TestReliableWriteMetaKvRetriesUndeterminedForMultiSaveAndRemoveWithoutPreds
// covers the qviews flush persist path: MultiSaveAndRemove without predicates
// is a deterministic Set operation, so undetermined results are retried.
func TestReliableWriteMetaKvRetriesUndeterminedForMultiSaveAndRemoveWithoutPreds(t *testing.T) {
	metaKV := mock_kv.NewMockMetaKv(t)
	calls := atomic.NewInt32(0)
	metaKV.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
			if calls.Inc() == 1 {
				return errors.Wrap(tikverr.ErrResultUndetermined, "commit failed")
			}
			return nil
		}).Maybe()

	rkv := NewReliableWriteMetaKv(metaKV)
	err := rkv.MultiSaveAndRemove(context.Background(), map[string]string{"k": "v"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

// TestReliableWriteMetaKvDoesNotRetryUndeterminedWriteResult verifies that a
// conditional operation (CAS) surfaces an undetermined write result instead of
// retrying: the first attempt may already have consumed the guarded condition,
// so the outcome ambiguity cannot be resolved by re-running it.
func TestReliableWriteMetaKvDoesNotRetryUndeterminedWriteResult(t *testing.T) {
	metaKV := mock_kv.NewMockMetaKv(t)
	calls := atomic.NewInt32(0)
	metaKV.EXPECT().CompareVersionAndSwap(mock.Anything, "k", int64(1), "v").RunAndReturn(
		func(ctx context.Context, key string, version int64, target string) (bool, error) {
			if calls.Inc() == 1 {
				return false, errors.Wrap(tikverr.ErrResultUndetermined, "commit failed")
			}
			return true, nil
		}).Maybe()

	rkv := NewReliableWriteMetaKv(metaKV)
	swapped, err := rkv.CompareVersionAndSwap(context.Background(), "k", 1, "v")
	assert.ErrorIs(t, err, tikverr.ErrResultUndetermined)
	assert.False(t, swapped)
	assert.Equal(t, int32(1), calls.Load())
}
