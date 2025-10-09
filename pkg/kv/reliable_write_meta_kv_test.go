package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/mocks/mock_kv"
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
