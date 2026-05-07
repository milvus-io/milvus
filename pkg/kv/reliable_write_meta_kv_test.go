package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
)

func TestReliableWriteMetaKv(t *testing.T) {
	t.Run("Save retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		saveMock := mockey.Mock((*testMetaKv).Save).To(
			func(*testMetaKv, context.Context, string, string) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer saveMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.Save(context.TODO(), "test", "test")
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("MultiSave retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		multiSaveMock := mockey.Mock((*testMetaKv).MultiSave).To(
			func(*testMetaKv, context.Context, map[string]string) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer multiSaveMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.MultiSave(context.TODO(), map[string]string{"test": "test"})
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("Remove retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		removeMock := mockey.Mock((*testMetaKv).Remove).To(
			func(*testMetaKv, context.Context, string) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer removeMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.Remove(context.TODO(), "test")
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("MultiRemove retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		multiRemoveMock := mockey.Mock((*testMetaKv).MultiRemove).To(
			func(*testMetaKv, context.Context, []string) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer multiRemoveMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.MultiRemove(context.TODO(), []string{"test"})
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("MultiSaveAndRemove retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		multiSaveAndRemoveMock := mockey.Mock((*testMetaKv).MultiSaveAndRemove).To(
			func(*testMetaKv, context.Context, map[string]string, []string, ...predicates.Predicate) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer multiSaveAndRemoveMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.MultiSaveAndRemove(
			context.TODO(),
			map[string]string{"test": "test"},
			[]string{"test"},
		)
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("MultiSaveAndRemoveWithPrefix retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		multiSaveAndRemoveWithPrefixMock := mockey.Mock((*testMetaKv).MultiSaveAndRemoveWithPrefix).To(
			func(*testMetaKv, context.Context, map[string]string, []string, ...predicates.Predicate) error {
				if calls.Inc() == 1 {
					return errors.New("test")
				}
				return nil
			},
		).Build()
		defer multiSaveAndRemoveWithPrefixMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		err := rkv.MultiSaveAndRemoveWithPrefix(
			context.TODO(),
			map[string]string{"test": "test"},
			[]string{"test"},
		)
		assert.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("CompareVersionAndSwap retries until success", func(t *testing.T) {
		calls := atomic.NewInt32(0)
		compareVersionAndSwapMock := mockey.Mock((*testMetaKv).CompareVersionAndSwap).To(
			func(*testMetaKv, context.Context, string, int64, string) (bool, error) {
				if calls.Inc() == 1 {
					return false, errors.New("test")
				}
				return true, nil
			},
		).Build()
		defer compareVersionAndSwapMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		swapped, err := rkv.CompareVersionAndSwap(context.TODO(), "test", 0, "test")
		assert.NoError(t, err)
		assert.True(t, swapped)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("CompareVersionAndSwap stops when context expires", func(t *testing.T) {
		compareVersionAndSwapMock := mockey.Mock((*testMetaKv).CompareVersionAndSwap).To(
			func(*testMetaKv, context.Context, string, int64, string) (bool, error) {
				return false, errors.New("test")
			},
		).Build()
		defer compareVersionAndSwapMock.UnPatch()

		rkv := NewReliableWriteMetaKv(&testMetaKv{})
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := rkv.CompareVersionAndSwap(ctx, "test", 0, "test")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

type testMetaKv struct {
	MetaKv
}
