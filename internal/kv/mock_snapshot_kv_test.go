package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_mockSnapshotKV_Save(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.Save(context.TODO(), "k", "v", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.SaveFunc = func(ctx context.Context, key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.Save(context.TODO(), "k", "v", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_Load(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		_, err := snapshot.Load(context.TODO(), "k", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.LoadFunc = func(ctx context.Context, key string, ts typeutil.Timestamp) (string, error) {
			return "", nil
		}
		_, err := snapshot.Load(context.TODO(), "k", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSave(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSave(context.TODO(), nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSave(context.TODO(), nil, 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_LoadWithPrefix(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		_, _, err := snapshot.LoadWithPrefix(context.TODO(), "prefix", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		_, _, err := snapshot.LoadWithPrefix(context.TODO(), "prefix", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSaveAndRemoveWithPrefix(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSaveAndRemoveWithPrefix(context.TODO(), nil, nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSaveAndRemoveWithPrefix(context.TODO(), nil, nil, 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSaveAndRemove(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSaveAndRemove(context.TODO(), nil, nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSaveAndRemove(context.TODO(), nil, nil, 0)
		assert.NoError(t, err)
	})
}
