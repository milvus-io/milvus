package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func Test_mockSnapshotKV_Save(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.Save("k", "v", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.Save("k", "v", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_Load(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		_, err := snapshot.Load("k", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", nil
		}
		_, err := snapshot.Load("k", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSave(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSave(nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSave(nil, 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_LoadWithPrefix(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		_, _, err := snapshot.LoadWithPrefix("prefix", 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		_, _, err := snapshot.LoadWithPrefix("prefix", 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSaveAndRemoveWithPrefix(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSaveAndRemoveWithPrefix(nil, nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSaveAndRemoveWithPrefix(nil, nil, 0)
		assert.NoError(t, err)
	})
}

func Test_mockSnapshotKV_MultiSaveAndRemove(t *testing.T) {
	t.Run("func not set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		err := snapshot.MultiSaveAndRemove(nil, nil, 0)
		assert.NoError(t, err)
	})
	t.Run("func set", func(t *testing.T) {
		snapshot := NewMockSnapshotKV()
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err := snapshot.MultiSaveAndRemove(nil, nil, 0)
		assert.NoError(t, err)
	})
}
