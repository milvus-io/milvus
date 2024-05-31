package kv

import (
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type mockSnapshotKV struct {
	SnapShotKV
	SaveFunc                         func(key string, value string, ts typeutil.Timestamp) error
	LoadFunc                         func(key string, ts typeutil.Timestamp) (string, error)
	MultiSaveFunc                    func(kvs map[string]string, ts typeutil.Timestamp) error
	LoadWithPrefixFunc               func(key string, ts typeutil.Timestamp) ([]string, []string, error)
	MultiSaveAndRemoveWithPrefixFunc func(saves map[string]string, removals []string, ts typeutil.Timestamp) error
	MultiSaveAndRemoveFunc           func(saves map[string]string, removals []string, ts typeutil.Timestamp) error
}

func NewMockSnapshotKV() *mockSnapshotKV {
	return &mockSnapshotKV{}
}

func (m mockSnapshotKV) Save(key string, value string, ts typeutil.Timestamp) error {
	if m.SaveFunc != nil {
		return m.SaveFunc(key, value, ts)
	}
	return nil
}

func (m mockSnapshotKV) Load(key string, ts typeutil.Timestamp) (string, error) {
	if m.LoadFunc != nil {
		return m.LoadFunc(key, ts)
	}
	return "", nil
}

func (m mockSnapshotKV) MultiSave(kvs map[string]string, ts typeutil.Timestamp) error {
	if m.MultiSaveFunc != nil {
		return m.MultiSaveFunc(kvs, ts)
	}
	return nil
}

func (m mockSnapshotKV) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	if m.LoadWithPrefixFunc != nil {
		return m.LoadWithPrefixFunc(key, ts)
	}
	return nil, nil, nil
}

func (m mockSnapshotKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	if m.MultiSaveAndRemoveWithPrefixFunc != nil {
		return m.MultiSaveAndRemoveWithPrefixFunc(saves, removals, ts)
	}
	return nil
}

func (m mockSnapshotKV) MultiSaveAndRemove(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	if m.MultiSaveAndRemoveFunc != nil {
		return m.MultiSaveAndRemoveFunc(saves, removals, ts)
	}
	return nil
}
