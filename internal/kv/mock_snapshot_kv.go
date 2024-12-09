package kv

import (
	"context"

	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ kv.SnapShotKV = &mockSnapshotKV{}

type mockSnapshotKV struct {
	kv.SnapShotKV
	SaveFunc                         func(ctx context.Context, key string, value string, ts typeutil.Timestamp) error
	LoadFunc                         func(ctx context.Context, key string, ts typeutil.Timestamp) (string, error)
	MultiSaveFunc                    func(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error
	LoadWithPrefixFunc               func(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error)
	MultiSaveAndRemoveWithPrefixFunc func(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error
	MultiSaveAndRemoveFunc           func(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error
}

func NewMockSnapshotKV() *mockSnapshotKV {
	return &mockSnapshotKV{}
}

func (m mockSnapshotKV) Save(ctx context.Context, key string, value string, ts typeutil.Timestamp) error {
	if m.SaveFunc != nil {
		return m.SaveFunc(ctx, key, value, ts)
	}
	return nil
}

func (m mockSnapshotKV) Load(ctx context.Context, key string, ts typeutil.Timestamp) (string, error) {
	if m.LoadFunc != nil {
		return m.LoadFunc(ctx, key, ts)
	}
	return "", nil
}

func (m mockSnapshotKV) MultiSave(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error {
	if m.MultiSaveFunc != nil {
		return m.MultiSaveFunc(ctx, kvs, ts)
	}
	return nil
}

func (m mockSnapshotKV) LoadWithPrefix(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error) {
	if m.LoadWithPrefixFunc != nil {
		return m.LoadWithPrefixFunc(ctx, key, ts)
	}
	return nil, nil, nil
}

func (m mockSnapshotKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	if m.MultiSaveAndRemoveWithPrefixFunc != nil {
		return m.MultiSaveAndRemoveWithPrefixFunc(ctx, saves, removals, ts)
	}
	return nil
}

func (m mockSnapshotKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	if m.MultiSaveAndRemoveFunc != nil {
		return m.MultiSaveAndRemoveFunc(ctx, saves, removals, ts)
	}
	return nil
}
