// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CompareFailedError is a helper type for checking MetaKv CompareAndSwap series func error type
type CompareFailedError struct {
	internalError error
}

// Error implements error interface
func (e *CompareFailedError) Error() string {
	return e.internalError.Error()
}

// NewCompareFailedError wraps error into NewCompareFailedError
func NewCompareFailedError(err error) error {
	return &CompareFailedError{internalError: err}
}

// BaseKV contains base operations of kv. Include save, load and remove.
type BaseKV interface {
	Load(ctx context.Context, key string) (string, error)
	MultiLoad(ctx context.Context, keys []string) ([]string, error)
	LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error)
	Save(ctx context.Context, key, value string) error
	MultiSave(ctx context.Context, kvs map[string]string) error
	Remove(ctx context.Context, key string) error
	MultiRemove(ctx context.Context, keys []string) error
	RemoveWithPrefix(ctx context.Context, key string) error
	Has(ctx context.Context, key string) (bool, error)
	HasPrefix(ctx context.Context, prefix string) (bool, error)
	Close()
}

// TxnKV contains extra txn operations of kv. The extra operations is transactional.
//
//go:generate mockery --name=TxnKV --with-expecter
type TxnKV interface {
	BaseKV
	MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error
	MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error
}

// MetaKv is TxnKV for metadata. It should save data with lease.
//
//go:generate mockery --name=MetaKv --with-expecter
type MetaKv interface {
	TxnKV
	GetPath(key string) string
	LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error)
	CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error)
	WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error
}

// WatchKV is watchable MetaKv. As of today(2023/06/24), it's coupled with etcd.
//
//go:generate mockery --name=WatchKV --with-expecter
type WatchKV interface {
	MetaKv
	Watch(ctx context.Context, key string) clientv3.WatchChan
	WatchWithPrefix(ctx context.Context, key string) clientv3.WatchChan
	WatchWithRevision(ctx context.Context, key string, revision int64) clientv3.WatchChan
}

// SnapShotKV is TxnKV for snapshot data. It must save timestamp.
//
//go:generate mockery --name=SnapShotKV --with-expecter
type SnapShotKV interface {
	Save(ctx context.Context, key string, value string, ts typeutil.Timestamp) error
	Load(ctx context.Context, key string, ts typeutil.Timestamp) (string, error)
	MultiSave(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error
	LoadWithPrefix(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error)
	MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error
	MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error
}
