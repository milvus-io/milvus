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
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	Load(key string) (string, error)
	MultiLoad(keys []string) ([]string, error)
	LoadWithPrefix(key string) ([]string, []string, error)
	Save(key, value string) error
	MultiSave(kvs map[string]string) error
	Remove(key string) error
	MultiRemove(keys []string) error
	RemoveWithPrefix(key string) error
	Has(key string) (bool, error)
	HasPrefix(prefix string) (bool, error)
	Close()
}

// TxnKV contains extra txn operations of kv. The extra operations is transactional.
//
//go:generate mockery --name=TxnKV --with-expecter
type TxnKV interface {
	BaseKV
	MultiSaveAndRemove(saves map[string]string, removals []string) error
	MultiRemoveWithPrefix(keys []string) error
	MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error
}

// MetaKv is TxnKV for metadata. It should save data with lease.
//
//go:generate mockery --name=MetaKv --with-expecter
type MetaKv interface {
	TxnKV
	GetPath(key string) string
	LoadWithPrefix(key string) ([]string, []string, error)
	CompareVersionAndSwap(key string, version int64, target string) (bool, error)
	WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error
}

// WatchKV is watchable MetaKv. As of today(2023/06/24), it's coupled with etcd.
//
//go:generate mockery --name=WatchKV --with-expecter
type WatchKV interface {
	MetaKv
	Watch(key string) clientv3.WatchChan
	WatchWithPrefix(key string) clientv3.WatchChan
	WatchWithRevision(key string, revision int64) clientv3.WatchChan
}

// SnapShotKV is TxnKV for snapshot data. It must save timestamp.
//
//go:generate mockery --name=SnapShotKV --with-expecter
type SnapShotKV interface {
	Save(key string, value string, ts typeutil.Timestamp) error
	Load(key string, ts typeutil.Timestamp) (string, error)
	MultiSave(kvs map[string]string, ts typeutil.Timestamp) error
	LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error)
	MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error
}
