// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package kv

import (
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type BaseKV interface {
	Load(key string) (string, error)
	MultiLoad(keys []string) ([]string, error)
	LoadWithPrefix(key string) ([]string, []string, error)
	Save(key, value string) error
	MultiSave(kvs map[string]string) error
	Remove(key string) error
	MultiRemove(keys []string) error
	RemoveWithPrefix(key string) error

	Close()
}

type TxnKV interface {
	BaseKV
	MultiSaveAndRemove(saves map[string]string, removals []string) error
	MultiRemoveWithPrefix(keys []string) error
	MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error
}

type SnapShotKV interface {
	Save(key string, value string, ts typeutil.Timestamp) error
	Load(key string, ts typeutil.Timestamp) (string, error)
	MultiSave(kvs map[string]string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error
	LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error)
	MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error
}
