// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdkv

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WalkPrefix reads an etcd prefix page by page in key order and passes every
// key value, revisions included, to fn. A requestTimeout of 0 bounds each page
// by ctx alone.
func WalkPrefix(
	ctx context.Context,
	client clientv3.KV,
	prefix string,
	paginationSize int,
	requestTimeout time.Duration,
	fn func(*mvccpb.KeyValue) error,
) error {
	key := prefix
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(int64(paginationSize)),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
	}

	for {
		requestCtx := ctx
		cancel := func() {}
		if requestTimeout > 0 {
			requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
		}
		resp, err := client.Get(requestCtx, key, opts...)
		cancel()
		if err != nil {
			return merr.WrapErrIoFailed(key, err)
		}

		for _, keyValue := range resp.Kvs {
			if err := fn(keyValue); err != nil {
				return err
			}
		}
		if !resp.More {
			return nil
		}
		if len(resp.Kvs) == 0 {
			return merr.WrapErrServiceInternalMsg(
				"etcd paginated range returned more=true without key values for prefix %q",
				prefix,
			)
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}
