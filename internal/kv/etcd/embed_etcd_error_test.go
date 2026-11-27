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

package etcdkv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type failedEmbeddedTxnKV struct {
	clientv3.KV
	err error
}

func (kv failedEmbeddedTxnKV) Txn(context.Context) clientv3.Txn {
	return failedEmbeddedTxn{err: kv.err}
}

type failedEmbeddedTxn struct {
	err error
}

func (t failedEmbeddedTxn) If(...clientv3.Cmp) clientv3.Txn  { return t }
func (t failedEmbeddedTxn) Then(...clientv3.Op) clientv3.Txn { return t }
func (t failedEmbeddedTxn) Else(...clientv3.Op) clientv3.Txn { return t }
func (t failedEmbeddedTxn) Commit() (*clientv3.TxnResponse, error) {
	return nil, t.err
}

func TestEmbedEtcdKVMultiSavePreservesEtcdCause(t *testing.T) {
	for _, tc := range []struct {
		cause     error
		retriable bool
	}{
		{rpctypes.ErrLeaderChanged, true},
		{rpctypes.ErrPermissionDenied, false},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
	} {
		t.Run(tc.cause.Error(), func(t *testing.T) {
			kv := &EmbedEtcdKV{
				client:         &clientv3.Client{KV: failedEmbeddedTxnKV{err: tc.cause}},
				rootPath:       "test",
				requestTimeout: time.Second,
			}
			err := kv.MultiSave(context.Background(), map[string]string{"rg": "value"})
			require.ErrorIs(t, err, tc.cause)
			require.ErrorIs(t, err, merr.ErrIoFailed)
			require.EqualValues(t, 1001, merr.Code(err))
			require.Equal(t, tc.retriable, etcd.IsRetriableEtcdErr(err))
		})
	}
}
