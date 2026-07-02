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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tikvtest "github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

func TestTiKVTxnUpdateRejectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnTiKVPersist(tikvtest.SetupLocalTxn())

	insertTxn := persist.Txn(ctx)
	insertTxn.Insert("datacoord-meta/test/cas", []byte("v1"))
	insertResults, err := insertTxn.Commit()
	require.NoError(t, err)
	require.Len(t, insertResults, 1)
	require.NotZero(t, insertResults[0].Version)

	concurrentTxn := persist.Txn(ctx)
	concurrentTxn.Update("datacoord-meta/test/cas", []byte("v2"), insertResults[0].Version)
	_, err = concurrentTxn.Commit()
	require.NoError(t, err)

	staleTxn := persist.Txn(ctx)
	staleTxn.Update("datacoord-meta/test/cas", []byte("stale"), insertResults[0].Version)
	_, err = staleTxn.Commit()
	require.ErrorIs(t, err, ErrCASFailed)

	keys, values, versions, err := persist.Scan(ctx, "datacoord-meta/test/")
	require.NoError(t, err)
	require.Equal(t, []string{"datacoord-meta/test/cas"}, keys)
	require.Equal(t, [][]byte{[]byte("v2")}, values)
	require.Len(t, versions, 1)
	require.NotEqual(t, insertResults[0].Version, versions[0])
}

func TestTiKVScanReturnsKeyCommitTS(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnTiKVPersist(tikvtest.SetupLocalTxn())

	txn := persist.Txn(ctx)
	txn.Insert("datacoord-meta/test/scan", []byte("v1"))
	results, err := txn.Commit()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotZero(t, results[0].Version)

	keys, values, versions, err := persist.Scan(ctx, "datacoord-meta/test/")
	require.NoError(t, err)
	require.Equal(t, []string{"datacoord-meta/test/scan"}, keys)
	require.Equal(t, [][]byte{[]byte("v1")}, values)
	require.Equal(t, []int64{results[0].Version}, versions)
}

func TestMemoryTxnUpdateRejectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnMemoryPersist()

	txn := persist.Txn(ctx)
	txn.Insert("key", []byte("v1"))
	results, err := txn.Commit()
	require.NoError(t, err)

	txn = persist.Txn(ctx)
	txn.Update("key", []byte("v2"), results[0].Version)
	_, err = txn.Commit()
	require.NoError(t, err)

	txn = persist.Txn(ctx)
	txn.Update("key", []byte("stale"), results[0].Version)
	_, err = txn.Commit()
	require.ErrorIs(t, err, ErrCASFailed)
}
