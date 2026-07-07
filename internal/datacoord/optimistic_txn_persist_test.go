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

	"github.com/cockroachdb/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	tikvtest "github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

func TestSegmentTxnWrapperStagesMainRecordAfterAuxiliaryKVs(t *testing.T) {
	ctx := context.Background()
	inner := &recordingPersist{}
	wrapper := NewSegmentTxnWrapper(inner).WithMetaRootPath("root")

	txn := wrapper.Txn(ctx)
	segment := &datapb.SegmentInfo{
		ID:           10,
		CollectionID: 100,
		PartitionID:  20,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 101}}},
		},
	}
	require.NoError(t, txn.Insert(segmentKey(100, 20, 10), segment))

	require.GreaterOrEqual(t, len(inner.txn.ops), 2)
	require.Equal(t, opPut, inner.txn.ops[0].kind, "side-prefix binlog writes must be staged before the segment key")
	require.Equal(t, opInsert, inner.txn.ops[len(inner.txn.ops)-1].kind, "segment key must be the last op for this typed segment")
	require.Equal(t, segmentKey(100, 20, 10), inner.txn.ops[len(inner.txn.ops)-1].key)
}

func TestSegmentTxnWrapperReturnsCommittedMainResultsOnPartialCommit(t *testing.T) {
	ctx := context.Background()
	partialErr := errors.New("later batch failed")
	inner := &partialResultPersist{
		results: []TxnResult{
			{Version: 21},
			{Version: 22},
		},
		err: newPartialCommitError(partialErr),
	}
	wrapper := NewSegmentTxnWrapper(inner)

	txn := wrapper.Txn(ctx)
	segment := &datapb.SegmentInfo{
		ID:           10,
		CollectionID: 100,
		PartitionID:  20,
	}
	require.NoError(t, txn.Insert(segmentKey(100, 20, 10), segment))

	results, err := txn.Commit()
	require.ErrorIs(t, err, ErrPartialCommit)
	require.Len(t, results, 1)
	require.EqualValues(t, 21, results[0].Version)
}

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

func TestClassifyTiKVCommitConflictAsCASFailed(t *testing.T) {
	writeConflict := tikverr.NewErrWriteConflictWithArgs(1, 2, 3, []byte("k"), kvrpcpb.WriteConflict_Optimistic)
	require.ErrorIs(t, classifyTiKVCommitError(writeConflict), ErrCASFailed)

	latchConflict := &tikverr.ErrWriteConflictInLatch{StartTS: 1}
	require.ErrorIs(t, classifyTiKVCommitError(latchConflict), ErrCASFailed)

	rawErr := errors.New("other tikv failure")
	require.Same(t, rawErr, classifyTiKVCommitError(rawErr))
}

type recordingPersist struct {
	txn *recordingTxn
}

func (p *recordingPersist) Txn(ctx context.Context) Txn {
	p.txn = &recordingTxn{}
	return p.txn
}

func (p *recordingPersist) Scan(ctx context.Context, prefix string) ([]string, [][]byte, []int64, error) {
	return nil, nil, nil, nil
}

type recordingTxn struct {
	ops []txnOp
}

func (t *recordingTxn) Insert(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opInsert, key: key, value: value})
}

func (t *recordingTxn) Update(key string, value []byte, expectedVersion int64) {
	t.ops = append(t.ops, txnOp{kind: opUpdate, key: key, value: value, expectedVersion: expectedVersion})
}

func (t *recordingTxn) Delete(key string) {
	t.ops = append(t.ops, txnOp{kind: opDelete, key: key})
}

func (t *recordingTxn) Put(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opPut, key: key, value: value})
}

func (t *recordingTxn) Remove(key string) {
	t.ops = append(t.ops, txnOp{kind: opRemove, key: key})
}

func (t *recordingTxn) Commit() ([]TxnResult, error) {
	results := make([]TxnResult, len(t.ops))
	for i := range results {
		results[i].Version = int64(i + 1)
	}
	return results, nil
}

type partialResultPersist struct {
	results []TxnResult
	err     error
}

func (p *partialResultPersist) Txn(ctx context.Context) Txn {
	return partialResultTxn{results: p.results, err: p.err}
}

func (p *partialResultPersist) Scan(ctx context.Context, prefix string) ([]string, [][]byte, []int64, error) {
	return nil, nil, nil, nil
}

type partialResultTxn struct {
	results []TxnResult
	err     error
}

func (t partialResultTxn) Insert(key string, value []byte)                        {}
func (t partialResultTxn) Update(key string, value []byte, expectedVersion int64) {}
func (t partialResultTxn) Delete(key string)                                      {}
func (t partialResultTxn) Put(key string, value []byte)                           {}
func (t partialResultTxn) Remove(key string)                                      {}
func (t partialResultTxn) Commit() ([]TxnResult, error)                           { return t.results, t.err }
