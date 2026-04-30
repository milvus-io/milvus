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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestSegmentTxnCommitReturnsStitchedSegment(t *testing.T) {
	ctx := context.Background()
	wrapper := NewSegmentTxnWrapper(NewOptimisticTxnMemoryPersist())
	key := segmentKey(1, 2, 3)
	seg := &datapb.SegmentInfo{
		ID:           3,
		CollectionID: 1,
		PartitionID:  2,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 10,
				Binlogs: []*datapb.Binlog{
					{LogID: 100},
				},
			},
		},
	}

	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Insert(key, seg))
	results, err := txn.Commit()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Segment.GetBinlogs(), 1)

	updated := proto.Clone(results[0].Segment).(*datapb.SegmentInfo)
	updated.State = commonpb.SegmentState_Dropped
	txn = wrapper.Txn(ctx)
	require.NoError(t, txn.Update(key, updated, results[0].Version, BinlogIncrement{}))
	results, err = txn.Commit()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Segment.GetBinlogs(), 1)
	require.Equal(t, commonpb.SegmentState_Dropped, results[0].Segment.GetState())

	_, values, _, err := wrapper.ScanRaw(ctx, segmentMetaPrefix)
	require.NoError(t, err)
	require.Len(t, values, 1)
	persisted := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(values[0], persisted))
	require.Empty(t, persisted.GetBinlogs())
}

func TestSegmentTxnWritesSidePrefixKVsUnderMetaRootPath(t *testing.T) {
	ctx := context.Background()
	metaRootPath := "by-dev/meta"
	wrapper := NewSegmentTxnWrapper(NewOptimisticTxnMemoryPersist()).WithMetaRootPath(metaRootPath)
	key := metaRootPath + "/" + segmentKey(1, 2, 3)
	seg := &datapb.SegmentInfo{
		ID:           3,
		CollectionID: 1,
		PartitionID:  2,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 10, Binlogs: []*datapb.Binlog{{LogID: 100}}},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{FieldID: 11, Binlogs: []*datapb.Binlog{{LogID: 101}}},
		},
		Statslogs: []*datapb.FieldBinlog{
			{FieldID: 12, Binlogs: []*datapb.Binlog{{LogID: 102}}},
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			{FieldID: 13, Binlogs: []*datapb.Binlog{{LogID: 103}}},
		},
	}

	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Insert(key, seg))
	_, err := txn.Commit()
	require.NoError(t, err)

	for _, prefix := range []string{
		datacoordkv.SegmentBinlogPathPrefix,
		datacoordkv.SegmentDeltalogPathPrefix,
		datacoordkv.SegmentStatslogPathPrefix,
		datacoordkv.SegmentBM25logPathPrefix,
	} {
		keys, _, _, err := wrapper.ScanRaw(ctx, metaRootPath+"/"+prefix)
		require.NoError(t, err)
		require.Len(t, keys, 1)

		keys, _, _, err = wrapper.ScanRaw(ctx, prefix)
		require.NoError(t, err)
		require.Empty(t, keys)
	}
}
