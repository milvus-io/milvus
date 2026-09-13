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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

func TestSegmentTxnSkipsSidePrefixKVsForManifestSegment(t *testing.T) {
	ctx := context.Background()
	wrapper := NewSegmentTxnWrapper(NewOptimisticTxnMemoryPersist())
	segment := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   1,
		PartitionID:    2,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   "/tmp/manifest/1/2/3/manifest_1.json",
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 10, Binlogs: []*datapb.Binlog{{LogID: 100}}},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{FieldID: 11, Binlogs: []*datapb.Binlog{{LogID: 101}}},
		},
	}

	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Insert(segmentKey(1, 2, 3), segment))
	_, err := txn.Commit()
	require.NoError(t, err)

	for _, prefix := range []string{
		datacoordkv.SegmentBinlogPathPrefix,
		datacoordkv.SegmentDeltalogPathPrefix,
		datacoordkv.SegmentStatslogPathPrefix,
		datacoordkv.SegmentBM25logPathPrefix,
	} {
		keys, _, _, err := wrapper.ScanRaw(ctx, prefix)
		require.NoError(t, err)
		require.Empty(t, keys)
	}
}

func TestSegmentTxnStateOnlyDropPersistsLegacyEmbeddedBinlogs(t *testing.T) {
	ctx := context.Background()
	raw := NewOptimisticTxnMemoryPersist()
	wrapper := NewSegmentTxnWrapper(raw)
	key := segmentKey(1, 2, 3)
	live := &datapb.SegmentInfo{
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
	}
	value, err := proto.Marshal(live)
	require.NoError(t, err)
	seed := raw.Txn(ctx)
	seed.Insert(key, value)
	seedResults, err := seed.Commit()
	require.NoError(t, err)

	dropped := proto.Clone(live).(*datapb.SegmentInfo)
	dropped.State = commonpb.SegmentState_Dropped
	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Update(key, dropped, seedResults[0].Version, BinlogIncrement{}))
	_, err = txn.Commit()
	require.NoError(t, err)

	for _, prefix := range []string{
		datacoordkv.SegmentBinlogPathPrefix,
		datacoordkv.SegmentDeltalogPathPrefix,
	} {
		keys, _, _, err := wrapper.ScanRaw(ctx, prefix)
		require.NoError(t, err)
		require.Len(t, keys, 1)
	}
}

func TestSegmentTxnCompactsStatsPathsInSegmentRecord(t *testing.T) {
	ctx := context.Background()
	wrapper := NewSegmentTxnWrapper(NewOptimisticTxnMemoryPersist())
	segment := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   1,
		PartitionID:    2,
		NumOfRows:      999,
		StorageVersion: storage.StorageV2,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{LogID: 10, EntriesNum: 7}},
		}},
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			4: {FieldID: 4, Files: []string{"root/text_log/11/12/1/2/3/4/text.idx"}},
		},
		JsonKeyStats: map[int64]*datapb.JsonKeyStats{
			5: {FieldID: 5, Files: []string{"root/insert_log/1/2/3/_stats/json_stats.5/shared/key.idx"}},
		},
	}

	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Insert(segmentKey(1, 2, 3), segment))
	_, err := txn.Commit()
	require.NoError(t, err)

	_, values, _, err := wrapper.ScanRaw(ctx, segmentMetaPrefix)
	require.NoError(t, err)
	require.Len(t, values, 1)
	persisted := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(values[0], persisted))
	require.EqualValues(t, 7, persisted.GetNumOfRows())
	require.Equal(t, []string{"text.idx"}, persisted.GetTextStatsLogs()[4].GetFiles())
	require.Equal(t, []string{"shared/key.idx"}, persisted.GetJsonKeyStats()[5].GetFiles())

	// buildSegmentWrite clones before normalizing; callers keep their full paths.
	require.EqualValues(t, 999, segment.GetNumOfRows())
	require.Equal(t, "root/text_log/11/12/1/2/3/4/text.idx", segment.GetTextStatsLogs()[4].GetFiles()[0])
	require.Equal(t, "root/insert_log/1/2/3/_stats/json_stats.5/shared/key.idx", segment.GetJsonKeyStats()[5].GetFiles()[0])
}

func TestSegmentTxnKeepsManifestSegmentRowCount(t *testing.T) {
	ctx := context.Background()
	wrapper := NewSegmentTxnWrapper(NewOptimisticTxnMemoryPersist())
	segment := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   1,
		PartitionID:    2,
		NumOfRows:      999,
		StorageVersion: storage.StorageV2,
		ManifestPath:   "/tmp/manifest/1/2/3/manifest_1.json",
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{LogID: 10, EntriesNum: 7}},
		}},
	}

	txn := wrapper.Txn(ctx)
	require.NoError(t, txn.Insert(segmentKey(1, 2, 3), segment))
	_, err := txn.Commit()
	require.NoError(t, err)

	_, values, _, err := wrapper.ScanRaw(ctx, segmentMetaPrefix)
	require.NoError(t, err)
	require.Len(t, values, 1)
	persisted := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(values[0], persisted))
	require.EqualValues(t, 999, persisted.GetNumOfRows())
}
