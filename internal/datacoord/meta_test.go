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
	"path/filepath"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestMeta_Basic(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"
	ctx := context.Background()

	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	testSchema := newTestSchema()
	collInfo := &datapb.CollectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{partID0, partID1},
	}
	collInfoWoPartition := &datapb.CollectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	}

	t.Run("Test Collection", func(t *testing.T) {
		meta.AddCollection(collInfo)
		// check has collection
		collInfo := meta.GetCollection(collID)
		assert.NotNil(t, collInfo)

		// check partition info
		assert.EqualValues(t, collID, collInfo.ID)
		assert.EqualValues(t, testSchema, collInfo.Schema)
		assert.EqualValues(t, 2, len(collInfo.Partitions))
		assert.EqualValues(t, partID0, collInfo.Partitions[0])
		assert.EqualValues(t, partID1, collInfo.Partitions[1])
	})

	t.Run("Test Segment", func(t *testing.T) {
		meta.AddCollection(collInfoWoPartition)
		// create seg0 for partition0, seg0/seg1 for partition1
		segID0_0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo0_0 := buildSegment(collID, partID0, segID0_0, channelName)
		segID1_0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1_0 := buildSegment(collID, partID1, segID1_0, channelName)
		segID1_1, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1_1 := buildSegment(collID, partID1, segID1_1, channelName)

		// check AddSegment
		err = meta.AddSegment(segInfo0_0)
		assert.Nil(t, err)
		err = meta.AddSegment(segInfo1_0)
		assert.Nil(t, err)
		err = meta.AddSegment(segInfo1_1)
		assert.Nil(t, err)

		// check GetSegment
		info0_0 := meta.GetSegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.True(t, proto.Equal(info0_0, segInfo0_0))
		info1_0 := meta.GetSegment(segID1_0)
		assert.NotNil(t, info1_0)
		assert.True(t, proto.Equal(info1_0, segInfo1_0))

		// check GetSegmentsOfCollection
		segIDs := meta.GetSegmentsIDOfCollection(collID)
		assert.EqualValues(t, 3, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check GetSegmentsOfPartition
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID0)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID1)
		assert.EqualValues(t, 2, len(segIDs))
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check DropSegment
		err = meta.DropSegment(segID1_0)
		assert.Nil(t, err)
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID1)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID1_1)

		err = meta.SetState(segID0_0, commonpb.SegmentState_Sealed)
		assert.Nil(t, err)
		err = meta.SetState(segID0_0, commonpb.SegmentState_Flushed)
		assert.Nil(t, err)

		info0_0 = meta.GetSegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.EqualValues(t, commonpb.SegmentState_Flushed, info0_0.State)
	})

	t.Run("Test segment with kv fails", func(t *testing.T) {
		// inject error for `Save`
		memoryKV := memkv.NewMemoryKV()
		fkv := &saveFailKV{TxnKV: memoryKV}
		meta, err := newMeta(fkv)
		assert.Nil(t, err)

		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.NotNil(t, err)

		fkv2 := &removeFailKV{TxnKV: memoryKV}
		meta, err = newMeta(fkv2)
		assert.Nil(t, err)
		// nil, since no segment yet
		err = meta.DropSegment(0)
		assert.Nil(t, err)
		// nil, since Save error not injected
		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.Nil(t, err)
		// error injected
		err = meta.DropSegment(0)
		assert.NotNil(t, err)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300

		// no segment
		nums := meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(segInfo0)
		assert.Nil(t, err)

		// add seg2 with 300 rows
		segID1, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(segInfo1)
		assert.Nil(t, err)

		// check partition/collection statistics
		nums = meta.GetNumRowsOfPartition(collID, partID0)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
		nums = meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
	})

	t.Run("Test GetSegmentsChanPart", func(t *testing.T) {
		result := meta.GetSegmentsChanPart(func(*SegmentInfo) bool { return true })
		assert.Equal(t, 2, len(result))
		for _, entry := range result {
			assert.Equal(t, "c1", entry.channelName)
			if entry.partitionID == UniqueID(100) {
				assert.Equal(t, 3, len(entry.segments))
			}
			if entry.partitionID == UniqueID(101) {
				assert.Equal(t, 1, len(entry.segments))
			}
		}
		result = meta.GetSegmentsChanPart(func(seg *SegmentInfo) bool { return seg.GetCollectionID() == 10 })
		assert.Equal(t, 0, len(result))
	})
}

func TestGetUnFlushedSegments(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	s1 := &datapb.SegmentInfo{
		ID:           0,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Growing,
	}
	err = meta.AddSegment(NewSegmentInfo(s1))
	assert.Nil(t, err)
	s2 := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	}
	err = meta.AddSegment(NewSegmentInfo(s2))
	assert.Nil(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.Nil(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}

func TestUpdateFlushSegmentsInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "binlog0")},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statslog0")}}}
		err = meta.AddSegment(segment1)
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, true, false, true, []*datapb.FieldBinlog{getFieldBinlogPaths(1, "binlog1")},
			[]*datapb.FieldBinlog{getFieldBinlogPaths(1, "statslog1")},
			[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000}}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.Nil(t, err)

		updated := meta.GetSegment(1)
		expected := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushing, NumOfRows: 10,
			StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}},
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogPaths(1, "binlog0", "binlog1")},
			Statslogs:     []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statslog0", "statslog1")},
			Deltalogs:     []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000}}}},
		}}
		assert.True(t, proto.Equal(expected, updated))
	})

	t.Run("update non-existed segment", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, false, false, nil, nil, nil, nil, nil)
		assert.Nil(t, err)
	})

	t.Run("update checkpoints and start position of non existed segment", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}}
		err = meta.AddSegment(segment1)
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, false, false, nil, nil, nil, []*datapb.CheckPoint{{SegmentID: 2, NumOfRows: 10}},

			[]*datapb.SegmentStartPosition{{SegmentID: 2, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.Nil(t, err)
		assert.Nil(t, meta.GetSegment(2))
	})

	t.Run("test save etcd failed", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		failedKv := &saveFailKV{kv}
		meta, err := newMeta(failedKv)
		assert.Nil(t, err)

		segmentInfo := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        1,
				NumOfRows: 0,
				State:     commonpb.SegmentState_Growing,
			},
		}
		meta.segments.SetSegment(1, segmentInfo)

		err = meta.UpdateFlushSegmentsInfo(1, true, false, false, []*datapb.FieldBinlog{getFieldBinlogPaths(1, "binlog")},
			[]*datapb.FieldBinlog{getFieldBinlogPaths(1, "statslog")},
			[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000}}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.NotNil(t, err)
		assert.Equal(t, "mocked fail", err.Error())
		segmentInfo = meta.GetSegment(1)
		assert.EqualValues(t, 0, segmentInfo.NumOfRows)
		assert.Equal(t, commonpb.SegmentState_Growing, segmentInfo.State)
		assert.Nil(t, segmentInfo.Binlogs)
		assert.Nil(t, segmentInfo.StartPosition)
	})
}

func TestSaveHandoffMeta(t *testing.T) {
	meta, err := newMeta(memkv.NewMemoryKV())
	assert.Nil(t, err)

	info := &datapb.SegmentInfo{
		ID:    100,
		State: commonpb.SegmentState_Flushed,
	}
	segmentInfo := &SegmentInfo{
		SegmentInfo: info,
	}

	err = meta.saveSegmentInfo(segmentInfo)
	assert.Nil(t, err)

	keys, _, err := meta.client.LoadWithPrefix(util.HandoffSegmentPrefix)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(keys))
	segmentID, err := strconv.ParseInt(filepath.Base(keys[0]), 10, 64)
	assert.Nil(t, err)
	assert.Equal(t, 100, int(segmentID))
}

func Test_meta_CompleteMergeCompaction(t *testing.T) {
	type fields struct {
		client      kv.TxnKV
		collections map[UniqueID]*datapb.CollectionInfo
		segments    *SegmentsInfo
	}
	type args struct {
		compactionLogs []*datapb.CompactionSegmentBinlogs
		result         *datapb.CompactionResult
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test normal merge compaction",
			fields{
				memkv.NewMemoryKV(),
				nil,
				&SegmentsInfo{map[int64]*SegmentInfo{
					1: {SegmentInfo: &datapb.SegmentInfo{
						ID:        1,
						Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
						Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
						Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
					}},
					2: {SegmentInfo: &datapb.SegmentInfo{
						ID:        2,
						Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
						Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
						Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
					}},
				}},
			},
			args{
				[]*datapb.CompactionSegmentBinlogs{
					{
						SegmentID:           1,
						FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
						Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
						Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
					},
					{
						SegmentID:           2,
						FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
						Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
						Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
					},
				},
				&datapb.CompactionResult{
					SegmentID:           3,
					InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log5")},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog5")},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog5")},
					NumOfRows:           1,
				},
			},
			false,
		},
		{
			"test removing all data merge compaction",
			fields{
				memkv.NewMemoryKV(),
				nil,
				&SegmentsInfo{map[int64]*SegmentInfo{
					1: {SegmentInfo: &datapb.SegmentInfo{
						ID:        1,
						Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
						Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
						Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
					}},
					2: {SegmentInfo: &datapb.SegmentInfo{
						ID:        2,
						Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
						Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
						Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
					}},
				}},
			},
			args{
				[]*datapb.CompactionSegmentBinlogs{
					{
						SegmentID:           1,
						FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
						Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
						Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
					},
					{
						SegmentID:           2,
						FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
						Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
						Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
					},
				},
				&datapb.CompactionResult{
					SegmentID:           3,
					InsertLogs:          nil,
					Field2StatslogPaths: nil,
					Deltalogs:           nil,
					NumOfRows:           0,
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				client:      tt.fields.client,
				collections: tt.fields.collections,
				segments:    tt.fields.segments,
			}
			canCompaction := func(segment *datapb.CompactionSegmentBinlogs) bool {
				return true
			}
			err := m.CompleteMergeCompaction(tt.args.compactionLogs, tt.args.result, canCompaction)
			assert.Equal(t, tt.wantErr, err != nil)
			if err == nil {
				for _, l := range tt.args.compactionLogs {
					assert.Nil(t, m.GetSegment(l.GetSegmentID()))
				}
				segment := m.GetSegment(tt.args.result.SegmentID)
				assert.Equal(t, segment != nil, tt.args.result.NumOfRows > 0)
				if segment != nil {
					assert.EqualValues(t, tt.args.result.GetInsertLogs(), segment.GetBinlogs())
					assert.EqualValues(t, tt.args.result.GetField2StatslogPaths(), segment.GetStatslogs())
					assert.EqualValues(t, tt.args.result.GetDeltalogs(), segment.GetDeltalogs())
					assert.NotZero(t, segment.lastFlushTime)
				}
			}
		})
	}
}

func Test_meta_CompleteInnerCompaction(t *testing.T) {
	type fields struct {
		client      kv.TxnKV
		collections map[UniqueID]*datapb.CollectionInfo
		segments    *SegmentsInfo
	}
	type args struct {
		segmentBinlogs *datapb.CompactionSegmentBinlogs
		result         *datapb.CompactionResult
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *SegmentInfo
	}{
		{
			"test normal inner compaction",
			fields{
				memkv.NewMemoryKV(),
				nil,
				&SegmentsInfo{
					map[int64]*SegmentInfo{
						1: {SegmentInfo: &datapb.SegmentInfo{
							ID:        1,
							Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
							Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
							Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
						}},
					},
				},
			},
			args{
				&datapb.CompactionSegmentBinlogs{
					SegmentID:           1,
					FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1")},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1")},
				},
				&datapb.CompactionResult{
					SegmentID:           1,
					InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3")},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3")},
					NumOfRows:           1,
				},
			},
			false,
			&SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:        1,
					Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2", "log3")},
					Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog2", "statlog3")},
					Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog2", "deltalog3")},
				},
			},
		},
		{
			"test removing all data inner compaction",
			fields{
				memkv.NewMemoryKV(),
				nil,
				&SegmentsInfo{
					map[int64]*SegmentInfo{
						1: {SegmentInfo: &datapb.SegmentInfo{
							ID:        1,
							Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
							Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
							Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
						}},
					},
				},
			},
			args{
				&datapb.CompactionSegmentBinlogs{
					SegmentID:           1,
					FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
				},
				&datapb.CompactionResult{
					SegmentID:           1,
					InsertLogs:          nil,
					Field2StatslogPaths: nil,
					Deltalogs:           nil,
					NumOfRows:           0,
				},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				client:      tt.fields.client,
				collections: tt.fields.collections,
				segments:    tt.fields.segments,
			}
			err := m.CompleteInnerCompaction(tt.args.segmentBinlogs, tt.args.result)
			assert.Equal(t, tt.wantErr, err != nil)

			if err != nil {
				segment := m.GetSegment(tt.args.result.SegmentID)
				assert.EqualValues(t, tt.want, segment)
			}
		})
	}
}

func Test_meta_SetSegmentCompacting(t *testing.T) {
	type fields struct {
		client   kv.TxnKV
		segments *SegmentsInfo
	}
	type args struct {
		segmentID  UniqueID
		compacting bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test set segment compacting",
			fields{
				memkv.NewMemoryKV(),
				&SegmentsInfo{
					map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
							isCompacting: false,
						},
					},
				},
			},
			args{
				segmentID:  1,
				compacting: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				client:   tt.fields.client,
				segments: tt.fields.segments,
			}
			m.SetSegmentCompacting(tt.args.segmentID, tt.args.compacting)
			segment := m.GetSegment(tt.args.segmentID)
			assert.Equal(t, tt.args.compacting, segment.isCompacting)
		})
	}
}

func Test_meta_GetSegmentsOfCollection(t *testing.T) {
	type fields struct {
		segments *SegmentsInfo
	}
	type args struct {
		collectionID UniqueID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		expect []*SegmentInfo
	}{
		{
			"test get segments",
			fields{
				&SegmentsInfo{
					map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           1,
								CollectionID: 1,
								State:        commonpb.SegmentState_Flushed,
							},
						},
						2: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           2,
								CollectionID: 1,
								State:        commonpb.SegmentState_Growing,
							},
						},
						3: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           3,
								CollectionID: 2,
								State:        commonpb.SegmentState_Flushed,
							},
						},
					},
				},
			},
			args{
				collectionID: 1,
			},
			[]*SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1,
						CollectionID: 1,
						State:        commonpb.SegmentState_Flushed,
					},
				},
				{
					SegmentInfo: &datapb.SegmentInfo{
						ID:           2,
						CollectionID: 1,
						State:        commonpb.SegmentState_Growing,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				segments: tt.fields.segments,
			}
			got := m.GetSegmentsOfCollection(tt.args.collectionID)
			assert.ElementsMatch(t, tt.expect, got)
		})
	}
}

func TestMeta_HasSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID: 1,
					},
					currRows: 100,
				},
			},
		},
	}

	has, err := m.HasSegments([]UniqueID{1})
	assert.Equal(t, true, has)
	assert.NoError(t, err)

	has, err = m.HasSegments([]UniqueID{2})
	assert.Equal(t, false, has)
	assert.Error(t, err)
}

func TestMeta_GetAllSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    1,
						State: commonpb.SegmentState_Growing,
					},
				},
				2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    2,
						State: commonpb.SegmentState_Dropped,
					},
				},
			},
		},
	}

	seg1 := m.GetSegment(1)
	seg1All := m.GetAllSegment(1)
	seg2 := m.GetSegment(2)
	seg2All := m.GetAllSegment(2)
	assert.NotNil(t, seg1)
	assert.NotNil(t, seg1All)
	assert.Nil(t, seg2)
	assert.NotNil(t, seg2All)
}
