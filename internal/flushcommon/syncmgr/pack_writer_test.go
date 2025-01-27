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

package syncmgr

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestNextID(t *testing.T) {
	al := allocator.NewMockGIDAllocator()
	i := int64(0)
	al.AllocF = func(count uint32) (int64, int64, error) {
		rt := i
		i += int64(count)
		return rt, int64(count), nil
	}
	al.AllocOneF = func() (allocator.UniqueID, error) {
		rt := i
		i++
		return rt, nil
	}
	bw := NewBulkPackWriter(nil, nil, al)
	bw.prefetchIDs(new(SyncPack).WithFlush())

	t.Run("normal_next", func(t *testing.T) {
		id := bw.nextID()
		assert.Equal(t, int64(0), id)
	})
	t.Run("id_exhausted", func(t *testing.T) {
		assert.Panics(t, func() {
			bw.nextID()
		})
	})
}

func TestBulkPackWriter_Write(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, nil, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	schema := &schemapb.CollectionSchema{
		Name: "sync_task_test_col",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}

	mc := metacache.NewMockMetaCache(t)
	mc.EXPECT().Collection().Return(collectionID).Maybe()
	mc.EXPECT().Schema().Return(schema).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("files").Maybe()
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	deletes := &storage.DeleteData{}
	entriesNum := 10
	for i := 0; i < entriesNum; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := uint64(100 + i)
		deletes.Append(pk, ts)
	}

	bw := &BulkPackWriter{
		metaCache:    mc,
		chunkManager: cm,
		allocator:    allocator.NewLocalAllocator(10000, 100000),
	}

	tests := []struct {
		name          string
		pack          *SyncPack
		wantInserts   map[int64]*datapb.FieldBinlog
		wantDeltas    func(pack *SyncPack) *datapb.FieldBinlog
		wantStats     map[int64]*datapb.FieldBinlog
		wantBm25Stats map[int64]*datapb.FieldBinlog
		wantSize      func(pack *SyncPack) int64
		wantErr       error
	}{
		{
			name:        "empty",
			pack:        new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName),
			wantInserts: map[int64]*datapb.FieldBinlog{},
			wantDeltas: func(pack *SyncPack) *datapb.FieldBinlog {
				return &datapb.FieldBinlog{}
			},
			wantStats:     map[int64]*datapb.FieldBinlog{},
			wantBm25Stats: map[int64]*datapb.FieldBinlog{},
			wantSize:      func(pack *SyncPack) int64 { return 0 },
			wantErr:       nil,
		},
		{
			name:        "with delete",
			pack:        new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithDeleteData(deletes),
			wantInserts: map[int64]*datapb.FieldBinlog{},
			wantDeltas: func(pack *SyncPack) *datapb.FieldBinlog {
				s, _ := NewStorageSerializer(bw.metaCache)
				deltaBlob, _ := s.serializeDeltalog(pack)
				return &datapb.FieldBinlog{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum: int64(entriesNum),
							LogPath:    "files/delta_log/123/456/789/10000",
							LogSize:    int64(len(deltaBlob.GetValue())),
							MemorySize: deltaBlob.MemorySize,
						},
					},
				}
			},
			wantStats:     map[int64]*datapb.FieldBinlog{},
			wantBm25Stats: map[int64]*datapb.FieldBinlog{},
			wantSize: func(pack *SyncPack) int64 {
				s, _ := NewStorageSerializer(bw.metaCache)
				deltaBlob, _ := s.serializeDeltalog(pack)
				return int64(len(deltaBlob.GetValue()))
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInserts, gotDeltas, gotStats, gotBm25Stats, gotSize, err := bw.Write(context.Background(), tt.pack)
			if err != tt.wantErr {
				t.Errorf("BulkPackWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInserts, tt.wantInserts) {
				t.Errorf("BulkPackWriter.Write() gotInserts = %v, want %v", gotInserts, tt.wantInserts)
			}
			if !reflect.DeepEqual(gotDeltas, tt.wantDeltas(tt.pack)) {
				t.Errorf("BulkPackWriter.Write() gotDeltas = %v, want %v", gotDeltas, tt.wantDeltas(tt.pack))
			}
			if !reflect.DeepEqual(gotStats, tt.wantStats) {
				t.Errorf("BulkPackWriter.Write() gotStats = %v, want %v", gotStats, tt.wantStats)
			}
			if !reflect.DeepEqual(gotBm25Stats, tt.wantBm25Stats) {
				t.Errorf("BulkPackWriter.Write() gotBm25Stats = %v, want %v", gotBm25Stats, tt.wantBm25Stats)
			}
			if gotSize != tt.wantSize(tt.pack) {
				t.Errorf("BulkPackWriter.Write() gotSize = %v, want %v", gotSize, tt.wantSize(tt.pack))
			}
		})
	}
}
