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

//go:build test

package policy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func NewBumpSchemaVersionTestCollection(collectionID int64, schemaVersion int32) *meta.CollectionInfo {
	return &meta.CollectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{
			Name:    "test_collection",
			Version: schemaVersion,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
}

func NewBumpSchemaVersionTestSegment(collectionID, segmentID int64, schemaVersion int32, storageVersion int64, manifestPath string) *meta.SegmentInfo {
	return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    10,
		InsertChannel:  "ch-1",
		Level:          datapb.SegmentLevel_L1,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		SchemaVersion:  schemaVersion,
		StorageVersion: storageVersion,
		ManifestPath:   manifestPath,
		StartPosition:  &msgpb.MsgPosition{Timestamp: 10000},
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{{LogPath: "test_path", EntriesNum: 1000}},
		}},
	}}
}

func GenSegmentsForMeta(label *view.CompactionGroupLabel) map[int64]*meta.SegmentInfo {
	segArgs := []struct {
		ID    UniqueID
		Level datapb.SegmentLevel
		State commonpb.SegmentState
		PosT  Timestamp

		InsertLogSize  int64
		InsertLogCount int

		DelatLogSize  int64
		DeltaLogCount int
	}{
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * meta.MB, 0, 4 * meta.MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * meta.MB, 0, 4 * meta.MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * meta.MB, 0, 4 * meta.MB, 1},
		{103, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 50000, 0 * meta.MB, 0, 4 * meta.MB, 1},
		{200, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 50000, 10 * meta.MB, 1, 0, 0},
		{201, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 30000, 10 * meta.MB, 1, 0, 0},
		{300, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 10000, 10 * meta.MB, 1, 0, 0},
		{301, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 20000, 10 * meta.MB, 1, 0, 0},
	}

	segments := make(map[int64]*meta.SegmentInfo)
	for _, arg := range segArgs {
		info := GenTestSegmentInfo(label, arg.ID, arg.Level, arg.State)
		if info.Level == datapb.SegmentLevel_L0 || info.State == commonpb.SegmentState_Flushed {
			info.Deltalogs = GenTestBinlogs(arg.DeltaLogCount, arg.DelatLogSize)
			info.DmlPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}
		info.Binlogs = GenTestBinlogs(arg.InsertLogCount, arg.InsertLogSize)
		if info.State == commonpb.SegmentState_Growing {
			info.StartPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}

		segments[arg.ID] = info
	}

	return segments
}

func GenTestSegmentInfo(label *view.CompactionGroupLabel, ID UniqueID, level datapb.SegmentLevel, state commonpb.SegmentState) *meta.SegmentInfo {
	return &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            ID,
			CollectionID:  label.CollectionID,
			PartitionID:   label.PartitionID,
			InsertChannel: label.Channel,
			Level:         level,
			State:         state,
		},
	}
}

func GenTestBinlogs(logCount int, logSize int64) []*datapb.FieldBinlog {
	var binlogs []*datapb.Binlog

	for i := 0; i < logCount; i++ {
		binlog := &datapb.Binlog{
			LogSize:    logSize,
			MemorySize: logSize,
		}
		binlogs = append(binlogs, binlog)
	}

	return []*datapb.FieldBinlog{
		{Binlogs: binlogs},
	}
}
