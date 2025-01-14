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

package binlog

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	logID        = int64(99)
	collectionID = int64(2)
	partitionID  = int64(1)
	segmentID    = int64(1)
	segmentID2   = int64(11)
	fieldID      = int64(1)
	rootPath     = "a"

	binlogPath   = metautil.BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, logID)
	deltalogPath = metautil.BuildDeltaLogPath(rootPath, collectionID, partitionID, segmentID, logID)
	statslogPath = metautil.BuildStatsLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, logID)

	binlogPath2   = metautil.BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID2, fieldID, logID)
	deltalogPath2 = metautil.BuildDeltaLogPath(rootPath, collectionID, partitionID, segmentID2, logID)
	statslogPath2 = metautil.BuildStatsLogPath(rootPath, collectionID, partitionID, segmentID2, fieldID, logID)

	invalidSegment = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    "badpath",
					},
				},
			},
		},
	}

	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    binlogPath,
				},
			},
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    deltalogPath,
				},
			},
		},
	}
	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    statslogPath,
				},
			},
		},
	}

	getlogs = func(logpath string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    logpath,
					},
				},
			},
		}
	}

	segment1 = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}

	droppedSegment = &datapb.SegmentInfo{
		ID:           segmentID2,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Dropped,
		Binlogs:      getlogs(binlogPath2),
		Deltalogs:    getlogs(deltalogPath2),
		Statslogs:    getlogs(statslogPath2),
	}
)

func getSegment(rootPath string, collectionID, partitionID, segmentID, fieldID int64, binlogNum int) *datapb.SegmentInfo {
	binLogPaths := make([]*datapb.Binlog, binlogNum)
	for i := 0; i < binlogNum; i++ {
		binLogPaths[i] = &datapb.Binlog{
			EntriesNum: 10000,
			LogPath:    metautil.BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(i)),
		}
	}
	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: binLogPaths,
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildDeltaLogPath(rootPath, collectionID, partitionID, segmentID, int64(rand.Int())),
				},
			},
		},
	}

	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildStatsLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(rand.Int())),
				},
			},
		},
	}

	return &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    10000,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}
}

func TestBinlog_Compress(t *testing.T) {
	paramtable.Init()
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	segmentInfo := getSegment(rootPath, 0, 1, 2, 3, 10)
	val, err := proto.Marshal(segmentInfo)
	assert.NoError(t, err)

	compressedSegmentInfo := proto.Clone(segmentInfo).(*datapb.SegmentInfo)
	err = CompressBinLogs(compressedSegmentInfo.GetBinlogs(), compressedSegmentInfo.GetDeltalogs(), compressedSegmentInfo.GetStatslogs())
	assert.NoError(t, err)

	valCompressed, err := proto.Marshal(compressedSegmentInfo)
	assert.NoError(t, err)

	assert.True(t, len(valCompressed) < len(val))

	// make sure the compact
	unmarshaledSegmentInfo := &datapb.SegmentInfo{}
	proto.Unmarshal(val, unmarshaledSegmentInfo)

	unmarshaledSegmentInfoCompressed := &datapb.SegmentInfo{}
	proto.Unmarshal(valCompressed, unmarshaledSegmentInfoCompressed)
	DecompressBinLogs(unmarshaledSegmentInfoCompressed)

	assert.Equal(t, len(unmarshaledSegmentInfo.GetBinlogs()), len(unmarshaledSegmentInfoCompressed.GetBinlogs()))
	for i := 0; i < 10; i++ {
		assert.Equal(t, unmarshaledSegmentInfo.GetBinlogs()[0].Binlogs[i].LogPath, unmarshaledSegmentInfoCompressed.GetBinlogs()[0].Binlogs[i].LogPath)
	}

	// test compress erorr path
	fakeBinlogs := make([]*datapb.Binlog, 1)
	fakeBinlogs[0] = &datapb.Binlog{
		EntriesNum: 10000,
		LogPath:    "test",
	}
	fieldBinLogs := make([]*datapb.FieldBinlog, 1)
	fieldBinLogs[0] = &datapb.FieldBinlog{
		FieldID: 106,
		Binlogs: fakeBinlogs,
	}
	segmentInfo1 := &datapb.SegmentInfo{
		Binlogs: fieldBinLogs,
	}
	err = CompressBinLogs(segmentInfo1.GetBinlogs(), segmentInfo1.GetDeltalogs(), segmentInfo1.GetStatslogs())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	fakeDeltalogs := make([]*datapb.Binlog, 1)
	fakeDeltalogs[0] = &datapb.Binlog{
		EntriesNum: 10000,
		LogPath:    "test",
	}
	fieldDeltaLogs := make([]*datapb.FieldBinlog, 1)
	fieldDeltaLogs[0] = &datapb.FieldBinlog{
		FieldID: 106,
		Binlogs: fakeBinlogs,
	}
	segmentInfo2 := &datapb.SegmentInfo{
		Deltalogs: fieldDeltaLogs,
	}
	err = CompressBinLogs(segmentInfo2.GetBinlogs(), segmentInfo2.GetDeltalogs(), segmentInfo2.GetStatslogs())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	fakeStatslogs := make([]*datapb.Binlog, 1)
	fakeStatslogs[0] = &datapb.Binlog{
		EntriesNum: 10000,
		LogPath:    "test",
	}
	fieldStatsLogs := make([]*datapb.FieldBinlog, 1)
	fieldStatsLogs[0] = &datapb.FieldBinlog{
		FieldID: 106,
		Binlogs: fakeBinlogs,
	}
	segmentInfo3 := &datapb.SegmentInfo{
		Statslogs: fieldDeltaLogs,
	}
	err = CompressBinLogs(segmentInfo3.GetBinlogs(), segmentInfo3.GetDeltalogs(), segmentInfo3.GetStatslogs())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	// test decompress error invalid Type
	// should not happen
	fakeBinlogs = make([]*datapb.Binlog, 1)
	fakeBinlogs[0] = &datapb.Binlog{
		EntriesNum: 10000,
		LogPath:    "",
		LogID:      1,
	}
	fieldBinLogs = make([]*datapb.FieldBinlog, 1)
	fieldBinLogs[0] = &datapb.FieldBinlog{
		FieldID: 106,
		Binlogs: fakeBinlogs,
	}
	segmentInfo = &datapb.SegmentInfo{
		Binlogs: fieldBinLogs,
	}
	invaildType := storage.BinlogType(100)
	err = DecompressBinLog(invaildType, 1, 1, 1, segmentInfo.Binlogs)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}
