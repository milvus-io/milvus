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

package compactor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestClusteringCompactionTaskStorageV2Suite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskStorageV2Suite))
}

type ClusteringCompactionTaskStorageV2Suite struct {
	ClusteringCompactionTaskSuite
}

func (s *ClusteringCompactionTaskStorageV2Suite) SetupTest() {
	s.setupTest()
	paramtable.Get().Save("common.storageType", "local")
	paramtable.Get().Save("common.storage.enableV2", "true")
	initcore.InitStorageV2FileSystem(paramtable.Get())
	refreshPlanParams(s.plan)
}

func (s *ClusteringCompactionTaskStorageV2Suite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
	paramtable.Get().Reset("common.storageType")
	paramtable.Get().Reset("common.storage.enableV2")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "insert_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "delta_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "stats_log")
}

func (s *ClusteringCompactionTaskStorageV2Suite) TestScalarCompactionNormal() {
	s.preparScalarCompactionNormalTask()
	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(len(compactionResult.GetSegments()), len(compactionResult.GetSegments()))

	for i := 0; i < len(compactionResult.GetSegments()); i++ {
		seg := compactionResult.GetSegments()[i]
		s.EqualValues(1, len(seg.InsertLogs))
	}

	s.EqualValues(10239,
		lo.SumBy(compactionResult.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
			return seg.GetNumOfRows()
		}),
	)
}

func (s *ClusteringCompactionTaskStorageV2Suite) TestScalarCompactionNormal_V2ToV2Format() {
	var segmentID int64 = 1001

	fBinlogs, deltalogs, _, _, _, err := s.initStorageV2Segments(10240, segmentID)
	s.NoError(err)

	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltalogs.GetBinlogs()[0].GetLogPath()}).
		Return([][]byte{dblobs.GetValue()}, nil).Once()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:      segmentID,
			FieldBinlogs:   lo.Values(fBinlogs),
			Deltalogs:      []*datapb.FieldBinlog{deltalogs},
			StorageVersion: storage.StorageV2,
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // max segment size = 1GB, we won't touch this value
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   101,
	}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 200,
		End:   2000,
	}

	compactionResultV2, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(5, len(s.task.clusterBuffers))
	s.Equal(5, len(compactionResultV2.GetSegments()))

	totalRowNum := int64(0)
	statsRowNum := int64(0)
	for _, seg := range compactionResultV2.GetSegments() {
		s.Equal(1, len(seg.GetInsertLogs()))
		s.Equal(1, len(seg.GetField2StatslogPaths()))
		totalRowNum += seg.GetNumOfRows()
		statsRowNum += seg.GetField2StatslogPaths()[0].GetBinlogs()[0].GetEntriesNum()
	}
	s.Equal(totalRowNum, statsRowNum)
	s.EqualValues(10239,
		lo.SumBy(compactionResultV2.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
			return seg.GetNumOfRows()
		}),
	)
}

func (s *ClusteringCompactionTaskStorageV2Suite) TestScalarCompactionNormal_V2ToV1Format() {
	paramtable.Get().Save("common.storage.enableV2", "false")
	refreshPlanParams(s.plan)

	var segmentID int64 = 1001

	fBinlogs, deltalogs, _, _, _, err := s.initStorageV2Segments(10240, segmentID)
	s.NoError(err)

	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltalogs.GetBinlogs()[0].GetLogPath()}).
		Return([][]byte{dblobs.GetValue()}, nil).Once()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:      segmentID,
			FieldBinlogs:   lo.Values(fBinlogs),
			Deltalogs:      []*datapb.FieldBinlog{deltalogs},
			StorageVersion: storage.StorageV2,
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // max segment size = 1GB, we won't touch this value
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   101,
	}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 200,
		End:   2000,
	}

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(5, len(s.task.clusterBuffers))
	s.Equal(5, len(compactionResult.GetSegments()))
	totalBinlogNum := 0
	totalRowNum := int64(0)
	for _, fb := range compactionResult.GetSegments()[0].GetInsertLogs() {
		for _, b := range fb.GetBinlogs() {
			totalBinlogNum++
			if fb.GetFieldID() == 100 {
				totalRowNum += b.GetEntriesNum()
			}
		}
	}
	statsBinlogNum := 0
	statsRowNum := int64(0)
	for _, sb := range compactionResult.GetSegments()[0].GetField2StatslogPaths() {
		for _, b := range sb.GetBinlogs() {
			statsBinlogNum++
			statsRowNum += b.GetEntriesNum()
		}
	}
	s.Equal(1, totalBinlogNum/len(s.plan.Schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)
	s.EqualValues(10239,
		lo.SumBy(compactionResult.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
			return seg.GetNumOfRows()
		}),
	)
}

func (s *ClusteringCompactionTaskStorageV2Suite) TestCompactionWithBM25Function() {
	// 8 + 8 + 8 + 7 + 8 = 39
	// 39*1024 = 39936
	// plus buffer on null bitsets etc., let's make it 45000
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "45000")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	refreshPlanParams(s.plan)
	s.prepareCompactionWithBM25FunctionTask()
	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)

	s.Equal(5, len(compactionResult.GetSegments()))

	for i := 0; i < len(compactionResult.GetSegments()); i++ {
		seg := compactionResult.GetSegments()[i]
		s.Equal(1, len(seg.InsertLogs))
		s.Equal(1, len(seg.Bm25Logs))
	}
}

func (s *ClusteringCompactionTaskStorageV2Suite) TestScalarCompactionNormalByMemoryLimit() {
	s.prepareScalarCompactionNormalByMemoryLimit()
	// 8+8+8+4+7+4*4=51
	// 51*1024 = 52224
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "52223")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)
	refreshPlanParams(s.plan)

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(2, len(s.task.clusterBuffers))
	s.Equal(2, len(compactionResult.GetSegments()))
	segment := compactionResult.GetSegments()[0]
	s.Equal(1, len(segment.InsertLogs))
	s.Equal(1, len(segment.Field2StatslogPaths))
}

func (s *ClusteringCompactionTaskStorageV2Suite) initStorageV2Segments(rows int, segmentID int64) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(int64(rows))(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().Schema().Return(genCollectionSchema()).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	deleteData := storage.NewDeleteData([]storage.PrimaryKey{storage.NewInt64PrimaryKey(100)}, []uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)})
	pack := new(syncmgr.SyncPack).WithCollectionID(CollectionID).WithPartitionID(PartitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData(genInsertData(rows, segmentID, genCollectionSchema())).WithDeleteData(deleteData)
	bw := syncmgr.NewBulkPackWriterV2(mc, cm, s.mockAlloc, packed.DefaultWriteBufferSize, 0)
	return bw.Write(context.Background(), pack)
}

func genInsertData(size int, seed int64, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	for i := 0; i < size; i++ {
		buf.Append(genRow(int64(i)))
	}
	return []*storage.InsertData{buf}
}
