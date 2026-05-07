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
	"math"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestClusteringCompactionTaskStorageV3Suite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskStorageV3Suite))
}

type ClusteringCompactionTaskStorageV3Suite struct {
	ClusteringCompactionTaskSuite
}

func (s *ClusteringCompactionTaskStorageV3Suite) SetupTest() {
	s.setupTest()
}

func (s *ClusteringCompactionTaskStorageV3Suite) TearDownTest() {
	initcore.CleanArrowFileSystem()
	s.ClusteringCompactionTaskSuite.TearDownTest()
}

func (s *ClusteringCompactionTaskStorageV3Suite) TestScalarAnalyzeManifestWithRowID() {
	var segmentID int64 = 1001

	rootPath := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, rootPath)
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(rootPath)
	s.task.compactionParams = compaction.GenParams()

	fBinlogs, _, _, _, manifest, _, err := s.initStorageV3Segments(10240, segmentID)
	s.Require().NoError(err)
	s.Require().NotEmpty(manifest)

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID:   CollectionID,
			PartitionID:    PartitionID,
			SegmentID:      segmentID,
			FieldBinlogs:   storage.SortFieldBinlogs(fBinlogs),
			StorageVersion: storage.StorageV3,
			Manifest:       manifest,
			InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID),
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.MaxSize = 1024 * 1024 * 1024
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   101,
	}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 200,
		End:   2000,
	}

	err = s.task.init()
	s.Require().NoError(err)
	defer s.task.cleanUp(context.Background())

	analyzeResult, err := s.task.scalarAnalyzeSegment(context.Background(), s.task.plan.SegmentBinlogs[0])
	s.Require().NoError(err)
	s.Len(analyzeResult, 10240)
}

func (s *ClusteringCompactionTaskStorageV3Suite) TestScalarCompactionWithManifest() {
	var segmentID int64 = 1001

	rootPath := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, rootPath)
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(rootPath)
	s.task.compactionParams = compaction.GenParams()

	fBinlogs, _, _, _, manifest, _, err := s.initStorageV3Segments(10240, segmentID)
	s.Require().NoError(err)
	s.Require().NotEmpty(manifest)

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID:   CollectionID,
			PartitionID:    PartitionID,
			SegmentID:      segmentID,
			FieldBinlogs:   storage.SortFieldBinlogs(fBinlogs),
			StorageVersion: storage.StorageV3,
			Manifest:       manifest,
			InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID),
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.MaxSize = 1024 * 1024 * 1024
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
	s.Require().NotNil(compactionResult)
	s.Equal(5, len(compactionResult.GetSegments()))

	var totalRows int64
	for _, segment := range compactionResult.GetSegments() {
		totalRows += segment.GetNumOfRows()
		s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
		s.NotEmpty(segment.GetManifest())
		s.NotEmpty(segment.GetInsertLogs())
		s.Equal(1, len(segment.GetField2StatslogPaths()))
	}
	s.EqualValues(10239, totalRows)
}

func (s *ClusteringCompactionTaskStorageV3Suite) initStorageV3Segments(rows int, segmentID int64) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(CollectionID, PartitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil)
	metacache.UpdateNumOfRows(int64(rows))(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().GetSchema(mock.Anything).Return(genCollectionSchema()).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	deleteData := storage.NewDeleteData(
		[]storage.PrimaryKey{storage.NewInt64PrimaryKey(100)},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	pack := new(syncmgr.SyncPack).
		WithCollectionID(CollectionID).
		WithPartitionID(PartitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertData(rows, segmentID, genCollectionSchema())).
		WithDeleteData(deleteData)
	sch := genCollectionSchema()
	fields := typeutil.GetAllFieldSchemas(sch)
	columnGroups := storagecommon.SplitColumns(fields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
	bw := syncmgr.NewBulkPackWriterV3(mc, sch, cm, s.mockAlloc, packed.DefaultWriteBufferSize, 0, &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}, columnGroups, manifestPath)
	return bw.Write(context.Background(), pack)
}

func TestMixCompactionTaskStorageV3Suite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskStorageV3Suite))
}

type MixCompactionTaskStorageV3Suite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO
	meta         *etcdpb.CollectionMeta
	task         *mixCompactionTask
}

func (s *MixCompactionTaskStorageV3Suite) SetupTest() {
	rootPath := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, rootPath)
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(rootPath)

	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMeta()
	params, err := compaction.GenerateJSONParams(s.meta.GetSchema())
	if err != nil {
		panic(err)
	}

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:        CollectionID,
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		Type:                   datapb.CompactionType_MixCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
	}

	pk, err := typeutil.GetPrimaryFieldSchema(s.meta.GetSchema())
	s.Require().NoError(err)
	s.task = NewMixCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams(), []int64{pk.FieldID})
}

func (s *MixCompactionTaskStorageV3Suite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
}

func (s *MixCompactionTaskStorageV3Suite) TestCompactV3ManifestSegments() {
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segmentID := range []int64{10, 11} {
		binlogs, _, _, _, manifest, _, err := s.initStorageV3Segments(1, segmentID, alloc)
		s.Require().NoError(err)
		s.Require().NotEmpty(manifest)

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID:   CollectionID,
			PartitionID:    PartitionID,
			SegmentID:      segmentID,
			FieldBinlogs:   storage.SortFieldBinlogs(binlogs),
			StorageVersion: storage.StorageV3,
			Manifest:       manifest,
		})
	}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(2, segment.GetNumOfRows())
	s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
	s.NotEmpty(segment.GetManifest())
	s.NotEmpty(segment.GetInsertLogs())
	s.Equal(1, len(segment.GetField2StatslogPaths()))
	s.Empty(segment.GetDeltalogs())
}

func (s *MixCompactionTaskStorageV3Suite) initStorageV3Segments(rows int, segmentID int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(CollectionID, PartitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil)
	metacache.UpdateNumOfRows(int64(rows))(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().GetSchema(mock.Anything).Return(s.meta.GetSchema()).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	pack := new(syncmgr.SyncPack).
		WithCollectionID(CollectionID).
		WithPartitionID(PartitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(getInsertData(rows, segmentID, s.meta.GetSchema()))
	schema := s.meta.GetSchema()
	fields := typeutil.GetAllFieldSchemas(schema)
	columnGroups := storagecommon.SplitColumns(fields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
	bw := syncmgr.NewBulkPackWriterV3(mc, schema, cm, alloc, packed.DefaultWriteBufferSize, 0, &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}, columnGroups, manifestPath)
	return bw.Write(context.Background(), pack)
}
