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
	"io"
	"math"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
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

func TestClusteringInitLOBCompactionContextDecodesAcrossPartitionBases(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	params := compaction.GenParams()
	task := &clusteringCompactionTask{
		collectionID: 10,
		partitionID:  20,
		plan: &datapb.CompactionPlan{
			Type: datapb.CompactionType_ClusteringCompaction,
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			}},
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
				SegmentID: 1,
				Manifest: packed.MarshalManifestPath(
					params.StorageConfig.GetRootPath()+"/files/insert_log/10/20/1", 1),
			}},
		},
		compactionParams: params,
	}

	require.NoError(t, task.initLOBCompactionContext(context.Background()))
	require.NotNil(t, task.lobContext)
	require.True(t, task.lobContext.ShouldRewriteAnyField())
	require.True(t, task.lobContext.DecodeTextFromSource)
	sourceConfigs, err := task.lobContext.GetSourceTextColumnConfigs(task.plan.GetSegmentBinlogs()[0].GetManifest())
	require.NoError(t, err)
	require.Len(t, sourceConfigs, 1)
	require.Contains(t, sourceConfigs[0].LobBasePath, "/files/insert_log/10/20/lobs/101")
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

	fBinlogs, _, _, _, manifest, _, _, err := s.initStorageV3Segments(10240, segmentID)
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

	fBinlogs, _, _, _, manifest, _, _, err := s.initStorageV3Segments(10240, segmentID)
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
		sizes := segment.GetStats().GetFieldLogicalSizes()
		s.EqualValues(segment.GetNumOfRows(), sizes.GetRowCount())
		s.EqualValues(segment.GetNumOfRows()*8, sizes.GetFields()[common.RowIDField].GetPayloadBytes())
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
	segmentStats *datapb.Statistics,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(CollectionID, PartitionID, segmentID)
	basePath := path.Join(rootPath, common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil, metacache.NewEmptySegmentStats())
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
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second))},
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
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	s.task = NewMixCompactionTask(context.Background(), s.mockBinlogIO, cm, plan, compaction.GenParams(), []int64{pk.FieldID})
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
		binlogs, _, _, _, manifest, _, _, err := s.initStorageV3Segments(1, segmentID, alloc)
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

func (s *MixCompactionTaskStorageV3Suite) TestMergeSortPreservesTextLobFiles() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.UseMergeSort.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.UseMergeSort.Key)
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key)

	schema := genTextLOBCompactionSchema()
	s.meta.Schema = schema
	s.task.plan.Schema = schema
	s.task.sortByFieldIDs = []int64{Int64Field}
	s.task.compactionParams = compaction.GenParams()

	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0, 2)

	totalSourceLobFiles := 0
	for _, segmentID := range []int64{10, 11} {
		binlogs, _, _, _, manifest, _, stats, err := s.initTextLOBStorageV3Segment(2, segmentID, alloc)
		s.Require().NoError(err)
		s.Require().NotEmpty(manifest)

		lobFiles, err := packed.GetManifestLobFiles(manifest, s.task.compactionParams.StorageConfig)
		s.Require().NoError(err)
		s.Require().NotEmpty(lobFiles)
		totalSourceLobFiles += len(lobFiles)

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID:      CollectionID,
			PartitionID:       PartitionID,
			SegmentID:         segmentID,
			FieldBinlogs:      storage.SortFieldBinlogs(binlogs),
			Deltalogs:         []*datapb.FieldBinlog{},
			StorageVersion:    storage.StorageV3,
			Manifest:          manifest,
			IsSorted:          true,
			FieldLogicalSizes: stats.GetFieldLogicalSizes(),
		})
	}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Len(result.GetSegments(), 1)

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(4, segment.GetNumOfRows())
	s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
	s.NotEmpty(segment.GetManifest())
	s.True(segment.GetIsSorted())
	s.EqualValues(4*(256+len("segment-10-row-0-")), segment.GetStats().GetFieldLogicalSizes().GetFields()[StringField].GetPayloadBytes())

	outputLobFiles, err := packed.GetManifestLobFiles(segment.GetManifest(), s.task.compactionParams.StorageConfig)
	s.Require().NoError(err)
	s.Len(outputLobFiles, totalSourceLobFiles)
	for _, lobFile := range outputLobFiles {
		s.EqualValues(StringField, lobFile.FieldID)
		s.Greater(lobFile.TotalRows, int64(0))
		s.Greater(lobFile.ValidRows, int64(0))
	}
}

func (s *MixCompactionTaskStorageV3Suite) initStorageV3Segments(rows int, segmentID int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	segmentStats *datapb.Statistics,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(CollectionID, PartitionID, segmentID)
	basePath := path.Join(rootPath, common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil, metacache.NewEmptySegmentStats())
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

func (s *MixCompactionTaskStorageV3Suite) initTextLOBStorageV3Segment(rows int, segmentID int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	segmentStats *datapb.Statistics,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(CollectionID, PartitionID, segmentID)
	basePath := path.Join(rootPath, common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil, nil)
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
		WithBatchRows(int64(rows)).
		WithInsertData(genTextLOBInsertData(rows, segmentID, s.meta.GetSchema()))
	schema := s.meta.GetSchema()
	fields := typeutil.GetAllFieldSchemas(schema)
	columnGroups := storagecommon.SplitColumns(fields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
	bw := syncmgr.NewBulkPackWriterV3(mc, schema, cm, alloc, packed.DefaultWriteBufferSize, 0, &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}, columnGroups, manifestPath)
	return bw.Write(context.Background(), pack)
}

func genTextLOBCompactionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "text_lob_schema",
		Description: "text_lob_schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "Timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      Int64Field,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  StringField,
				Name:     "text",
				DataType: schemapb.DataType_Text,
			},
			{
				FieldID:     FloatVectorField,
				Name:        "vector",
				Description: "float_vector",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
		},
	}
}

// The merge seeds its readers in [1000, 9] order but emits [9, 1000]. Each
// source is complete in one distinct output. Its own body total must follow
// its emitted row even though both external references occupy the same bytes.
func (s *MixCompactionTaskStorageV3Suite) TestMergeSortRoutesReusedTextToActualOutputs() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key)
	schema := genTextLOBCompactionSchema()
	s.meta.Schema = schema
	plan := s.task.plan
	plan.Schema, plan.SegmentBinlogs, plan.TotalRows, plan.MaxSize = schema, nil, 2, 1
	s.task.sortByFieldIDs = []int64{Int64Field}
	s.task.compactionParams = compaction.GenParams()
	s.task.compactionParams.UseMergeSort, s.task.compactionParams.BinLogMaxSize = true, 1
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	expected := make(map[int64]uint64)
	for _, id := range []int64{1000, 9} {
		_, _, _, _, manifest, _, stats, err := s.initTextLOBStorageV3Segment(1, id, alloc)
		s.Require().NoError(err)
		payload := uint64(len(fmt.Sprintf("segment-%d-row-0-", id)) + 256)
		s.Require().EqualValues(payload, stats.GetFieldLogicalSizes().GetFields()[StringField].GetPayloadBytes())
		expected[id*100] = payload
		// Missing group-size estimates allow REUSE despite the real rotation.
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID: CollectionID, PartitionID: PartitionID, SegmentID: id,
			StorageVersion: storage.StorageV3, Manifest: manifest, IsSorted: true,
			FieldLogicalSizes: stats.GetFieldLogicalSizes(),
		})
	}
	cfg := s.task.compactionParams.StorageConfig
	lobDir := path.Join(storage.SegmentPartitionBasePath(cfg.GetRootPath(), CollectionID, PartitionID), "lobs", fmt.Sprint(StringField))
	s.Require().NoError(os.Rename(lobDir, lobDir+".unavailable"))
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), 2)
	for _, out := range result.GetSegments() {
		cols, rows := readAllColumns(s.T(), cfg, out.GetManifest(), &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{typeutil.GetField(schema, Int64Field)}})
		s.Require().Equal(1, rows)
		pk := cols[Int64Field][0].(int64)
		s.Require().Contains(expected, pk)
		sizes := out.GetStats().GetFieldLogicalSizes()
		s.EqualValues(1, sizes.GetRowCount())
		s.EqualValues(expected[pk], sizes.GetFields()[StringField].GetPayloadBytes())
		delete(expected, pk)
	}
	s.Empty(expected)
}

func genTextLOBInsertData(rows int, seed int64, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	ts := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
	for i := 0; i < rows; i++ {
		pk := seed*100 + int64(i)
		_ = buf.Append(map[int64]interface{}{
			common.RowIDField:     pk,
			common.TimeStampField: ts,
			Int64Field:            pk,
			StringField:           fmt.Sprintf("segment-%d-row-%d-%s", seed, i, strings.Repeat("x", 256)),
			FloatVectorField:      []float32{float32(i), 1, 2, 3},
		})
	}
	return []*storage.InsertData{buf}
}

// The V3 source has four rows. A delete and a TTL expiration remove two.
// Each surviving row is written to a separate segment under REWRITE_ALL.
func TestCompactionRewrittenTextSizesAfterDeleteTTLAndSplit(t *testing.T) {
	for _, kind := range []datapb.CompactionType{datapb.CompactionType_MixCompaction, datapb.CompactionType_ClusteringCompaction} {
		t.Run(kind.String(), func(t *testing.T) {
			setupBumpUTEnv(t)
			const nameID, bodyID = int64(104), int64(105)
			names := []any{"猫", "", nil, "ab"}
			bodies := []string{"x", strings.Repeat("y", 65536), strings.Repeat("z", 90000), "expired"}
			now := time.Now()
			rowTime := func(i int) uint64 {
				if i == 3 {
					return tsoutil.ComposeTSByTime(now.Add(-2 * time.Hour))
				}
				return tsoutil.ComposeTSByTime(now)
			}
			name := &schemapb.FieldSchema{
				FieldID: nameID, Name: "name", DataType: schemapb.DataType_VarChar, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "32"}},
			}
			body := &schemapb.FieldSchema{FieldID: bodyID, Name: "body", DataType: schemapb.DataType_Text}
			fix := buildBumpFixture(t, withRows(4), withoutSourceText(), withSourceFields(name, body), withTextLOBSource(bodyID),
				withCollectionTTL(time.Hour), withRowTs(rowTime), withDeletedPKs(map[any]uint64{int64(3): rowTime(2) + 1}),
				withFillValue(func(i int, _ uint64, v map[int64]any) {
					v[bumpFxPKField], v[nameID], v[bodyID] = int64(i+1), names[i], bodies[i]
				}))
			input := fix.segment.GetFieldLogicalSizes()
			require.EqualValues(t, 4, input.GetRowCount())
			require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: 5, ValidityBits: 4}, input.GetFields()[nameID])
			require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: 155544}, input.GetFields()[bodyID])

			fix.task.plan.MaxSize, fix.task.compactionParams.BinLogMaxSize = 1, 1
			var result *datapb.CompactionPlanResult
			var err error
			if kind == datapb.CompactionType_ClusteringCompaction {
				task := newClusteringTextTask(fix)
				task.currentTime = now
				result, err = task.Compact()
				require.NoError(t, err)
				require.Len(t, task.clusterBuffers, 1, "both segments must come from the same bucket")
				require.Equal(t, compaction.LOBStrategyRewriteAll, task.lobContext.GetStrategy(bodyID))
			} else {
				plan, params := fix.task.plan, fix.task.compactionParams
				plan.Type, params.UseMergeSort, params.MaxSegmentMergeSort = kind, true, 16
				fix.segment.IsSorted = true
				task := NewMixCompactionTask(context.Background(), binlogio.NewBinlogIO(fix.task.chunkManager), fix.task.chunkManager, plan, params, []int64{bumpFxPKField})
				task.currentTime = now
				result, err = task.Compact()
				require.NoError(t, err)
				require.Equal(t, compaction.LOBStrategyRewriteAll, task.lobContext.GetStrategy(bodyID))
			}
			outputs := result.GetSegments()
			require.Len(t, outputs, 2)
			require.NotEqual(t, outputs[0].GetSegmentID(), outputs[1].GetSegmentID())
			partitionBase := storage.SegmentPartitionBasePath(fix.cfg.GetRootPath(), CollectionID, PartitionID)
			textConfigs := []packed.TextColumnConfig{{FieldID: bodyID, LobBasePath: path.Join(partitionBase, "lobs", fmt.Sprint(bodyID))}}
			for i, out := range outputs {
				require.EqualValues(t, 1, out.GetNumOfRows())
				reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), out.GetManifest(), fix.targetSchema, textConfigs,
					storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fix.cfg))
				require.NoError(t, err)
				rec, err := reader.Next()
				require.NoError(t, err)
				require.Equal(t, 1, rec.Len())
				require.EqualValues(t, i+1, rec.Column(bumpFxPKField).(*array.Int64).Value(0))
				require.Equal(t, names[i], rec.Column(nameID).(*array.String).Value(0))
				require.Equal(t, bodies[i], rec.Column(bodyID).(*array.String).Value(0))
				_, err = reader.Next()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, reader.Close())
				sizes := out.GetStats().GetFieldLogicalSizes()
				require.EqualValues(t, 1, sizes.GetRowCount())
				require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: uint64(len(names[i].(string))), ValidityBits: 1}, sizes.GetFields()[nameID])
				require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: uint64(len(bodies[i]))}, sizes.GetFields()[bodyID])
			}
		})
	}
}

// Run real reference-preserving compactions after making every LOB body
// unavailable. Neither exact inheritance nor an unknown result needs to open
// a body. A stale estimate of one output is not proof that the rows stayed
// together, and a surviving 24-byte reference is not a 24-byte payload.
func TestCompactionReusedTextLogicalSizes(t *testing.T) {
	const textID = int64(105)
	for _, tc := range []struct {
		name        string
		kind        datapb.CompactionType
		mergeSort   bool
		deleteFirst bool
		split       bool
		keepInline  bool
	}{
		{name: "mix whole", kind: datapb.CompactionType_MixCompaction},
		{name: "mix deleted prefix", kind: datapb.CompactionType_MixCompaction, deleteFirst: true},
		{name: "merge whole", kind: datapb.CompactionType_MixCompaction, mergeSort: true},
		{name: "merge actual split", kind: datapb.CompactionType_MixCompaction, mergeSort: true, split: true},
		{name: "merge remaining inline", kind: datapb.CompactionType_MixCompaction, mergeSort: true, deleteFirst: true, keepInline: true},
		{name: "sort whole", kind: datapb.CompactionType_SortCompaction},
		{name: "sort deleted prefix", kind: datapb.CompactionType_SortCompaction, deleteFirst: true},
		{name: "schema whole", kind: datapb.CompactionType_BumpSchemaVersionCompaction},
		{name: "schema deleted prefix", kind: datapb.CompactionType_BumpSchemaVersionCompaction, deleteFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupBumpUTEnv(t)
			// Both large bodies must be external irrespective of the boundary
			// convention at exactly the default threshold (65,536 bytes).
			paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "100")
			defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key)
			bodies := []string{strings.Repeat("x", 90000), strings.Repeat("y", 65536)}
			if tc.keepInline {
				bodies[1] = "猫"
			}
			var deletes map[any]uint64
			if tc.deleteFirst {
				deletes = map[any]uint64{int64(0): bumpFxTS(0) + 1}
			}
			fix := buildBumpFixture(t, withRows(2), withTextLOBSource(textID), withDeletedPKs(deletes),
				withTargetDroppedField(bumpFxTextField),
				withSourceFields(&schemapb.FieldSchema{FieldID: textID, Name: "body", DataType: schemapb.DataType_Text, Nullable: true}),
				withFillValue(func(i int, _ uint64, v map[int64]any) { v[textID] = bodies[i] }))
			refs := readTextRefs(t, fix, fix.sourceManifest, textID)
			require.Equal(t, byte(1), refs[0][0])
			require.Equal(t, tc.keepInline, refs[1][0] == 0)
			sourceSize := uint64(len(bodies[0]) + len(bodies[1]))
			require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: sourceSize, ValidityBits: 2}, fix.segment.GetFieldLogicalSizes().GetFields()[textID])
			lobDir := path.Join(storage.SegmentPartitionBasePath(fix.cfg.GetRootPath(), CollectionID, PartitionID), "lobs", fmt.Sprint(textID))
			require.NoError(t, os.Rename(lobDir, lobDir+".unavailable"))

			plan, params := fix.task.plan, fix.task.compactionParams
			plan.Type, params.UseMergeSort, params.MaxSegmentMergeSort = tc.kind, tc.mergeSort, 16
			fix.segment.IsSorted = tc.mergeSort
			if tc.split {
				// A V3 source recovered without FieldBinlogs has no size
				// estimate; the actual batch and writer boundaries split it.
				fix.segment.FieldBinlogs = nil
				plan.MaxSize, params.BinLogMaxSize = 1, 1
			}
			var task Compactor = fix.task
			switch tc.kind {
			case datapb.CompactionType_MixCompaction:
				task = NewMixCompactionTask(context.Background(), binlogio.NewBinlogIO(fix.task.chunkManager), fix.task.chunkManager, plan, params, []int64{bumpFxPKField})
			case datapb.CompactionType_SortCompaction:
				task = NewSortCompactionTask(context.Background(), fix.task.chunkManager, plan, params, []int64{bumpFxPKField})
			}
			result, err := task.Compact()
			require.NoError(t, err)
			wantOutputs := 1
			if tc.split {
				wantOutputs = 2
			}
			require.Len(t, result.GetSegments(), wantOutputs)
			var rowCount int64
			for _, out := range result.GetSegments() {
				rowCount += out.GetNumOfRows()
				sizes := out.GetStats().GetFieldLogicalSizes()
				require.Equal(t, out.GetNumOfRows(), sizes.GetRowCount())
				require.EqualValues(t, sizes.GetRowCount()*8, sizes.GetFields()[bumpFxPKField].GetPayloadBytes())
				require.NotContains(t, sizes.GetFields(), bumpFxTextField) // dropped from target schema
				switch {
				case tc.keepInline:
					require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: 3, ValidityBits: 1}, sizes.GetFields()[textID])
				case tc.deleteFirst || tc.split:
					require.NotContains(t, sizes.GetFields(), textID)
				default:
					require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: sourceSize, ValidityBits: 2}, sizes.GetFields()[textID])
				}
				for pk, ref := range readTextRefs(t, fix, out.GetManifest(), textID) {
					require.Equal(t, refs[pk], ref)
				}
			}
			require.EqualValues(t, len(bodies)-len(deletes), rowCount)
		})
	}
}

func newClusteringTextTask(fixture *bumpFixture) *clusteringCompactionTask {
	plan := proto.Clone(fixture.task.plan).(*datapb.CompactionPlan)
	plan.Type = datapb.CompactionType_ClusteringCompaction
	plan.ClusteringKeyField = bumpFxPKField
	plan.PreferSegmentRows, plan.MaxSegmentRows = 128, 128
	plan.AnalyzeResultPath = fixture.cfg.RootPath + "/analyze_stats/999"
	return NewClusteringCompactionTask(context.Background(), binlogio.NewBinlogIO(fixture.task.chunkManager), plan, fixture.task.compactionParams)
}

func TestClusteringTextRewriteAcrossPartitionNamespaces(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	const rows = 6
	fixture := buildBumpFixture(t, withRows(rows),
		withSourceFields(&schemapb.FieldSchema{FieldID: textID, Name: "text_lob", DataType: schemapb.DataType_Text}),
		withFillValue(func(i int, _ uint64, values map[int64]any) { values[textID] = bumpFxLobText(i) }),
		withTextLOBSource(textID), withLegacySourceNamespace())
	task := newClusteringTextTask(fixture)
	result, err := task.Compact()
	require.NoError(t, err)
	require.Len(t, result.Segments, 1)
	segment := result.Segments[0]
	require.EqualValues(t, rows, segment.NumOfRows)
	base, _, err := packed.UnmarshalManifestPath(segment.Manifest)
	require.NoError(t, err)
	require.Equal(t, storage.SegmentManifestBasePath(fixture.cfg.RootPath, CollectionID, PartitionID, segment.SegmentID), base)
	lobFiles, err := packed.GetManifestLobFiles(segment.Manifest, fixture.cfg)
	require.NoError(t, err)
	require.NotEmpty(t, lobFiles)
	require.NotNil(t, task.lobContext)
	require.True(t, task.lobContext.DecodeTextFromSource)
	configs, err := task.lobContext.GetSourceTextColumnConfigs(segment.Manifest)
	require.NoError(t, err)
	reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), segment.Manifest, task.plan.Schema, configs,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fixture.cfg))
	require.NoError(t, err)
	defer reader.Close()
	readRows := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for row := 0; row < record.Len(); row++ {
			require.EqualValues(t, readRows, record.Column(bumpFxPKField).(*array.Int64).Value(row))
			require.Equal(t, bumpFxLobText(readRows), record.Column(textID).(*array.String).Value(row))
			readRows++
		}
	}
	require.Equal(t, rows, readRows)
}

func TestClusteringTextAddedAfterAllSourceSegments(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	const rows = 6
	fixture := buildBumpFixture(t, withRows(rows), withLegacySourceNamespace(),
		withTargetAddedField(&schemapb.FieldSchema{FieldID: textID, Name: "added_text", DataType: schemapb.DataType_Text, Nullable: true}))
	task := newClusteringTextTask(fixture)
	task.compactionParams.BinLogMaxSize = 1 // Each source row produces a separate all-null TEXT batch.
	result, err := task.Compact()
	require.NoError(t, err)
	require.True(t, task.lobContext.DecodeTextFromSource)
	require.Len(t, result.Segments, 1)
	segment := result.Segments[0]
	require.EqualValues(t, rows, segment.NumOfRows)
	configs, err := task.lobContext.GetSourceTextColumnConfigs(segment.Manifest)
	require.NoError(t, err)
	reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), segment.Manifest, task.plan.Schema, configs,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fixture.cfg))
	require.NoError(t, err)
	defer reader.Close()
	readRows := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, record.Len(), record.Column(textID).NullN())
		for row := 0; row < record.Len(); row++ {
			require.EqualValues(t, readRows, record.Column(bumpFxPKField).(*array.Int64).Value(row))
			readRows++
		}
	}
	require.Equal(t, rows, readRows)
}
