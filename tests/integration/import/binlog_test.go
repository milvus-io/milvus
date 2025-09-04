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

package importv2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type DMLGroup struct {
	insertRowNums []int
	deleteRowNums []int
}

type SourceCollectionInfo struct {
	collectionID   int64
	partitionID    int64
	l0SegmentIDs   []int64
	SegmentIDs     []int64
	insertedIDs    *schemapb.IDs
	storageVersion int
}

func (s *BulkInsertSuite) PrepareSourceCollection(dim int, dmlGroup *DMLGroup) *SourceCollectionInfo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	collectionName := "TestBinlogImport_A_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataTypeWithStruct(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	s.NoError(merr.CheckRPCCall(showCollectionsResp, err))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	showPartitionsResp, err := c.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(showPartitionsResp, err))
	log.Info("ShowPartitions result", zap.Any("showPartitionsResp", showPartitionsResp))

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	createIndexResult, err := c.MilvusClient.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.StructSubFloatVecField,
		IndexName:      "array_of_vector_index",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexEmbListHNSW, metric.MaxSim),
	})
	s.NoError(err)
	s.Require().Equal(createIndexResult.GetErrorCode(), commonpb.ErrorCode_Success)
	s.WaitForIndexBuilt(context.TODO(), collectionName, integration.StructSubFloatVecField)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	const delBatch = 2
	var (
		totalInsertRowNum = 0
		totalDeleteRowNum = 0
		totalInsertedIDs  = &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 0),
				},
			},
		}
	)

	for i := range dmlGroup.insertRowNums {
		insRow := dmlGroup.insertRowNums[i]
		delRow := dmlGroup.deleteRowNums[i]
		totalInsertRowNum += insRow
		totalDeleteRowNum += delRow

		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, insRow, dim)
		structColumn := integration.NewStructArrayFieldData(schema.StructArrayFields[0], integration.StructArrayField, insRow, dim)
		hashKeys := integration.GenerateHashKeys(insRow)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn, structColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(insRow),
		})
		s.NoError(merr.CheckRPCCall(insertResult, err))
		insertedIDs := insertResult.GetIDs()
		totalInsertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data = append(
			totalInsertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data, insertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data...)

		// delete
		beginIndex := 0
		for j := 0; j < delBatch; j++ {
			if delRow == 0 {
				continue
			}
			delCnt := delRow / delBatch
			idBegin := insertedIDs.GetIntId().GetData()[beginIndex]
			idEnd := insertedIDs.GetIntId().GetData()[beginIndex+delCnt-1]
			deleteResult, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           fmt.Sprintf("%d <= %s <= %d", idBegin, integration.Int64Field, idEnd),
			})
			s.NoError(merr.CheckRPCCall(deleteResult, err))
			beginIndex += delCnt
		}

		// flush
		flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			CollectionNames: []string{collectionName},
		})
		s.NoError(merr.CheckRPCCall(flushResp, err))
		segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[collectionName]
		s.True(has)
		s.WaitForFlush(ctx, ids, flushTs, "", collectionName)
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)
		for _, segment := range segments {
			log.Info("ShowSegments result", zap.String("segment", segment.String()))
		}
	}

	// check segments
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	l0Segments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() == datapb.SegmentLevel_L0
	})
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() == datapb.SegmentLevel_L1
	})
	// check l0 segments
	if totalDeleteRowNum > 0 {
		s.True(len(l0Segments) > 0)
	}

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	expectResult := nq * topk
	if expectResult > totalInsertRowNum-totalDeleteRowNum {
		expectResult = totalInsertRowNum - totalDeleteRowNum
	}
	s.Equal(expectResult, len(searchResult.GetResults().GetScores()))

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(totalInsertRowNum-totalDeleteRowNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, totalInsertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	expectCount := 10
	if dmlGroup.deleteRowNums[0] >= 10 {
		expectCount = 0
	}
	s.Equal(expectCount, count)

	// get collectionID and partitionID
	collectionID := showCollectionsResp.GetCollectionIds()[0]
	partitionID := showPartitionsResp.GetPartitionIDs()[0]

	storageVersion := 0
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = 2
	}

	return &SourceCollectionInfo{
		collectionID: collectionID,
		partitionID:  partitionID,
		l0SegmentIDs: lo.Map(l0Segments, func(segment *datapb.SegmentInfo, _ int) int64 {
			return segment.GetID()
		}),
		SegmentIDs: lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) int64 {
			return segment.GetID()
		}),
		insertedIDs:    totalInsertedIDs,
		storageVersion: storageVersion,
	}
}

func (s *BulkInsertSuite) runBinlogTest(dmlGroup *DMLGroup) {
	const dim = 128

	sourceCollectionInfo := s.PrepareSourceCollection(dim, dmlGroup)
	collectionID := sourceCollectionInfo.collectionID
	partitionID := sourceCollectionInfo.partitionID
	l0SegmentIDs := sourceCollectionInfo.l0SegmentIDs
	segmentIDs := sourceCollectionInfo.SegmentIDs
	insertedIDs := sourceCollectionInfo.insertedIDs

	log.Info("prepare source collection done",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64s("segments", segmentIDs),
		zap.Int64s("l0 segments", l0SegmentIDs))

	c := s.Cluster
	ctx := c.GetContext()

	totalInsertRowNum := lo.SumBy(dmlGroup.insertRowNums, func(num int) int {
		return num
	})
	totalDeleteRowNum := lo.SumBy(dmlGroup.deleteRowNums, func(num int) int {
		return num
	})

	collectionName := "TestBinlogImport_B_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))
	newCollectionID := describeCollectionResp.GetCollectionID()

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// binlog import
	files := make([]*internalpb.ImportFile, 0)
	for _, segmentID := range segmentIDs {
		files = append(files, &internalpb.ImportFile{Paths: []string{fmt.Sprintf("%s/insert_log/%d/%d/%d",
			s.Cluster.RootPath(), collectionID, partitionID, segmentID)}})
	}

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "backup", Value: "true"},
			{Key: importutilv2.StorageVersion, Value: strconv.Itoa(sourceCollectionInfo.storageVersion)},
		},
	})
	s.NoError(merr.CheckRPCCall(importResp, err))
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID
	})
	log.Info("Show segments", zap.Any("segments", segments))
	s.Equal(2, len(segments))
	segment, ok := lo.Find(segments, func(segment *datapb.SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed
	})
	s.True(ok)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.True(len(segment.GetBinlogs()) > 0)
	s.NoError(CheckLogID(segment.GetBinlogs()))
	s.True(len(segment.GetDeltalogs()) == 0)
	s.NoError(CheckLogID(segment.GetDeltalogs()))
	s.True(len(segment.GetStatslogs()) > 0)
	s.NoError(CheckLogID(segment.GetStatslogs()))

	// l0 import
	if totalDeleteRowNum > 0 {
		files = make([]*internalpb.ImportFile, 0)
		for _, segmentID := range l0SegmentIDs {
			files = append(files, &internalpb.ImportFile{Paths: []string{fmt.Sprintf("%s/delta_log/%d/%d/%d",
				s.Cluster.RootPath(), collectionID, common.AllPartitionsID, segmentID)}})
		}
		importResp, err = c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: collectionName,
			Files:          files,
			Options: []*commonpb.KeyValuePair{
				{Key: "l0_import", Value: "true"},
				{Key: importutilv2.StorageVersion, Value: strconv.Itoa(sourceCollectionInfo.storageVersion)},
			},
		})
		s.NoError(merr.CheckRPCCall(importResp, err))
		log.Info("Import result", zap.Any("importResp", importResp))

		jobID = importResp.GetJobID()
		err = WaitForImportDone(ctx, c, jobID)
		s.NoError(err)

		segments, err = c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)
		log.Info("Show segments", zap.Any("segments", segments))
		l0Segments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetLevel() == datapb.SegmentLevel_L0
		})
		s.Equal(1, len(l0Segments))
		segment = l0Segments[0]
		s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
		s.Equal(common.AllPartitionsID, segment.GetPartitionID())
		s.True(len(segment.GetBinlogs()) == 0)
		s.True(len(segment.GetDeltalogs()) > 0)
		s.NoError(CheckLogID(segment.GetDeltalogs()))
		s.True(len(segment.GetStatslogs()) == 0)
	}

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	expectResult := nq * topk
	if expectResult > totalInsertRowNum-totalDeleteRowNum {
		expectResult = totalInsertRowNum - totalDeleteRowNum
	}
	s.Equal(expectResult, len(searchResult.GetResults().GetScores()))
	// check ids from collectionA, because during binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	insertedIDsMap := lo.SliceToMap(insertedIDs.GetIntId().GetData(), func(id int64) (int64, struct{}) {
		return id, struct{}{}
	})
	for _, id := range searchResult.GetResults().GetIds().GetIntId().GetData() {
		_, ok := insertedIDsMap[id]
		s.True(ok)
	}

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             expr,
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(totalInsertRowNum-totalDeleteRowNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, insertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             expr,
		OutputFields:     []string{},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	expectCount := 10
	if dmlGroup.deleteRowNums[0] >= 10 {
		expectCount = 0
	}
	s.Equal(expectCount, count)
}

func (s *BulkInsertSuite) TestInvalidInput() {
	const dim = 128
	c := s.Cluster
	ctx := c.GetContext()

	collectionName := "TestBinlogImport_InvalidInput_" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))

	// binlog import
	files := []*internalpb.ImportFile{
		{
			Paths: []string{"invalid-path", "invalid-path", "invalid-path"},
		},
	}
	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "backup", Value: "true"},
		},
	})
	err = merr.CheckRPCCall(importResp, err)
	s.True(strings.Contains(err.Error(), "too many input paths for binlog import"))
	s.Error(err)
	log.Info("Import result", zap.Any("importResp", importResp))
}

func (s *BulkInsertSuite) TestBinlogImport() {
	dmlGroup := &DMLGroup{
		insertRowNums: []int{500, 500, 500},
		deleteRowNums: []int{300, 300, 300},
	}
	s.runBinlogTest(dmlGroup)
}

func (s *BulkInsertSuite) TestBinlogImport_NoDelete() {
	dmlGroup := &DMLGroup{
		insertRowNums: []int{500, 500, 500},
		deleteRowNums: []int{0, 0, 0},
	}
	s.runBinlogTest(dmlGroup)
}

func (s *BulkInsertSuite) TestBinlogImport_Partial_0_Rows_Segment() {
	dmlGroup := &DMLGroup{
		insertRowNums: []int{500, 500, 500},
		deleteRowNums: []int{500, 300, 0},
	}
	s.runBinlogTest(dmlGroup)
}

func (s *BulkInsertSuite) TestBinlogImport_All_0_Rows_Segment() {
	dmlGroup := &DMLGroup{
		insertRowNums: []int{500, 500, 500},
		deleteRowNums: []int{500, 500, 500},
	}
	s.runBinlogTest(dmlGroup)
}
