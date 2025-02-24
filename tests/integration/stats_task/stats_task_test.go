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

package statstask

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type StatsTaskCheckerSuite struct {
	integration.MiniClusterSuite

	pkType     schemapb.DataType
	dbName     string
	dim        int
	batch      int
	batchCnt   int
	indexType  string
	metricType string

	cm storage.ChunkManager
}

func TestStatsTask(t *testing.T) {
	suite.Run(t, new(StatsTaskCheckerSuite))
}

func (s *StatsTaskCheckerSuite) initParams() {
	s.dbName = "default"
	s.dim = 128
	s.batch = 2000
	s.batchCnt = 5
	s.indexType = integration.IndexFaissIvfFlat
	s.metricType = metric.L2
	cm, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	s.NoError(err)
	s.cm = cm
}

func (s *StatsTaskCheckerSuite) TestStatsTaskChecker_Int64PK() {
	s.initParams()
	s.pkType = schemapb.DataType_Int64
	s.run()
}

func (s *StatsTaskCheckerSuite) TestStatsTaskChecker_VarcharPK() {
	s.initParams()
	s.pkType = schemapb.DataType_VarChar
	s.run()
}

func (s *StatsTaskCheckerSuite) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	collectionName := "TestStatsTask" + funcutil.GenRandomStr()

	var pkField *schemapb.FieldSchema
	if s.pkType == schemapb.DataType_VarChar {
		pkField = &schemapb.FieldSchema{
			FieldID:      100,
			Name:         "pk",
			IsPrimaryKey: true,
			Description:  "primary key",
			DataType:     schemapb.DataType_VarChar,
			TypeParams:   []*commonpb.KeyValuePair{{Key: "max_length", Value: "10000"}},
			AutoID:       false,
		}
	} else {
		pkField = &schemapb.FieldSchema{
			FieldID:      100,
			Name:         "pk",
			IsPrimaryKey: true,
			Description:  "primary key",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   []*commonpb.KeyValuePair{},
			AutoID:       false,
		}
	}

	varcharField := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "var",
		IsPrimaryKey: false,
		Description:  "test enable match",
		DataType:     schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "10000"},
		},
	}

	vectorField := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     integration.FloatVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: strconv.Itoa(s.dim)},
		},
	}

	schema := integration.ConstructSchema(collectionName, s.dim, false, pkField, varcharField, vectorField)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	insertCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("insert check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("insert report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeInsert, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RequestDataSizeKey])
				return
			}
		}
	}
	go insertCheckReport()
	// batch insert to generate some segments
	for i := 0; i < s.batchCnt; i++ {
		var pkColumn *schemapb.FieldData
		if s.pkType == schemapb.DataType_VarChar {
			stringData := make([]string, s.batch)
			for j := 0; j < s.batch; j++ {
				stringData[j] = fmt.Sprintf("%d", s.batch*(s.batchCnt-i)-j)
			}
			pkColumn = &schemapb.FieldData{
				Type:      schemapb.DataType_VarChar,
				FieldName: "pk",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
				FieldId: 100,
			}
		} else {
			intData := make([]int64, s.batch)
			for j := 0; j < s.batch; j++ {
				intData[j] = int64(s.batch*(s.batchCnt-i) - j)
			}
			pkColumn = &schemapb.FieldData{
				Type:      schemapb.DataType_VarChar,
				FieldName: "pk",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: intData,
							},
						},
					},
				},
				FieldId: 100,
			}
		}
		stringData := make([]string, s.batch)
		for j := 0; j < s.batch; j++ {
			stringData[j] = fmt.Sprintf("hello milvus with %d", s.batch*(s.batchCnt-i)-j)
		}
		varcharColumn := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "var",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: stringData,
						},
					},
				},
			},
			FieldId: 100,
		}
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, s.batch, s.dim)
		hashKeys := integration.GenerateHashKeys(s.batch)

		insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         s.dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn, varcharColumn, fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(s.batch),
		})
		s.NoError(err)
		s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

		// flush
		flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          s.dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[collectionName]
		s.True(has)

		segments, err := c.MetaWatcher.ShowSegments()
		s.NoError(err)
		s.NotEmpty(segments)
		for _, segment := range segments {
			log.Info("ShowSegments result", zap.String("segment", segment.String()))
		}
		s.WaitForFlush(ctx, ids, flushTs, s.dbName, collectionName)
	}

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, s.indexType, s.metricType),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	s.WaitForSortedSegmentLoaded(ctx, s.dbName, collectionName)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		if segment.GetIsSorted() && segment.GetState() != commonpb.SegmentState_Dropped {
			s.checkSegmentIsSorted(ctx, segment)
		}
	}
	// search
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, "",
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, s.metricType, params, nq, s.dim, topk, roundDecimal)

	searchCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("search check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("search report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeSearch, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.ResultDataSizeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RelatedDataSizeKey])
				s.EqualValues(s.batch*s.batchCnt, reportInfo[hookutil.RelatedCntKey])
				return
			}
		}
	}
	go searchCheckReport()
	searchResult, err := c.Proxy.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)

	queryCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("query check timeout")
				return
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("query report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeQuery, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.ResultDataSizeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RelatedDataSizeKey])
				s.EqualValues(s.batch*s.batchCnt, reportInfo[hookutil.RelatedCntKey])
				return
			}
		}
	}
	go queryCheckReport()
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())

	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	log.Info("TestStatsTask succeed")
}

func (s *StatsTaskCheckerSuite) checkBinlogIsSorted(ctx context.Context, binlogPath string, lastValue interface{}) interface{} {
	bs, err := s.cm.Read(ctx, binlogPath)
	s.NoError(err)

	reader, err := storage.NewBinlogReader(bs)
	s.NoError(err)
	defer reader.Close()
	er, err := reader.NextEventReader()
	s.NoError(err)
	if s.pkType == schemapb.DataType_VarChar {
		pks, _, err := er.GetStringFromPayload()
		s.NoError(err)
		for _, pk := range pks {
			s.GreaterOrEqual(pk, lastValue)
			lastValue = pk
		}
		return lastValue
	}
	pks, _, err := er.GetInt64FromPayload()
	s.NoError(err)
	for _, pk := range pks {
		s.GreaterOrEqual(pk, lastValue)
		lastValue = pk
	}
	return lastValue
}

func (s *StatsTaskCheckerSuite) checkSegmentIsSorted(ctx context.Context, segment *datapb.SegmentInfo) {
	err := binlog.DecompressBinLogs(segment)
	s.NoError(err)
	var pkBinlogs *datapb.FieldBinlog
	for _, fb := range segment.Binlogs {
		if fb.FieldID == 100 {
			pkBinlogs = fb
			break
		}
	}
	entitiesNum := int64(0)
	if s.pkType == schemapb.DataType_VarChar {
		lastValue := ""
		for _, b := range pkBinlogs.Binlogs {
			lastValue = s.checkBinlogIsSorted(ctx, b.GetLogPath(), lastValue).(string)
			entitiesNum += b.GetEntriesNum()
		}
	} else {
		lastValue := int64(math.MinInt64)
		for _, b := range pkBinlogs.Binlogs {
			lastValue = s.checkBinlogIsSorted(ctx, b.GetLogPath(), lastValue).(int64)
			entitiesNum += b.GetEntriesNum()
		}
	}
	s.Equal(segment.GetNumOfRows(), entitiesNum)
}
