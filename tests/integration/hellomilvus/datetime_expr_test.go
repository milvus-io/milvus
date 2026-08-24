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

package hellomilvus

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dateFieldName = "date_field"
	timeFieldName = "time_field"
)

func epochDays(year int, month time.Month, day int) int32 {
	t := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return int32(t.Unix() / (24 * 60 * 60))
}

func pkFromQuery(s *HelloMilvusSuite, resp *milvuspb.QueryResults, fieldName string) int64 {
	s.Require().NotNil(resp)
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() == fieldName {
			data := field.GetScalars().GetLongData().GetData()
			s.Require().Equal(1, len(data))
			return data[0]
		}
	}
	s.FailNow("pk field missing from query result")
	return 0
}

func (s *HelloMilvusSuite) TestDateTimeCreateInsertFlushLoadQuery() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 8
		dbName = ""
		rowNum = 3
	)

	collectionName := "TestDateTimeExpr" + funcutil.GenRandomStr()
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}
	dateField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     dateFieldName,
		DataType: schemapb.DataType_Date,
	}
	timeField := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     timeFieldName,
		DataType: schemapb.DataType_Time,
	}
	vec := &schemapb.FieldSchema{
		FieldID:  103,
		Name:     integration.FloatVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: strconv.Itoa(dim)},
		},
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{pk, dateField, timeField, vec},
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode(), createStatus.GetReason())

	pkData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 1)
	dateData := &schemapb.FieldData{
		Type:      schemapb.DataType_Date,
		FieldName: dateFieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DateData{
					DateData: &schemapb.DateArray{
						Data: []int32{
							epochDays(2024, time.June, 21),
							epochDays(2024, time.June, 22),
							epochDays(2024, time.June, 23),
						},
					},
				},
			},
		},
	}
	timeData := &schemapb.FieldData{
		Type:      schemapb.DataType_Time,
		FieldName: timeFieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimeData{
					TimeData: &schemapb.TimeArray{
						Data: []int64{
							0,
							(13*3600 + 45*60 + 30) * 1_000_000,
							24 * 3600 * 1_000_000,
						},
					},
				},
			},
		},
	}
	vecData := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkData, dateData, timeData, vecData},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, insertResult.GetStatus().GetErrorCode(), insertResult.GetStatus().GetReason())

	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, flushResp.GetStatus().GetErrorCode(), flushResp.GetStatus().GetReason())
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(has)
	s.Require().NotEmpty(segmentIDs.GetData())
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, dbName, collectionName)

	indexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIDMap, metric.L2),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, indexStatus.GetErrorCode(), indexStatus.GetReason())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode(), loadStatus.GetReason())
	s.WaitForLoad(ctx, collectionName)

	dateQuery, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           dateFieldName + ` > "2024-06-22"`,
		OutputFields:   []string{integration.Int64Field, dateFieldName},
	})
	err = merr.CheckRPCCall(dateQuery, err)
	s.NoError(err)
	s.Equal(int64(3), pkFromQuery(s, dateQuery, integration.Int64Field))

	timeQuery, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           timeFieldName + ` in ["13:45:30"]`,
		OutputFields:   []string{integration.Int64Field, timeFieldName},
	})
	err = merr.CheckRPCCall(timeQuery, err)
	s.NoError(err)
	s.Equal(int64(2), pkFromQuery(s, timeQuery, integration.Int64Field))

	status, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
}
