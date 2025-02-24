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
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *BulkInsertSuite) TestImportWithPartitionKey() {
	const (
		rowCount = 10000
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 120*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert_WithPartitionKey_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true, &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       true,
	}, &schemapb.FieldSchema{
		FieldID:  101,
		Name:     integration.FloatVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
	}, &schemapb.FieldSchema{
		FieldID:  102,
		Name:     integration.VarCharField,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: fmt.Sprintf("%d", 256),
			},
		},
		IsPartitionKey: true,
	})
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "",
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(int32(0), createCollectionStatus.GetCode())

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.Equal(int32(0), createIndexStatus.GetCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// import
	var files []*internalpb.ImportFile
	err = os.MkdirAll(c.ChunkManager.RootPath(), os.ModePerm)
	s.NoError(err)

	filePath := fmt.Sprintf("/tmp/test_%d.parquet", rand.Int())
	insertData, err := GenerateParquetFileAndReturnInsertData(filePath, schema, rowCount)
	s.NoError(err)
	defer os.Remove(filePath)
	files = []*internalpb.ImportFile{
		{
			Paths: []string{
				filePath,
			},
		},
	}

	importResp, err := c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	log.Info("Show segments", zap.Any("segments", segments))

	// load refresh
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
		Refresh:        true,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoadRefresh(ctx, "", collectionName)

	// query partition key, TermExpr
	queryNum := 10
	partitionKeyData := insertData.Data[int64(102)].GetDataRows().([]string)
	queryData := partitionKeyData[:queryNum]
	strs := lo.Map(queryData, func(str string, _ int) string {
		return fmt.Sprintf("\"%s\"", str)
	})
	str := strings.Join(strs, `,`)
	expr := fmt.Sprintf("%s in [%v]", integration.VarCharField, str)
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{integration.VarCharField},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	for _, data := range queryResult.GetFieldsData() {
		if data.GetType() == schemapb.DataType_VarChar {
			resData := data.GetScalars().GetStringData().GetData()
			s.Equal(queryNum, len(resData))
			s.ElementsMatch(resData, queryData)
		}
	}

	// query partition key, CmpOp 1
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err = c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{integration.VarCharField},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	for _, data := range queryResult.GetFieldsData() {
		if data.GetType() == schemapb.DataType_VarChar {
			resData := data.GetScalars().GetStringData().GetData()
			s.Equal(rowCount, len(resData))
			s.ElementsMatch(resData, partitionKeyData)
		}
	}

	// query partition key, CmpOp 2
	target := partitionKeyData[rand.Intn(rowCount)]
	expr = fmt.Sprintf("%s == \"%s\"", integration.VarCharField, target)
	queryResult, err = c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{integration.VarCharField},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	for _, data := range queryResult.GetFieldsData() {
		if data.GetType() == schemapb.DataType_VarChar {
			resData := data.GetScalars().GetStringData().GetData()
			s.Equal(1, len(resData))
			s.Equal(resData[0], target)
		}
	}
}
