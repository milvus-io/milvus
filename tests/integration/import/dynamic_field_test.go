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
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"strings"
	"time"
)

func (s *BulkInsertSuite) TestImportDynamicField() {
	const (
		rowCount = 10000
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 60*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert_B_" + funcutil.GenRandomStr()

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
	})
	schema.EnableDynamicField = true
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
	importFile, err := GenerateNumpyFiles(c.ChunkManager, schema, rowCount)
	s.NoError(err)
	importFile.Paths = lo.Filter(importFile.Paths, func(path string, _ int) bool {
		return !strings.Contains(path, "$meta")
	})
	files := []*internalpb.ImportFile{importFile}
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

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))
}
