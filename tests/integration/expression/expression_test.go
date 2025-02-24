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

package expression

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type ExpressionSuite struct {
	integration.MiniClusterSuite
	dbName         string
	collectionName string
	dim            int
	rowNum         int
}

func (s *ExpressionSuite) setParams() {
	prefix := "TestExpression"
	s.dbName = ""
	s.collectionName = prefix + funcutil.GenRandomStr()
	s.dim = 128
	s.rowNum = 100
}

func newJSONData(fieldName string, rowNum int) *schemapb.FieldData {
	jsonData := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := map[string]interface{}{
			"A": i,
			"B": rowNum - i,
			"C": []int{i, rowNum - i},
			"D": fmt.Sprintf("name-%d", i),
			"E": map[string]interface{}{
				"F": i,
				"G": i + 10,
			},
			"str1": `abc\"def-` + string(rune(i)),
			"str2": fmt.Sprintf("abc\"def-%d", i),
			"str3": fmt.Sprintf("abc\ndef-%d", i),
			"str4": fmt.Sprintf("abc\367-%d", i),
		}
		if i%2 == 0 {
			data = map[string]interface{}{
				"B": rowNum - i,
				"C": []int{i, rowNum - i},
				"D": fmt.Sprintf("name-%d", i),
				"E": map[string]interface{}{
					"F": i,
					"G": i + 10,
				},
			}
		}
		if i == 100 {
			data = nil
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return nil
		}
		jsonData = append(jsonData, jsonBytes)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: jsonData,
					},
				},
			},
		},
	}
}

func (s *ExpressionSuite) insertFlushIndexLoad(ctx context.Context, fieldData []*schemapb.FieldData) {
	hashKeys := integration.GenerateHashKeys(s.rowNum)
	insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         s.dbName,
		CollectionName: s.collectionName,
		FieldsData:     fieldData,
		HashKeys:       hashKeys,
		NumRows:        uint32(s.rowNum),
	})
	s.NoError(err)
	s.NoError(merr.Error(insertResult.GetStatus()))

	// flush
	flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          s.dbName,
		CollectionNames: []string{s.collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[s.collectionName]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[s.collectionName]
	s.True(has)

	segments, err := s.Cluster.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	s.WaitForFlush(ctx, ids, flushTs, s.dbName, s.collectionName)

	// create index
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		CollectionName: s.collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	s.NoError(err)
	s.WaitForIndexBuilt(context.TODO(), s.collectionName, integration.FloatVecField)
	log.Info("=========================Index created=========================")

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: s.collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	s.NoError(err)
	s.WaitForLoad(context.TODO(), s.collectionName)
	log.Info("=========================Collection loaded=========================")
}

func (s *ExpressionSuite) setupData() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	schema := integration.ConstructSchema(s.collectionName, s.dim, true)
	schema.EnableDynamicField = true
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         s.dbName,
		CollectionName: s.collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)
	err = merr.Error(createCollectionStatus)
	s.NoError(err)

	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	err = merr.Error(showCollectionsResp.GetStatus())
	s.NoError(err)

	describeCollectionResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: s.collectionName})
	s.NoError(err)
	err = merr.Error(describeCollectionResp.GetStatus())
	s.NoError(err)
	s.True(describeCollectionResp.Schema.EnableDynamicField)
	s.Equal(2, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, s.rowNum, s.dim)
	jsonData := newJSONData(common.MetaFieldName, s.rowNum)
	jsonData.IsDynamic = true
	s.insertFlushIndexLoad(ctx, []*schemapb.FieldData{fVecColumn, jsonData})
}

type testCase struct {
	expr   string
	topK   int
	resNum int
}

func (s *ExpressionSuite) searchWithExpression() {
	testcases := []testCase{
		{"A + 5 > 0", 10, 10},
		{"B - 5 >= 0", 10, 10},
		{"C[0] * 5 < 500", 10, 10},
		{"E['F'] / 5 <= 100", 10, 10},
		{"E['G'] % 5 == 4", 10, 10},
		{"A / 5 != 4", 10, 10},
	}
	for _, c := range testcases {
		params := integration.GetSearchParams(integration.IndexFaissIDMap, metric.IP)
		searchReq := integration.ConstructSearchRequest(s.dbName, s.collectionName, c.expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, 1, s.dim, c.topK, -1)

		searchResult, err := s.Cluster.Proxy.Search(context.Background(), searchReq)
		s.NoError(err)
		err = merr.Error(searchResult.GetStatus())
		s.NoError(err)
		s.Equal(c.resNum, len(searchResult.GetResults().GetScores()))
		log.Info(fmt.Sprintf("=========================Search done with expr:%s =========================", c.expr))
	}
}

func (s *ExpressionSuite) TestExpression() {
	s.setParams()
	s.setupData()
	s.searchWithExpression()
}

func TestExpression(t *testing.T) {
	suite.Run(t, new(ExpressionSuite))
}
