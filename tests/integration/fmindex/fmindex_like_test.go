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

package fmindex

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const contentField = "content"

type FMIndexLikeSuite struct {
	integration.MiniClusterSuite
}

func (s *FMIndexLikeSuite) queryPKs(ctx context.Context, collection, expr string) []int64 {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             expr,
		OutputFields:     []string{integration.Int64Field},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(resp, err)
	s.Require().NoError(err)
	var pks []int64
	for _, fd := range resp.GetFieldsData() {
		if fd.GetFieldName() == integration.Int64Field {
			pks = append(pks, fd.GetScalars().GetLongData().GetData()...)
		}
	}
	sort.Slice(pks, func(i, j int) bool { return pks[i] < pks[j] })
	return pks
}

func (s *FMIndexLikeSuite) flush(ctx context.Context, collection string) {
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, flushResp.GetStatus().GetErrorCode())
	segIDs := flushResp.GetCollSegIDs()[collection]
	s.Require().NotNil(segIDs)
	flushTs, has := flushResp.GetCollFlushTs()[collection]
	s.Require().True(has)
	s.WaitForFlush(ctx, segIDs.GetData(), flushTs, "", collection)
}

func makeRow(i int) (text string, valid bool) {
	filler := strings.Repeat("y", 500)
	switch i % 80 {
	case 0:
		return "QOP" + filler + "ZEBRA", true
	case 1:
		return filler + "ZEBRA", true
	case 2:
		return "QOP" + filler, true
	case 3:
		return "", true
	case 4:
		return filler, false
	default:
		return filler, true
	}
}

func (s *FMIndexLikeSuite) insertBatch(ctx context.Context, collection string, start, n int) {
	ids := make([]int64, n)
	valids := make([]bool, n)
	var texts []string
	for i := 0; i < n; i++ {
		ids[i] = int64(start + i)
		text, valid := makeRow(start + i)
		valids[i] = valid
		if valid {
			texts = append(texts, text)
		}
	}
	pk := integration.NewInt64FieldDataWithStart(integration.Int64Field, n, int64(start))
	pk.GetScalars().GetLongData().Data = ids
	vec := integration.NewFloatVectorFieldData(integration.FloatVecField, n, 16)
	content := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: contentField,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: texts},
				},
			},
		},
	}
	typeutil.SetFieldDataValidData(content, valids)
	insertResult, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, content},
		HashKeys:       integration.GenerateHashKeys(n),
		NumRows:        uint32(n),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insertResult.GetStatus().GetErrorCode())
}

func (s *FMIndexLikeSuite) TestGeneralLikeBeforeAndAfterIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster
	const (
		dim      = 16
		batch    = 200
		segments = 2
	)
	collectionName := "TestFMIndexLike" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, false, []*schemapb.FieldSchema{
		{
			FieldID:      100,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		},
		{
			FieldID:  101,
			Name:     integration.FloatVecField,
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
			},
		},
		{
			FieldID:  102,
			Name:     contentField,
			DataType: schemapb.DataType_VarChar,
			Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "65535"},
			},
		},
	}...)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode())

	for seg := 0; seg < segments; seg++ {
		s.insertBatch(ctx, collectionName, seg*batch, batch)
		s.flush(ctx, collectionName)
	}

	vecIdx, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "vec_idx",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, vecIdx.GetErrorCode())
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, integration.FloatVecField, "vec_idx")

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	exprs := []string{
		fmt.Sprintf(`%s like "QOP%%ZEBRA"`, contentField),
		fmt.Sprintf(`%s like "%%ZEBRA%%"`, contentField),
		fmt.Sprintf(`%s like "%%y%%"`, contentField),
		fmt.Sprintf(`%s like "%%%%"`, contentField),
		fmt.Sprintf(`%s like ""`, contentField),
	}
	before := make([][]int64, len(exprs))
	for i, expr := range exprs {
		before[i] = s.queryPKs(ctx, collectionName, expr)
	}
	s.Require().NotEmpty(before[0], "selective QOP%%ZEBRA should hit")
	s.Require().Greater(len(before[1]), len(before[0]), "%%ZEBRA%% should be a superset of QOP%%ZEBRA")

	release, err := c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(release, err)
	s.Require().NoError(err)

	fmIdx, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      contentField,
		IndexName:      "fm_idx",
		ExtraParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "FMINDEX"},
			{Key: "fm_sa_sample_rate", Value: "8"},
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, fmIdx.GetErrorCode())
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, contentField, "fm_idx")

	loadStatus, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	for i, expr := range exprs {
		after := s.queryPKs(ctx, collectionName, expr)
		s.Equal(before[i], after, expr)
	}
}

func TestFMIndexLike(t *testing.T) {
	suite.Run(t, new(FMIndexLikeSuite))
}
