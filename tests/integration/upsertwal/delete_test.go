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

package upsertwal

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A separate Proxy process keeps the small logical Delete limit local to this test.
type UpsertWALDeleteSuite struct{ upsertWALFixture }

func TestUpsertWALDelete(t *testing.T) { suite.Run(t, new(UpsertWALDeleteSuite)) }

func (s *UpsertWALDeleteSuite) SetupSuite() {
	s.WithMilvusConfig("quotaAndLimits.limits.maxDeleteSize", "512")
	s.upsertWALFixture.SetupSuite()
}

func (s *UpsertWALDeleteSuite) TestPublicDeleteSpansLogicalMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	c := s.create(ctx)
	keys, markers, vectors := make([]int64, 65), make([]int64, 65), make([]float32, 130)
	expressionKeys := make([]string, 64)
	for row := range keys {
		keys[row], markers[row] = 1<<58+int64(row), 1
		vectors[2*row], vectors[2*row+1] = 1, 2
		if row < 64 {
			expressionKeys[row] = strconv.FormatInt(keys[row], 10)
		}
	}
	inserted, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 65, FieldsData: []*schemapb.FieldData{
		{FieldName: "pk", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: keys}}}}},
		{FieldName: "marker", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: markers}}}}},
		{FieldName: "vector", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}}}}},
	}})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	s.flush(ctx, c)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err = producer.Append(ctx, c.upsert(keys[0], 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, keys[0], 2, 1)
	s.assertRows(ctx, c, keys[63], 1, 1)
	s.assertRows(ctx, c, keys[64], 1, 1)
	targets := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys[:64]}}}
	s.Require().Greater(proto.Size(targets), 512, "PKs alone exceed the configured logical Delete limit")
	expression := "pk in [" + strings.Join(expressionKeys, ",") + "]"
	deleted, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: c.name, Expr: expression})
	s.Require().NoError(merr.CheckRPCCall(deleted, err))
	s.Require().Equal(int64(64), deleted.GetDeleteCnt())
	s.Require().Positive(deleted.GetTimestamp())
	check := func() {
		result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: c.name, Expr: expression, OutputFields: []string{"pk"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		s.Require().Len(result.FieldsData, 1)
		s.Require().Empty(result.FieldsData[0].GetScalars().GetLongData().GetData())
		s.assertRows(ctx, c, keys[0], 1, 0)
		s.assertRows(ctx, c, keys[0], 2, 0)
		s.assertRows(ctx, c, keys[64], 1, 1)
	}
	check()
	s.flush(ctx, c)
	producer.Close()
	s.restartStreamingNode(c)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	check()
}
