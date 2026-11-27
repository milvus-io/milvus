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

package cmek

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
)

type consumerRow struct {
	batchID int64
	payload string
	vector  []float32
}

func consumerVector(pk int64) []float32 {
	vector := make([]float32, rawDataDim)
	vector[0] = float32(pk)
	for i := 1; i < rawDataDim; i++ {
		vector[i] = float32((pk+int64(i))%97) / 100
	}
	return vector
}

func consumerCampaign(name string) rawDataCampaign {
	return rawDataCampaign{
		name:      name,
		shardsNum: 1,
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{Name: "batch_id", DataType: schemapb.DataType_Int64},
			{Name: "payload", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}}},
			vectorSchema("float_vector", schemapb.DataType_FloatVector, rawDataDim),
		}},
		loadFields: []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"},
	}
}

func consumerBatch(start, count int, batchID int64) ([]*schemapb.FieldData, map[int64]consumerRow) {
	pks, batches := make([]int64, count), make([]int64, count)
	payloads, vectors := make([]string, count), make([]float32, count*rawDataDim)
	expected := make(map[int64]consumerRow, count)
	for i := 0; i < count; i++ {
		pk := int64(start + i)
		pks[i], batches[i] = pk, batchID
		payloads[i] = fmt.Sprintf("batch=%d/pk=%d", batchID, pk)
		vector := consumerVector(pk)
		copy(vectors[i*rawDataDim:], vector)
		expected[pk] = consumerRow{batchID, payloads[i], vector}
	}
	return []*schemapb.FieldData{
		testutils.NewInt64FieldDataWithValue(fixturePrimaryKey, pks),
		testutils.NewInt64FieldDataWithValue("batch_id", batches),
		testutils.NewVarCharFieldDataWithValue("payload", payloads),
		testutils.NewFloatVectorFieldDataWithValue("float_vector", vectors, rawDataDim),
	}, expected
}

func (s *rawDataSuite) assertConsumerRows(ctx context.Context, collection string, expected map[int64]consumerRow) {
	count, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection, Expr: "", OutputFields: []string{"count(*)"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(count, err))
	s.Require().Len(count.GetFieldsData(), 1)
	s.Require().Len(count.GetFieldsData()[0].GetScalars().GetLongData().GetData(), 1)
	s.Require().Equal(int64(len(expected)), count.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	ids := make([]int64, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for start := 0; start < len(ids); start += 100 {
		end := min(start+100, len(ids))
		selected := ids[start:end]
		values := make([]string, len(selected))
		for i, id := range selected {
			values[i] = strconv.FormatInt(id, 10)
		}
		result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName: s.dbName, CollectionName: collection, Expr: fmt.Sprintf("%s in [%s]", fixturePrimaryKey, strings.Join(values, ",")),
			OutputFields: []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		fields := fieldDataByName(result.GetFieldsData())
		s.Require().Len(fields, 4)
		for _, name := range []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"} {
			s.Require().Contains(fields, name)
		}
		actualIDs := fields[fixturePrimaryKey].GetScalars().GetLongData().GetData()
		actualBatch := fields["batch_id"].GetScalars().GetLongData().GetData()
		actualPayload := fields["payload"].GetScalars().GetStringData().GetData()
		actualVectors := fields["float_vector"].GetVectors().GetFloatVector().GetData()
		s.Require().Len(actualIDs, len(selected))
		s.Require().Len(actualBatch, len(selected))
		s.Require().Len(actualPayload, len(selected))
		s.Require().Len(actualVectors, len(selected)*rawDataDim)
		seen := make(map[int64]bool)
		for i, id := range actualIDs {
			s.Require().Contains(selected, id, "query returned an ID outside the requested page")
			want, ok := expected[id]
			s.Require().True(ok, "unexpected pk %d", id)
			s.Require().False(seen[id], "duplicate pk %d", id)
			seen[id] = true
			s.Require().Equal(want.batchID, actualBatch[i], "pk=%d", id)
			s.Require().Equal(want.payload, actualPayload[i], "pk=%d", id)
			s.Require().Equal(want.vector, actualVectors[i*rawDataDim:(i+1)*rawDataDim], "pk=%d", id)
		}
	}
	representatives := make(map[int64]int64)
	for _, id := range ids {
		batch := expected[id].batchID
		if _, exists := representatives[batch]; !exists {
			representatives[batch] = id
		}
	}
	for batch, id := range representatives {
		vector := consumerVector(id)
		request := integration.ConstructSearchRequest(s.dbName, collection, "", "float_vector", schemapb.DataType_FloatVector,
			[]string{fixturePrimaryKey}, metric.L2, map[string]any{"ef": 200}, 1, rawDataDim, 1, -1)
		placeholder, err := proto.Marshal(funcutil.Float32VectorsToPlaceholderGroup([][]float32{vector}))
		s.Require().NoError(err)
		request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
		result, err := s.Cluster.MilvusClient.Search(ctx, request)
		s.Require().NoError(merr.CheckRPCCall(result, err), "batch %d", batch)
		s.Require().Equal([]int64{id}, result.GetResults().GetIds().GetIntId().GetData(), "batch %d", batch)
		s.Require().Len(result.GetResults().GetScores(), 1)
		s.Require().InDelta(0, result.GetResults().GetScores()[0], 1e-6)
	}
}

func (s *rawDataSuite) flushConsumerCollection(ctx context.Context, description *milvuspb.DescribeCollectionResponse) []*datapb.SegmentInfo {
	collection := description.GetCollectionName()
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	ids := flushSegmentIDs(flush, collection)
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[collection], s.dbName, collection)
	return s.rawFlushedSegments(collection, ids)
}
