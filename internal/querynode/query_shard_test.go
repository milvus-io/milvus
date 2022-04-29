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

package querynode

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func genSimpleQueryShard(ctx context.Context) (*queryShard, error) {
	tSafe := newTSafeReplica()
	historical, err := genSimpleHistorical(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	streaming, err := genSimpleStreaming(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	localCM, err := genLocalChunkManager()
	if err != nil {
		return nil, err
	}

	remoteCM, err := genRemoteChunkManager(ctx)
	if err != nil {
		return nil, err
	}

	shardCluster := NewShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel,
		&mockNodeDetector{}, &mockSegmentDetector{}, buildMockQueryNode)
	shardClusterService := &ShardClusterService{
		clusters: sync.Map{},
	}
	shardClusterService.clusters.Store(defaultDMLChannel, shardCluster)

	qs := newQueryShard(ctx, defaultCollectionID, defaultDMLChannel, defaultReplicaID, shardClusterService,
		historical, streaming, localCM, remoteCM, false)
	qs.deltaChannel = defaultDeltaChannel

	err = qs.watchDMLTSafe()
	if err != nil {
		return nil, err
	}
	err = qs.watchDeltaTSafe()
	if err != nil {
		return nil, err
	}

	return qs, nil
}

func updateQueryShardTSafe(qs *queryShard, timestamp Timestamp) error {
	err := qs.streaming.tSafeReplica.setTSafe(defaultDMLChannel, timestamp)
	if err != nil {
		return err
	}
	return qs.historical.tSafeReplica.setTSafe(defaultDeltaChannel, timestamp)
}

func TestQueryShard_Close(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	qs.Close()
}

func TestQueryShard_Search(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	assert.NoError(t, err)

	t.Run("search follower", func(t *testing.T) {
		request := &querypb.SearchRequest{
			Req:        req,
			DmlChannel: "",
			SegmentIDs: []int64{defaultSegmentID},
		}

		_, err = qs.search(context.Background(), request)
		assert.NoError(t, err)
	})

	t.Run("search leader", func(t *testing.T) {
		request := &querypb.SearchRequest{
			Req:        req,
			DmlChannel: defaultDMLChannel,
			SegmentIDs: []int64{},
		}

		_, err = qs.search(context.Background(), request)
		assert.NoError(t, err)
	})
}

func TestQueryShard_Query(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	req, err := genRetrieveRequest(schema)
	assert.NoError(t, err)

	t.Run("query follower", func(t *testing.T) {
		request := &querypb.QueryRequest{
			Req:        req,
			DmlChannel: "",
			SegmentIDs: []int64{defaultSegmentID},
		}

		resp, err := qs.query(context.Background(), request)
		assert.NoError(t, err)
		assert.ElementsMatch(t, resp.Ids.GetIntId().Data, []int64{1, 2, 3})
	})

	t.Run("query follower with wrong segment", func(t *testing.T) {
		request := &querypb.QueryRequest{
			Req:        req,
			DmlChannel: "",
			SegmentIDs: []int64{defaultSegmentID + 1},
		}

		_, err := qs.query(context.Background(), request)
		assert.Error(t, err)
	})

	t.Run("query leader", func(t *testing.T) {
		request := &querypb.QueryRequest{
			Req:        req,
			DmlChannel: defaultDMLChannel,
			SegmentIDs: []int64{},
		}

		_, err := qs.query(context.Background(), request)
		assert.NoError(t, err)
	})
}

func TestQueryShard_waitNewTSafe(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	err = updateQueryShardTSafe(qs, timestamp)
	assert.NoError(t, err)

	dmlTimestamp, err := qs.getNewTSafe(tsTypeDML)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, dmlTimestamp)

	deltaTimestamp, err := qs.getNewTSafe(tsTypeDelta)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, deltaTimestamp)
}

func TestQueryShard_WaitUntilServiceable(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	err = updateQueryShardTSafe(qs, 1000)
	assert.NoError(t, err)
	qs.waitUntilServiceable(context.Background(), 1000, tsTypeDML)
}

func genSearchResultData(nq int64, topk int64, ids []int64, scores []float32, topks []int64) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       topk,
		FieldsData: nil,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Topks: topks,
	}
}

func TestReduceSearchResultData(t *testing.T) {
	const (
		nq         = 1
		topk       = 4
		metricType = "L2"
	)
	plan := &SearchPlan{pkType: schemapb.DataType_Int64}
	t.Run("case1", func(t *testing.T) {
		ids := []int64{1, 2, 3, 4}
		scores := []float32{-1.0, -2.0, -3.0, -4.0}
		topks := []int64{int64(len(ids))}
		data1 := genSearchResultData(nq, topk, ids, scores, topks)
		data2 := genSearchResultData(nq, topk, ids, scores, topks)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := reduceSearchResultData(dataArray, nq, topk, plan)
		assert.Nil(t, err)
		assert.Equal(t, ids, res.Ids.GetIntId().Data)
		assert.Equal(t, scores, res.Scores)
	})
	t.Run("case2", func(t *testing.T) {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := genSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := genSearchResultData(nq, topk, ids2, scores2, topks2)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		res, err := reduceSearchResultData(dataArray, nq, topk, plan)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int64{1, 5, 2, 3}, res.Ids.GetIntId().Data)
	})
}

func TestMergeInternalRetrieveResults(t *testing.T) {
	const (
		Dim                  = 8
		Int64FieldName       = "Int64Field"
		FloatVectorFieldName = "FloatVectorField"
		Int64FieldID         = common.StartOfUserFieldID + 1
		FloatVectorFieldID   = common.StartOfUserFieldID + 2
	)
	Int64Array := []int64{11, 22}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:16], Dim))

	result1 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{0, 1},
				},
			},
		},
		// Offset:     []int64{0, 1},
		FieldsData: fieldDataArray1,
	}
	result2 := &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{0, 1},
				},
			},
		},
		// Offset:     []int64{0, 1},
		FieldsData: fieldDataArray2,
	}

	result, err := mergeInternalRetrieveResults([]*internalpb.RetrieveResults{result1, result2})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.FieldsData[0].GetScalars().GetLongData().Data))
	assert.Equal(t, 2*Dim, len(result.FieldsData[1].GetVectors().GetFloatVector().Data))

	_, err = mergeInternalRetrieveResults(nil)
	assert.NoError(t, err)
}
