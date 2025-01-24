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

package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	AnnsFieldKey    = "anns_field"
	TopKKey         = "topk"
	NQKey           = "nq"
	MetricTypeKey   = common.MetricTypeKey
	SearchParamsKey = common.IndexParamsKey
	RoundDecimalKey = "round_decimal"
	OffsetKey       = "offset"
	LimitKey        = "limit"
)

func (s *MiniClusterSuite) WaitForLoadWithDB(ctx context.Context, dbName, collection string) {
	s.waitForLoadInternal(ctx, dbName, collection)
}

func (s *MiniClusterSuite) WaitForLoad(ctx context.Context, collection string) {
	s.waitForLoadInternal(ctx, "", collection)
}

func (s *MiniClusterSuite) WaitForSortedSegmentLoaded(ctx context.Context, dbName, collection string) {
	cluster := s.Cluster
	getSegmentsSorted := func() bool {
		querySegmentInfo, err := cluster.Proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{
			DbName:         dbName,
			CollectionName: collection,
		})
		if err != nil {
			panic("GetQuerySegmentInfo fail")
		}

		for _, info := range querySegmentInfo.GetInfos() {
			if !info.GetIsSorted() {
				return false
			}
		}
		return true
	}

	for !getSegmentsSorted() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for get segments sorted")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (s *MiniClusterSuite) waitForLoadInternal(ctx context.Context, dbName, collection string) {
	cluster := s.Cluster
	getLoadingProgress := func() *milvuspb.GetLoadingProgressResponse {
		loadProgress, err := cluster.Proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			DbName:         dbName,
			CollectionName: collection,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		return loadProgress
	}
	for getLoadingProgress().GetProgress() != 100 {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for load")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (s *MiniClusterSuite) WaitForLoadRefresh(ctx context.Context, dbName, collection string) {
	cluster := s.Cluster
	getLoadingProgress := func() *milvuspb.GetLoadingProgressResponse {
		loadProgress, err := cluster.Proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			DbName:         dbName,
			CollectionName: collection,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		return loadProgress
	}
	for getLoadingProgress().GetRefreshProgress() != 100 {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for load (refresh)")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// CheckCollectionCacheReleased checks if the collection cache was released from querynodes.
func (s *MiniClusterSuite) CheckCollectionCacheReleased(collectionID int64) {
	for _, qn := range s.Cluster.GetAllQueryNodes() {
		s.Eventually(func() bool {
			state, err := qn.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
			s.NoError(err)
			if state.GetState().GetStateCode() != commonpb.StateCode_Healthy {
				// skip checking stopping/stopped node
				return true
			}
			req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
			s.NoError(err)
			resp, err := qn.GetQueryNode().GetMetrics(context.Background(), req)
			err = merr.CheckRPCCall(resp.GetStatus(), err)
			s.NoError(err)
			infos := metricsinfo.QueryNodeInfos{}
			err = metricsinfo.UnmarshalComponentInfos(resp.Response, &infos)
			s.NoError(err)
			for _, id := range infos.QuotaMetrics.Effect.CollectionIDs {
				if id == collectionID {
					s.T().Logf("collection %d was not released in querynode %d", collectionID, qn.GetQueryNode().GetNodeID())
					return false
				}
			}
			s.T().Logf("collection %d has been released from querynode %d", collectionID, qn.GetQueryNode().GetNodeID())
			return true
		}, 3*time.Minute, 200*time.Millisecond)
	}
}

func ConstructSearchRequest(
	dbName, collectionName string,
	expr string,
	vecField string,
	vectorType schemapb.DataType,
	outputFields []string,
	metricType string,
	params map[string]any,
	nq, dim int, topk, roundDecimal int,
) *milvuspb.SearchRequest {
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim, vectorType)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     outputFields,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MetricTypeKey,
				Value: metricType,
			},
			{
				Key:   SearchParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: vecField,
			},
			{
				Key:   common.TopKKey,
				Value: strconv.Itoa(topk),
			},
			{
				Key:   RoundDecimalKey,
				Value: strconv.Itoa(roundDecimal),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
		Nq:                 int64(nq),
	}
}

func ConstructSearchRequestWithConsistencyLevel(
	dbName, collectionName string,
	expr string,
	vecField string,
	vectorType schemapb.DataType,
	outputFields []string,
	metricType string,
	params map[string]any,
	nq, dim int, topk, roundDecimal int,
	useDefaultConsistency bool,
	consistencyLevel commonpb.ConsistencyLevel,
) *milvuspb.SearchRequest {
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim, vectorType)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     outputFields,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MetricTypeKey,
				Value: metricType,
			},
			{
				Key:   SearchParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: vecField,
			},
			{
				Key:   common.TopKKey,
				Value: strconv.Itoa(topk),
			},
			{
				Key:   RoundDecimalKey,
				Value: strconv.Itoa(roundDecimal),
			},
		},
		TravelTimestamp:       0,
		GuaranteeTimestamp:    0,
		UseDefaultConsistency: useDefaultConsistency,
		ConsistencyLevel:      consistencyLevel,
	}
}

func constructPlaceholderGroup(nq, dim int, vectorType schemapb.DataType) *commonpb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	var placeholderType commonpb.PlaceholderType
	switch vectorType {
	case schemapb.DataType_FloatVector:
		placeholderType = commonpb.PlaceholderType_FloatVector
		for i := 0; i < nq; i++ {
			bs := make([]byte, 0, dim*4)
			for j := 0; j < dim; j++ {
				var buffer bytes.Buffer
				f := rand.Float32()
				err := binary.Write(&buffer, common.Endian, f)
				if err != nil {
					panic(err)
				}
				bs = append(bs, buffer.Bytes()...)
			}
			values = append(values, bs)
		}
	case schemapb.DataType_BinaryVector:
		placeholderType = commonpb.PlaceholderType_BinaryVector
		for i := 0; i < nq; i++ {
			total := dim / 8
			ret := make([]byte, total)
			_, err := rand.Read(ret)
			if err != nil {
				panic(err)
			}
			values = append(values, ret)
		}
	case schemapb.DataType_Float16Vector:
		placeholderType = commonpb.PlaceholderType_Float16Vector
		data := testutils.GenerateFloat16Vectors(nq, dim)
		for i := 0; i < nq; i++ {
			rowBytes := dim * 2
			values = append(values, data[rowBytes*i:rowBytes*(i+1)])
		}
	case schemapb.DataType_BFloat16Vector:
		placeholderType = commonpb.PlaceholderType_BFloat16Vector
		data := testutils.GenerateBFloat16Vectors(nq, dim)
		for i := 0; i < nq; i++ {
			rowBytes := dim * 2
			values = append(values, data[rowBytes*i:rowBytes*(i+1)])
		}
	case schemapb.DataType_SparseFloatVector:
		// for sparse, all query rows are encoded in a single byte array
		values = make([][]byte, 0, 1)
		placeholderType = commonpb.PlaceholderType_SparseFloatVector
		sparseVecs := GenerateSparseFloatArray(nq)
		values = append(values, sparseVecs.Contents...)
	case schemapb.DataType_Int8Vector:
		placeholderType = commonpb.PlaceholderType_Int8Vector
		data := testutils.GenerateInt8Vectors(nq, dim)
		for i := 0; i < nq; i++ {
			rowBytes := dim
			values = append(values, typeutil.Int8ArrayToBytes(data[rowBytes*i:rowBytes*(i+1)]))
		}
	default:
		panic("invalid vector data type")
	}

	return &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   placeholderType,
				Values: values,
			},
		},
	}
}
