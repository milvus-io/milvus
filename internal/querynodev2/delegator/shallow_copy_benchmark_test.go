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

package delegator

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// createTestQueryRequest creates a realistic QueryRequest for benchmarking
func createTestQueryRequest() *querypb.QueryRequest {
	// Create a realistic RetrieveRequest with typical field sizes
	return &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Retrieve,
				MsgID:     12345,
				Timestamp: 1000000,
				SourceID:  1,
				TargetID:  2,
			},
			ReqID:                        100,
			DbID:                         1,
			CollectionID:                 1000,
			PartitionIDs:                 []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Typical partition list
			SerializedExprPlan:           make([]byte, 1024),                     // 1KB expression plan (typical size)
			OutputFieldsId:               []int64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109},
			MvccTimestamp:                1000000,
			GuaranteeTimestamp:           999999,
			TimeoutTimestamp:             2000000,
			Limit:                        100,
			IgnoreGrowing:                false,
			IsCount:                      false,
			IterationExtensionReduceRate: 0,
			Username:                     "test_user",
			ReduceStopForBest:            false,
			ReduceType:                   0,
			ConsistencyLevel:             commonpb.ConsistencyLevel_Bounded,
			IsIterator:                   false,
			CollectionTtlTimestamps:      0,
			GroupByFieldIds:              []int64{100, 101},
			Aggregates:                   nil,
			EntityTtlPhysicalTime:        0,
		},
		DmlChannels:     []string{"channel1", "channel2"},
		SegmentIDs:      []int64{1001, 1002, 1003, 1004, 1005},
		FromShardLeader: true,
		Scope:           querypb.DataScope_Historical,
	}
}

// createTestGetStatisticsRequest creates a realistic GetStatisticsRequest for benchmarking
func createTestGetStatisticsRequest() *querypb.GetStatisticsRequest {
	return &querypb.GetStatisticsRequest{
		Req: &internalpb.GetStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_GetCollectionStatistics,
				MsgID:     12345,
				Timestamp: 1000000,
				SourceID:  1,
				TargetID:  2,
			},
			DbID:               1,
			CollectionID:       1000,
			PartitionIDs:       []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			TravelTimestamp:    1000000,
			GuaranteeTimestamp: 999999,
			TimeoutTimestamp:   2000000,
		},
		DmlChannels:     []string{"channel1", "channel2"},
		SegmentIDs:      []int64{1001, 1002, 1003, 1004, 1005},
		FromShardLeader: true,
		Scope:           querypb.DataScope_Historical,
	}
}

// Old implementation using proto.Clone for QueryRequest
func modifyQueryRequestWithProtoClone(req *querypb.QueryRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64, vchannelName string) *querypb.QueryRequest {
	nodeReq := proto.Clone(req).(*querypb.QueryRequest)
	nodeReq.Scope = scope
	nodeReq.Req.Base.TargetID = targetID
	nodeReq.SegmentIDs = segmentIDs
	nodeReq.DmlChannels = []string{vchannelName}
	return nodeReq
}

// New implementation using shallow copy for QueryRequest
func shallowCopyRetrieveRequest(req *internalpb.RetrieveRequest, targetID int64) *internalpb.RetrieveRequest {
	return &internalpb.RetrieveRequest{
		Base:                         &commonpb.MsgBase{TargetID: targetID},
		ReqID:                        req.ReqID,
		DbID:                         req.DbID,
		CollectionID:                 req.CollectionID,
		PartitionIDs:                 req.PartitionIDs,
		SerializedExprPlan:           req.SerializedExprPlan,
		OutputFieldsId:               req.OutputFieldsId,
		MvccTimestamp:                req.MvccTimestamp,
		GuaranteeTimestamp:           req.GuaranteeTimestamp,
		TimeoutTimestamp:             req.TimeoutTimestamp,
		Limit:                        req.Limit,
		IgnoreGrowing:                req.IgnoreGrowing,
		IsCount:                      req.IsCount,
		IterationExtensionReduceRate: req.IterationExtensionReduceRate,
		Username:                     req.Username,
		ReduceStopForBest:            req.ReduceStopForBest,
		ReduceType:                   req.ReduceType,
		ConsistencyLevel:             req.ConsistencyLevel,
		IsIterator:                   req.IsIterator,
		CollectionTtlTimestamps:      req.CollectionTtlTimestamps,
		GroupByFieldIds:              req.GroupByFieldIds,
		Aggregates:                   req.Aggregates,
		EntityTtlPhysicalTime:        req.EntityTtlPhysicalTime,
	}
}

func modifyQueryRequestWithShallowCopy(req *querypb.QueryRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64, vchannelName string) *querypb.QueryRequest {
	return &querypb.QueryRequest{
		Req:             shallowCopyRetrieveRequest(req.GetReq(), targetID),
		DmlChannels:     []string{vchannelName},
		SegmentIDs:      segmentIDs,
		FromShardLeader: req.FromShardLeader,
		Scope:           scope,
	}
}

// Old implementation using proto.Clone for GetStatisticsRequest
func modifyGetStatisticsRequestWithProtoClone(req *querypb.GetStatisticsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.GetStatisticsRequest {
	nodeReq := proto.Clone(req).(*querypb.GetStatisticsRequest)
	nodeReq.GetReq().GetBase().TargetID = targetID
	nodeReq.Scope = scope
	nodeReq.SegmentIDs = segmentIDs
	nodeReq.FromShardLeader = true
	return nodeReq
}

// New implementation using shallow copy for GetStatisticsRequest
func modifyGetStatisticsRequestWithShallowCopy(req *querypb.GetStatisticsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.GetStatisticsRequest {
	innerReq := req.GetReq()
	return &querypb.GetStatisticsRequest{
		Req: &internalpb.GetStatisticsRequest{
			Base:               &commonpb.MsgBase{TargetID: targetID},
			DbID:               innerReq.GetDbID(),
			CollectionID:       innerReq.GetCollectionID(),
			PartitionIDs:       innerReq.GetPartitionIDs(),
			TravelTimestamp:    innerReq.GetTravelTimestamp(),
			GuaranteeTimestamp: innerReq.GetGuaranteeTimestamp(),
			TimeoutTimestamp:   innerReq.GetTimeoutTimestamp(),
		},
		DmlChannels:     req.GetDmlChannels(),
		SegmentIDs:      segmentIDs,
		FromShardLeader: true,
		Scope:           scope,
	}
}

// Benchmark for QueryRequest with proto.Clone (old implementation)
func BenchmarkQueryRequest_ProtoClone(b *testing.B) {
	req := createTestQueryRequest()
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyQueryRequestWithProtoClone(req, querypb.DataScope_Historical, segmentIDs, int64(i), vchannelName)
	}
}

// Benchmark for QueryRequest with shallow copy (new implementation)
func BenchmarkQueryRequest_ShallowCopy(b *testing.B) {
	req := createTestQueryRequest()
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyQueryRequestWithShallowCopy(req, querypb.DataScope_Historical, segmentIDs, int64(i), vchannelName)
	}
}

// Benchmark for GetStatisticsRequest with proto.Clone (old implementation)
func BenchmarkGetStatisticsRequest_ProtoClone(b *testing.B) {
	req := createTestGetStatisticsRequest()
	segmentIDs := []int64{2001, 2002, 2003}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyGetStatisticsRequestWithProtoClone(req, querypb.DataScope_Historical, segmentIDs, int64(i))
	}
}

// Benchmark for GetStatisticsRequest with shallow copy (new implementation)
func BenchmarkGetStatisticsRequest_ShallowCopy(b *testing.B) {
	req := createTestGetStatisticsRequest()
	segmentIDs := []int64{2001, 2002, 2003}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyGetStatisticsRequestWithShallowCopy(req, querypb.DataScope_Historical, segmentIDs, int64(i))
	}
}

// Benchmark simulating multiple worker nodes (realistic scenario)
// In a real cluster, each query fans out to N worker nodes
func BenchmarkQueryRequest_ProtoClone_MultiWorker(b *testing.B) {
	req := createTestQueryRequest()
	numWorkers := 10 // Simulate 10 worker nodes
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for w := 0; w < numWorkers; w++ {
			_ = modifyQueryRequestWithProtoClone(req, querypb.DataScope_Historical, segmentIDs, int64(w), vchannelName)
		}
	}
}

func BenchmarkQueryRequest_ShallowCopy_MultiWorker(b *testing.B) {
	req := createTestQueryRequest()
	numWorkers := 10 // Simulate 10 worker nodes
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for w := 0; w < numWorkers; w++ {
			_ = modifyQueryRequestWithShallowCopy(req, querypb.DataScope_Historical, segmentIDs, int64(w), vchannelName)
		}
	}
}

// Benchmark with larger SerializedExprPlan (complex query expressions)
func BenchmarkQueryRequest_LargeExprPlan_ProtoClone(b *testing.B) {
	req := createTestQueryRequest()
	req.Req.SerializedExprPlan = make([]byte, 64*1024) // 64KB expression plan
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyQueryRequestWithProtoClone(req, querypb.DataScope_Historical, segmentIDs, int64(i), vchannelName)
	}
}

func BenchmarkQueryRequest_LargeExprPlan_ShallowCopy(b *testing.B) {
	req := createTestQueryRequest()
	req.Req.SerializedExprPlan = make([]byte, 64*1024) // 64KB expression plan
	segmentIDs := []int64{2001, 2002, 2003}
	vchannelName := "test_vchannel"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = modifyQueryRequestWithShallowCopy(req, querypb.DataScope_Historical, segmentIDs, int64(i), vchannelName)
	}
}
