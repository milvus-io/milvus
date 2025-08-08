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

package segments

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RetrieveSegmentResult struct {
	Result  *segcorepb.RetrieveResults
	Segment Segment
}

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan, req *querypb.QueryRequest) ([]RetrieveSegmentResult, error) {
	resultCh := make(chan RetrieveSegmentResult, len(segments))

	anySegIsLazyLoad := func() bool {
		for _, seg := range segments {
			if seg.IsLazyLoad() {
				return true
			}
		}
		return false
	}()
	plan.SetIgnoreNonPk(!anySegIsLazyLoad && len(segments) > 1 && req.GetReq().GetLimit() != typeutil.Unlimited && plan.ShouldIgnoreNonPk())

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	retriever := func(ctx context.Context, s Segment) error {
		tr := timerecord.NewTimeRecorder("retrieveOnSegments")
		result, err := s.Retrieve(ctx, plan)
		if err != nil {
			return err
		}

		log := log.Ctx(ctx)
		if log.Core().Enabled(zap.DebugLevel) && req.GetReq().GetIsCount() {
			allRetrieveCount := result.AllRetrieveCount
			countRet := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
			if allRetrieveCount != countRet {
				log.Debug("count segment done with delete",
					zap.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
					zap.String("channel", s.LoadInfo().GetInsertChannel()),
					zap.Int64("segmentID", s.ID()),
					zap.Int64("allRetrieveCount", allRetrieveCount),
					zap.Int64("countRet", countRet))
			} else {
				log.Debug("count segment done",
					zap.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
					zap.String("channel", s.LoadInfo().GetInsertChannel()),
					zap.Int64("segmentID", s.ID()),
					zap.Int64("allRetrieveCount", allRetrieveCount),
					zap.Int64("countRet", countRet))
			}
		}
		resultCh <- RetrieveSegmentResult{
			result,
			s,
		}
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	err := doOnSegments(ctx, mgr, segments, retriever)
	close(resultCh)
	if err != nil {
		return nil, err
	}

	results := make([]RetrieveSegmentResult, 0, len(segments))
	for r := range resultCh {
		results = append(results, r)
	}
	return results, nil
}

func retrieveOnSegmentsWithStream(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan, svr streamrpc.QueryStreamServer) error {
	var (
		errs = make([]error, len(segments))
		wg   sync.WaitGroup
	)

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	for i, segment := range segments {
		wg.Add(1)
		go func(segment Segment, i int) {
			defer wg.Done()
			tr := timerecord.NewTimeRecorder("retrieveOnSegmentsWithStream")
			var result *segcorepb.RetrieveResults
			err := doOnSegment(ctx, mgr, segment, func(ctx context.Context, segment Segment) error {
				var err error
				result, err = segment.Retrieve(ctx, plan)
				return err
			})
			if err != nil {
				errs[i] = err
				return
			}

			if len(result.GetOffset()) != 0 {
				if err = svr.Send(&internalpb.RetrieveResults{
					Status:     merr.Success(),
					Ids:        result.GetIds(),
					FieldsData: result.GetFieldsData(),
					CostAggregation: &internalpb.CostAggregation{
						TotalRelatedDataSize: GetSegmentRelatedDataSize(segment),
					},
					SealedSegmentIDsRetrieved: []int64{segment.ID()},
					AllRetrieveCount:          result.GetAllRetrieveCount(),
				}); err != nil {
					errs[i] = err
				}
			}

			errs[i] = nil
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segment, i)
	}
	wg.Wait()
	return merr.Combine(errs...)
}

// retrieve will retrieve all the validate target segments
func Retrieve(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest) ([]RetrieveSegmentResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()
	log := log.Ctx(ctx)
	log.Debug("retrieve on segments", zap.Int64s("segmentIDs", segIDs), zap.Int64("collectionID", collID))

	// Detect if this is a distance query
	isDistQuery := req.GetReq().GetIsDistanceQuery()

	// 使用fmt.Printf确保输出
	fmt.Printf("=== RETRIEVE DEBUG: IsDistanceQuery=%v, RequestType=%T ===\n",
		isDistQuery, req.GetReq())

	log.Info("=== INFO: Check distance query flag ===",
		zap.Bool("IsDistanceQuery", isDistQuery),
		zap.String("requestType", fmt.Sprintf("%T", req.GetReq())))

	if isDistQuery {
		fmt.Printf("=== RETRIEVE DEBUG: Enter distance query executor ===\n")
		log.Info("detected distance query, routing to distance query executor")

		results, segments, err := routeToDistanceExecutor(ctx, manager, plan, req)
		log.Info("routeToDistanceExecutor返回",
			zap.Error(err),
			zap.Int("resultsCount", len(results)),
			zap.Int("segmentsCount", len(segments)))

		return results, segments, err
	} else {
		fmt.Printf("=== RETRIEVE DEBUG: Normal query flow ===\n")
		log.Info("普通查询，使用标准处理流程")
	}

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	}

	if err != nil {
		return nil, retrieveSegments, err
	}

	result, err := retrieveOnSegments(ctx, manager, retrieveSegments, SegType, plan, req)
	return result, retrieveSegments, err
}

// retrieveStreaming will retrieve all the validate target segments  and  return by stream
func RetrieveStream(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) ([]Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	}

	if err != nil {
		return retrieveSegments, err
	}

	err = retrieveOnSegmentsWithStream(ctx, manager, retrieveSegments, SegType, plan, srv)
	return retrieveSegments, err
}

// routeToDistanceExecutor routes distance queries to distance query executor
func routeToDistanceExecutor(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest) ([]RetrieveSegmentResult, []Segment, error) {
	log := log.Ctx(ctx)

	// 1. 获取集合schema
	collection := manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		return nil, nil, fmt.Errorf("集合不存在: %d", req.Req.GetCollectionID())
	}

	// 2. 获取相关的segments
	var segments []Segment
	var err error

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()

	if req.GetScope() == querypb.DataScope_Historical {
		segments, err = validateOnHistorical(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	} else {
		segments, err = validateOnStream(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs)
	}

	if err != nil {
		return nil, segments, err
	}

	// 3. 从 QueryRequest 中提取序列化的 PlanNode
	// QueryRequest 包含 SerializedExprPlan 字段
	log.Info("开始从QueryRequest提取PlanNode")

	// 从 QueryRequest 获取序列化的查询计划
	serializedPlan := req.GetReq().GetSerializedExprPlan()
	if len(serializedPlan) == 0 {
		return nil, segments, fmt.Errorf("QueryRequest中没有包含序列化的查询计划")
	}

	// 反序列化为 PlanNode
	var planNode planpb.PlanNode
	if err := proto.Unmarshal(serializedPlan, &planNode); err != nil {
		return nil, segments, fmt.Errorf("反序列化PlanNode失败: %w", err)
	}

	log.Info("成功提取PlanNode",
		zap.Bool("isQuery", planNode.GetQuery() != nil),
		zap.Bool("isDistanceQuery", planNode.GetQuery().GetIsDistanceQuery()),
		zap.Int("outputFieldIds", len(planNode.GetOutputFieldIds())))

	log.Debug("创建距离查询执行器",
		zap.Int("segmentCount", len(segments)),
		zap.Int64("collectionID", collID))

	// 4. 执行距离查询
	distanceResult, err := ExecuteDistanceQuery(ctx, manager, collection.Schema(), segments, &planNode, req)
	if err != nil {
		return nil, segments, fmt.Errorf("距离查询执行失败: %w", err)
	}

	log.Info("距离查询执行完成，准备转换结果格式",
		zap.Bool("distanceResultNotNil", distanceResult != nil),
		zap.Int("fieldsDataCount", len(distanceResult.GetFieldsData())),
		zap.String("idsType", fmt.Sprintf("%T", distanceResult.GetIds())))

	// 5. 转换结果格式为 RetrieveSegmentResult
	// 创建一个虚拟的segment来包装距离查询结果
	// 重要：必须设置Offset字段，否则会在MergeSegcoreRetrieveResults中被过滤掉

	// 计算结果数量
	resultCount := 0
	if distanceResult.Ids != nil {
		switch idField := distanceResult.Ids.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			resultCount = len(idField.IntId.GetData())
		case *schemapb.IDs_StrId:
			resultCount = len(idField.StrId.GetData())
		}
	}

	// 创建连续的offset数组
	offsets := make([]int64, resultCount)
	for i := 0; i < resultCount; i++ {
		offsets[i] = int64(i)
	}

	results := []RetrieveSegmentResult{
		{
			Result: &segcorepb.RetrieveResults{
				Ids:        distanceResult.Ids,
				FieldsData: distanceResult.FieldsData,
				Offset:     offsets, // 关键：设置Offset字段防止被过滤
			},
			Segment: nil, // 距离查询不对应特定的segment
		},
	}

	log.Info("结果格式转换完成",
		zap.Int("resultCount", len(results)),
		zap.Bool("resultNotNil", results[0].Result != nil),
		zap.Int("resultFieldsCount", len(results[0].Result.FieldsData)))

	log.Debug("距离查询执行完成",
		zap.Int("resultCount", len(results)))

	// 最终调试：确保结果正确返回
	log.Info("routeToDistanceExecutor即将返回结果",
		zap.Int("totalResults", len(results)),
		zap.Int("totalSegments", len(segments)),
		zap.Bool("hasResults", len(results) > 0))

	if len(results) > 0 {
		log.Info("第一个结果详情",
			zap.Bool("resultNotNil", results[0].Result != nil),
			zap.Int("fieldsCount", len(results[0].Result.FieldsData)))
	}

	return results, segments, nil
}
