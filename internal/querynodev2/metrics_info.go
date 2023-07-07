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

package querynodev2

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func getRateMetric() ([]metricsinfo.RateMetric, error) {
	rms := make([]metricsinfo.RateMetric, 0)
	for _, label := range collector.RateMetrics() {
		rate, err := collector.Rate.Rate(label, ratelimitutil.DefaultAvgDuration)
		if err != nil {
			return nil, err
		}

		rms = append(rms, metricsinfo.RateMetric{
			Label: label,
			Rate:  rate,
		})
	}
	return rms, nil
}

func getSearchNQInQueue() (metricsinfo.ReadInfoInQueue, error) {
	average, err := collector.Average.Average(metricsinfo.SearchQueueMetric)
	if err != nil {
		return metricsinfo.ReadInfoInQueue{}, err
	}
	defer collector.Average.Reset(metricsinfo.SearchQueueMetric)

	readyQueueLabel := collector.ConstructLabel(metricsinfo.ReadyQueueType, metricsinfo.SearchQueueMetric)
	executeQueueLabel := collector.ConstructLabel(metricsinfo.ExecuteQueueType, metricsinfo.SearchQueueMetric)

	return metricsinfo.ReadInfoInQueue{
		ReadyQueue:       collector.Counter.Get(readyQueueLabel),
		ExecuteChan:      collector.Counter.Get(executeQueueLabel),
		AvgQueueDuration: time.Duration(int64(average)),
	}, nil
}

func getQueryTasksInQueue() (metricsinfo.ReadInfoInQueue, error) {
	average, err := collector.Average.Average(metricsinfo.QueryQueueMetric)
	if err != nil {
		return metricsinfo.ReadInfoInQueue{}, err
	}
	defer collector.Average.Reset(metricsinfo.QueryQueueMetric)

	readyQueueLabel := collector.ConstructLabel(metricsinfo.ReadyQueueType, metricsinfo.QueryQueueMetric)
	executeQueueLabel := collector.ConstructLabel(metricsinfo.ExecuteQueueType, metricsinfo.QueryQueueMetric)

	return metricsinfo.ReadInfoInQueue{
		ReadyQueue:       collector.Counter.Get(readyQueueLabel),
		ExecuteChan:      collector.Counter.Get(executeQueueLabel),
		AvgQueueDuration: time.Duration(int64(average)),
	}, nil
}

// getQuotaMetrics returns QueryNodeQuotaMetrics.
func getQuotaMetrics(node *QueryNode) (*metricsinfo.QueryNodeQuotaMetrics, error) {
	rms, err := getRateMetric()
	if err != nil {
		return nil, err
	}

	sqms, err := getSearchNQInQueue()
	if err != nil {
		return nil, err
	}

	qqms, err := getQueryTasksInQueue()
	if err != nil {
		return nil, err
	}

	minTsafeChannel, minTsafe := node.tSafeManager.Min()

	var totalGrowingSize int64
	growingSegments := node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeGrowing))
	growingGroupByCollection := lo.GroupBy(growingSegments, func(seg segments.Segment) int64 {
		return seg.Collection()
	})
	for collection, segs := range growingGroupByCollection {
		size := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.MemSize()
		})
		totalGrowingSize += size
		metrics.QueryNodeEntitiesSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			fmt.Sprint(collection), segments.SegmentTypeGrowing.String()).Set(float64(size))
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeSealed))
	sealedGroupByCollection := lo.GroupBy(sealedSegments, func(seg segments.Segment) int64 {
		return seg.Collection()
	})
	for collection, segs := range sealedGroupByCollection {
		size := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.MemSize()
		})
		metrics.QueryNodeEntitiesSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			fmt.Sprint(collection), segments.SegmentTypeSealed.String()).Set(float64(size))
	}

	allSegments := node.manager.Segment.GetBy()
	collections := typeutil.NewUniqueSet()
	for _, segment := range allSegments {
		collections.Insert(segment.Collection())
	}

	return &metricsinfo.QueryNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphChannel: minTsafeChannel,
			MinFlowGraphTt:      minTsafe,
			NumFlowGraph:        node.pipelineManager.Num(),
		},
		SearchQueue:         sqms,
		QueryQueue:          qqms,
		GrowingSegmentsSize: totalGrowingSize,
		Effect: metricsinfo.NodeEffect{
			NodeID:        paramtable.GetNodeID(),
			CollectionIDs: collections.Collect(),
		},
	}, nil
}

// getSystemInfoMetrics returns metrics info of QueryNode
func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (*milvuspb.GetMetricsResponse, error) {
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := getQuotaMetrics(node)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
		}, nil
	}
	hardwareInfos := metricsinfo.HardwareMetrics{
		IP:           node.session.Address,
		CPUCoreCount: hardware.GetCPUNum(),
		CPUCoreUsage: hardware.GetCPUUsage(),
		Memory:       totalMem,
		MemoryUsage:  usedMem,
		Disk:         hardware.GetDiskCount(),
		DiskUsage:    hardware.GetDiskUsage(),
	}
	quotaMetrics.Hms = hardwareInfos

	nodeInfos := metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name:          metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, paramtable.GetNodeID()),
			HardwareInfos: hardwareInfos,
			SystemInfo:    metricsinfo.DeployMetrics{},
			CreatedTime:   paramtable.GetCreateTime().String(),
			UpdatedTime:   paramtable.GetUpdateTime().String(),
			Type:          typeutil.QueryNodeRole,
			ID:            node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.QueryNodeConfiguration{
			SimdType: paramtable.Get().CommonCfg.SimdType.GetValue(),
		},
		QuotaMetrics: quotaMetrics,
	}
	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, paramtable.GetNodeID()),
	}, nil
}
