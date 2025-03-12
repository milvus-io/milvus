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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

// getQuotaMetrics returns QueryNodeQuotaMetrics.
func getQuotaMetrics(node *QueryNode) (*metricsinfo.QueryNodeQuotaMetrics, error) {
	rms, err := getRateMetric()
	if err != nil {
		return nil, err
	}

	minTsafeChannel, minTsafe := node.tSafeManager.Min()
	collections := node.manager.Collection.ListWithName()
	nodeID := fmt.Sprint(node.GetNodeID())

	metrics.QueryNodeNumEntities.Reset()
	metrics.QueryNodeEntitiesSize.Reset()

	var totalGrowingSize int64
	growingSegments := node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeGrowing))
	growingGroupByCollection := lo.GroupBy(growingSegments, func(seg segments.Segment) int64 {
		return seg.Collection()
	})
	for collection := range collections {
		segs := growingGroupByCollection[collection]
		size := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.MemSize()
		})
		totalGrowingSize += size
		metrics.QueryNodeEntitiesSize.WithLabelValues(nodeID, fmt.Sprint(collection),
			segments.SegmentTypeGrowing.String()).Set(float64(size))
	}

	for _, segs := range growingGroupByCollection {
		numEntities := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.RowNum()
		})
		segment := segs[0]
		metrics.QueryNodeNumEntities.WithLabelValues(
			segment.DatabaseName(),
			collections[segment.Collection()],
			nodeID,
			fmt.Sprint(segment.Collection()),
			segments.SegmentTypeGrowing.String(),
		).Set(float64(numEntities))
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeSealed))
	sealedGroupByCollection := lo.GroupBy(sealedSegments, func(seg segments.Segment) int64 {
		return seg.Collection()
	})
	for collection := range collections {
		segs := sealedGroupByCollection[collection]
		size := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.MemSize()
		})
		metrics.QueryNodeEntitiesSize.WithLabelValues(fmt.Sprint(node.GetNodeID()),
			fmt.Sprint(collection), segments.SegmentTypeSealed.String()).Set(float64(size))
	}
	sealedGroupByPartition := lo.GroupBy(sealedSegments, func(seg segments.Segment) int64 {
		return seg.Partition()
	})
	for _, segs := range sealedGroupByPartition {
		numEntities := lo.SumBy(segs, func(seg segments.Segment) int64 {
			return seg.RowNum()
		})
		segment := segs[0]
		metrics.QueryNodeNumEntities.WithLabelValues(
			segment.DatabaseName(),
			collections[segment.Collection()],
			nodeID,
			fmt.Sprint(segment.Collection()),
			segments.SegmentTypeSealed.String(),
		).Set(float64(numEntities))
	}

	deleteBufferNum := make(map[int64]int64)
	deleteBufferSize := make(map[int64]int64)

	node.delegators.Range(func(_ string, sd delegator.ShardDelegator) bool {
		collectionID := sd.Collection()
		entryNum, memorySize := sd.GetDeleteBufferSize()
		deleteBufferNum[collectionID] += entryNum
		deleteBufferSize[collectionID] += memorySize
		return true
	})

	return &metricsinfo.QueryNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphChannel: minTsafeChannel,
			MinFlowGraphTt:      minTsafe,
			NumFlowGraph:        node.pipelineManager.Num(),
		},
		GrowingSegmentsSize: totalGrowingSize,
		Effect: metricsinfo.NodeEffect{
			NodeID:        node.GetNodeID(),
			CollectionIDs: lo.Keys(collections),
		},
		DeleteBufferInfo: metricsinfo.DeleteBufferInfo{
			CollectionDeleteBufferNum:  deleteBufferNum,
			CollectionDeleteBufferSize: deleteBufferSize,
		},
	}, nil
}

func getCollectionMetrics(node *QueryNode) (*metricsinfo.QueryNodeCollectionMetrics, error) {
	allSegments := node.manager.Segment.GetBy()
	ret := &metricsinfo.QueryNodeCollectionMetrics{
		CollectionRows: make(map[int64]int64),
	}
	for _, segment := range allSegments {
		collectionID := segment.Collection()
		ret.CollectionRows[collectionID] += segment.RowNum()
	}
	return ret, nil
}

// getSystemInfoMetrics returns metrics info of QueryNode
func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (*milvuspb.GetMetricsResponse, error) {
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := getQuotaMetrics(node)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, node.GetNodeID()),
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

	collectionMetrics, err := getCollectionMetrics(node)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, node.GetNodeID()),
		}, nil
	}

	nodeInfos := metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name:          metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, node.GetNodeID()),
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
		QuotaMetrics:      quotaMetrics,
		CollectionMetrics: collectionMetrics,
	}
	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, node.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, node.GetNodeID()),
	}, nil
}
