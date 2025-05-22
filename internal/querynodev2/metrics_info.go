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
	"math"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	minTsafeChannel := ""
	minTsafe := uint64(math.MaxUint64)
	node.delegators.Range(func(channel string, delegator delegator.ShardDelegator) bool {
		tsafe := delegator.GetTSafe()
		if tsafe < minTsafe {
			minTsafeChannel = channel
			minTsafe = tsafe
		}
		return true
	})

	collections := node.manager.Collection.ListWithName()
	nodeID := fmt.Sprint(node.GetNodeID())

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

	for _, segs := range sealedGroupByCollection {
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

// getChannelJSON returns the JSON string of channels
func getChannelJSON(node *QueryNode, collectionID int64) string {
	stats := node.pipelineManager.GetChannelStats(collectionID)
	ret, err := json.Marshal(stats)
	if err != nil {
		log.Warn("failed to marshal channels", zap.Error(err))
		return ""
	}
	return string(ret)
}

// getSegmentJSON returns the JSON string of segments
func getSegmentJSON(node *QueryNode, collectionID int64) string {
	allSegments := node.manager.Segment.GetBy()
	var ms []*metricsinfo.Segment
	for _, s := range allSegments {
		if collectionID > 0 && s.Collection() != collectionID {
			continue
		}

		indexes := make([]*metricsinfo.IndexedField, 0, len(s.Indexes()))
		for _, index := range s.Indexes() {
			indexes = append(indexes, &metricsinfo.IndexedField{
				IndexFieldID: index.IndexInfo.FieldID,
				IndexID:      index.IndexInfo.IndexID,
				IndexSize:    index.IndexInfo.IndexSize,
				BuildID:      index.IndexInfo.BuildID,
				IsLoaded:     index.IsLoaded,
				HasRawData:   s.HasRawData(index.IndexInfo.FieldID),
			})
		}

		ms = append(ms, &metricsinfo.Segment{
			SegmentID:            s.ID(),
			CollectionID:         s.Collection(),
			PartitionID:          s.Partition(),
			MemSize:              s.MemSize(),
			IndexedFields:        indexes,
			State:                s.Type().String(),
			ResourceGroup:        s.ResourceGroup(),
			LoadedInsertRowCount: s.InsertCount(),
			NodeID:               node.GetNodeID(),
		})
	}

	ret, err := json.Marshal(ms)
	if err != nil {
		log.Warn("failed to marshal segments", zap.Error(err))
		return ""
	}
	return string(ret)
}

// getSystemInfoMetrics returns metrics info of QueryNode
func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (string, error) {
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := getQuotaMetrics(node)
	if err != nil {
		return "", err
	}

	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}

	hardwareInfos := metricsinfo.HardwareMetrics{
		IP:               node.session.Address,
		CPUCoreCount:     hardware.GetCPUNum(),
		CPUCoreUsage:     hardware.GetCPUUsage(),
		Memory:           totalMem,
		MemoryUsage:      usedMem,
		Disk:             total,
		DiskUsage:        used,
		IOWaitPercentage: ioWait,
	}
	quotaMetrics.Hms = hardwareInfos

	collectionMetrics, err := getCollectionMetrics(node)
	if err != nil {
		return "", err
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

	return metricsinfo.MarshalComponentInfos(nodeInfos)
}
