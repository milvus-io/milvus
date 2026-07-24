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

package querycoordv2

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/autoscale"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	loadResourcePrecheckSuggestKey      = "suggested_expand_percent"
	maxSegmentNumPerGetIndexInfoRequest = 1024
	maxConcurrentQueryNodeMetrics       = 16
	bytesPerMB                          = 1024 * 1024
	bytesPerGB                          = 1024 * 1024 * 1024
)

type autoscaleResourceUsage struct {
	memoryBytes int64
	diskBytes   int64
}

type queryNodeResourceStatus struct {
	available autoscaleResourceUsage
	capacity  autoscaleResourceUsage
}

func (usage autoscaleResourceUsage) isZero() bool {
	return usage.memoryBytes == 0 && usage.diskBytes == 0
}

func (usage autoscaleResourceUsage) multiply(factor int64) autoscaleResourceUsage {
	return autoscaleResourceUsage{
		memoryBytes: usage.memoryBytes * factor,
		diskBytes:   usage.diskBytes * factor,
	}
}

func (usage *autoscaleResourceUsage) add(other autoscaleResourceUsage) {
	usage.memoryBytes += other.memoryBytes
	usage.diskBytes += other.diskBytes
}

func (usage autoscaleResourceUsage) shortageFrom(current autoscaleResourceUsage) autoscaleResourceUsage {
	return autoscaleResourceUsage{
		memoryBytes: max(usage.memoryBytes-current.memoryBytes, 0),
		diskBytes:   max(usage.diskBytes-current.diskBytes, 0),
	}
}

type loadResourcePrecheckError struct {
	message                string
	suggestedExpandPercent int64
}

func (e *loadResourcePrecheckError) Error() string {
	return e.message
}

func (e *loadResourcePrecheckError) Unwrap() error {
	return merr.ErrServiceResourceInsufficient
}

func statusWithLoadResourcePrecheckSuggestion(err error) *commonpb.Status {
	status := merr.Status(err)
	var precheckErr *loadResourcePrecheckError
	if !errors.As(err, &precheckErr) {
		return status
	}
	status.Retriable = false
	if status.ExtraInfo == nil {
		status.ExtraInfo = make(map[string]string)
	}
	status.ExtraInfo[loadResourcePrecheckSuggestKey] = strconv.FormatInt(precheckErr.suggestedExpandPercent, 10)
	return status
}

func checkAutoscaleLoadResourceByRGWithCapacity(requiredByRG, availableByRG, capacityByRG map[string]autoscaleResourceUsage, globalCapacity, globalLimit autoscaleResourceUsage, autoscaleEnabled bool) error {
	totalShortage := autoscaleResourceUsage{}
	suggestedExpandPercent := int64(0)
	for rgName, required := range requiredByRG {
		available := availableByRG[rgName]
		if required.memoryBytes <= available.memoryBytes && required.diskBytes <= available.diskBytes {
			continue
		}
		shortage := required.shortageFrom(available)
		totalShortage.add(shortage)
		capacity := capacityByRG[rgName]
		suggestedExpandPercent = max(
			suggestedExpandPercent,
			max(
				calculateExpandPercentFromShortage(shortage.memoryBytes, capacity.memoryBytes),
				calculateExpandPercentFromShortage(shortage.diskBytes, capacity.diskBytes),
			),
		)
	}
	if totalShortage.isZero() {
		return nil
	}

	if autoscaleEnabled && canExpandWithinGlobalLimit(totalShortage, globalCapacity, globalLimit) {
		return nil
	}

	return newLoadResourcePrecheckError(suggestedExpandPercent)
}

func canExpandWithinGlobalLimit(shortage, globalCapacity, globalLimit autoscaleResourceUsage) bool {
	return canExpandResourceWithinLimit(shortage.memoryBytes, globalCapacity.memoryBytes, globalLimit.memoryBytes) &&
		canExpandResourceWithinLimit(shortage.diskBytes, globalCapacity.diskBytes, globalLimit.diskBytes)
}

func canExpandResourceWithinLimit(shortage, currentCapacity, limit int64) bool {
	if shortage <= 0 {
		return true
	}
	if limit <= 0 || currentCapacity >= limit {
		return false
	}
	return shortage <= limit-currentCapacity
}

func newLoadResourcePrecheckError(suggestedExpandPercent int64) error {
	return &loadResourcePrecheckError{
		message:                "Insufficient cluster resources. Please scale out the cluster or enable autoscaling. Suggested scale-out ratio: " + strconv.FormatInt(suggestedExpandPercent, 10) + "%",
		suggestedExpandPercent: suggestedExpandPercent,
	}
}

func newQueryNodeResourceMetricsUnavailableError(scope string) error {
	return merr.WrapErrServiceUnavailableMsg("query node resource metrics unavailable, scope=%s", scope)
}

func calculateExpandPercentFromShortage(shortage, capacity int64) int64 {
	if shortage <= 0 {
		return 0
	}
	if capacity <= 0 {
		return 100
	}
	return (shortage*100 + capacity - 1) / capacity
}

func (s *Server) checkLoadResource(ctx context.Context, req *job.AlterLoadConfigRequest) error {
	if !paramtable.Get().QueryCoordCfg.AutoscalePrecheckEnabled.GetAsBool() {
		return nil
	}

	err := s.evaluateLoadResource(ctx, req)
	if err == nil {
		return nil
	}

	var precheckErr *loadResourcePrecheckError
	if errors.As(err, &precheckErr) {
		return err
	}

	mlog.Warn(ctx, "skip load resource precheck due to internal error",
		mlog.Int64("collectionID", req.CollectionInfo.GetCollectionID()),
		mlog.Err(err),
	)
	return nil
}

func (s *Server) evaluateLoadResource(ctx context.Context, req *job.AlterLoadConfigRequest) error {
	requiredByRG, err := s.requiredLoadResourceByRG(ctx, req)
	if err != nil {
		return err
	}
	if len(requiredByRG) == 0 {
		return nil
	}

	autoscaleEnabled := paramtable.Get().QueryCoordCfg.AutoscaleEnabled.GetAsBool()
	currentByRG, capacityByRG, globalCapacity, err := s.currentQueryNodeResourceStatus(ctx, requiredByRG, autoscaleEnabled)
	if err != nil {
		return err
	}
	globalLimit := autoscaleGlobalLimitFromConfig(
		paramtable.Get().QueryCoordCfg.AutoscaleMaxMemoryLimit.GetAsInt64(),
		paramtable.Get().QueryCoordCfg.AutoscaleMaxDiskLimit.GetAsInt64(),
	)
	err = checkAutoscaleLoadResourceByRGWithCapacity(requiredByRG, currentByRG, capacityByRG, globalCapacity, globalLimit, autoscaleEnabled)
	mlog.Info(ctx, "autoscale load resource precheck",
		mlog.Int64("collectionID", req.CollectionInfo.GetCollectionID()),
		mlog.Bool("autoscaleEnabled", autoscaleEnabled),
		mlog.Bool("passed", err == nil),
		mlog.Any("requiredByRGMB", resourceUsageByRGToMB(requiredByRG)),
		mlog.Any("availableByRGMB", resourceUsageByRGToMB(currentByRG)),
		mlog.Any("shortageByRGMB", shortageByRGToMB(requiredByRG, currentByRG)),
		mlog.Any("capacityByRGMB", resourceUsageByRGToMB(capacityByRG)),
		mlog.Float64("globalCapacityMemoryMB", bytesToMB(globalCapacity.memoryBytes)),
		mlog.Float64("globalCapacityDiskMB", bytesToMB(globalCapacity.diskBytes)),
		mlog.Float64("globalLimitMemoryMB", bytesToMB(globalLimit.memoryBytes)),
		mlog.Float64("globalLimitDiskMB", bytesToMB(globalLimit.diskBytes)),
		mlog.Err(err),
	)
	return err
}

func resourceUsageByRGToMB(usages map[string]autoscaleResourceUsage) map[string]map[string]float64 {
	result := make(map[string]map[string]float64, len(usages))
	for rgName, usage := range usages {
		result[rgName] = map[string]float64{
			"memory": bytesToMB(usage.memoryBytes),
			"disk":   bytesToMB(usage.diskBytes),
		}
	}
	return result
}

func shortageByRGToMB(requiredByRG, currentByRG map[string]autoscaleResourceUsage) map[string]map[string]float64 {
	shortageByRG := make(map[string]autoscaleResourceUsage, len(requiredByRG))
	for rgName, required := range requiredByRG {
		shortageByRG[rgName] = required.shortageFrom(currentByRG[rgName])
	}
	return resourceUsageByRGToMB(shortageByRG)
}

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / bytesPerMB
}

func autoscaleGlobalLimitFromConfig(maxMemoryLimitGB, maxDiskLimitGB int64) autoscaleResourceUsage {
	return autoscaleResourceUsage{
		memoryBytes: maxMemoryLimitGB * bytesPerGB,
		diskBytes:   maxDiskLimitGB * bytesPerGB,
	}
}

func (s *Server) requiredLoadResourceByRG(ctx context.Context, req *job.AlterLoadConfigRequest) (map[string]autoscaleResourceUsage, error) {
	collectionID := req.CollectionInfo.GetCollectionID()
	expectedPartitionIDs := req.Expected.ExpectedPartitionIDs
	if len(expectedPartitionIDs) == 0 {
		return nil, nil
	}

	_, segments, err := s.broker.GetRecoveryInfoV2(ctx, collectionID, expectedPartitionIDs...)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, nil
	}

	segmentIDs := lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) int64 {
		return segment.GetID()
	})
	indexes, err := getIndexInfoBySegments(ctx, s.broker, collectionID, segmentIDs)
	if err != nil {
		return nil, err
	}

	expectedUsage, err := estimateLoadResourceForLoadConfig(
		req.CollectionInfo.GetSchema(),
		segments,
		indexes,
		req.Expected.ExpectedLoadFields,
		req.Expected.ExpectedFieldIndexID,
	)
	if err != nil {
		return nil, err
	}
	if expectedUsage.isZero() {
		return nil, nil
	}

	currentUsage := autoscaleResourceUsage{}
	currentReplicasNumber := map[string]int{}
	if req.Current.Collection != nil {
		currentPartitionSet := typeutil.NewSet(req.Current.GetPartitionIDs()...)
		currentSegments := filterSegmentsByPartitionIDs(segments, currentPartitionSet)
		if len(currentSegments) > 0 {
			currentUsage, err = estimateLoadResourceForLoadConfig(
				currentSchema(req),
				currentSegments,
				indexes,
				req.Current.GetLoadFields(),
				req.Current.GetFieldIndexID(),
			)
			if err != nil {
				return nil, err
			}
		}
		currentReplicasNumber = req.Current.GetReplicaNumber()
	}

	return buildRequiredLoadResourceByRGForLoadConfig(expectedUsage, currentUsage, currentReplicasNumber, req.Expected.ExpectedReplicaNumber), nil
}

func currentSchema(req *job.AlterLoadConfigRequest) *schemapb.CollectionSchema {
	if req.Current.Collection != nil && req.Current.Collection.Schema != nil {
		return req.Current.Collection.Schema
	}
	return req.CollectionInfo.GetSchema()
}

func estimateLoadResourceForLoadConfig(schema *schemapb.CollectionSchema, segments []*datapb.SegmentInfo, indexes map[int64][]*querypb.FieldIndexInfo, loadFields []int64, fieldIndexID map[int64]int64) (autoscaleResourceUsage, error) {
	usage, err := autoscale.EstimateSegmentsLoadResourceForLoadConfig(schema, segments, indexes, loadFields, fieldIndexID, autoscale.DefaultEstimateOptions())
	if err != nil {
		return autoscaleResourceUsage{}, err
	}
	return autoscaleResourceUsage{
		memoryBytes: usage.MemoryBytes,
		diskBytes:   usage.DiskBytes,
	}, nil
}

func getIndexInfoBySegments(ctx context.Context, broker meta.Broker, collectionID int64, segmentIDs []int64) (map[int64][]*querypb.FieldIndexInfo, error) {
	if len(segmentIDs) == 0 {
		return nil, nil
	}

	indexes := make(map[int64][]*querypb.FieldIndexInfo)
	for _, batch := range lo.Chunk(segmentIDs, maxSegmentNumPerGetIndexInfoRequest) {
		batchIndexes, err := broker.GetIndexInfo(ctx, collectionID, batch...)
		if err != nil {
			if errors.Is(err, merr.ErrIndexNotFound) {
				continue
			}
			return nil, err
		}
		for segmentID, infos := range batchIndexes {
			indexes[segmentID] = infos
		}
	}
	if len(indexes) == 0 {
		return nil, nil
	}
	return indexes, nil
}

func filterSegmentsByPartitionIDs(segments []*datapb.SegmentInfo, partitionIDs typeutil.Set[int64]) []*datapb.SegmentInfo {
	if len(partitionIDs) == 0 {
		return nil
	}
	return lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return partitionIDs.Contain(segment.GetPartitionID())
	})
}

func buildRequiredLoadResourceByRG(required autoscaleResourceUsage, expectedReplicasNumber map[string]int) map[string]autoscaleResourceUsage {
	requiredByRG := make(map[string]autoscaleResourceUsage)
	for rgName, num := range expectedReplicasNumber {
		if num <= 0 {
			continue
		}
		requiredByRG[rgName] = required.multiply(int64(num))
	}
	if len(requiredByRG) == 0 {
		requiredByRG[meta.DefaultResourceGroupName] = required
	}
	return requiredByRG
}

func buildRequiredLoadResourceByRGForLoadConfig(expectedUsage, currentUsage autoscaleResourceUsage, currentReplicasNumber, expectedReplicasNumber map[string]int) map[string]autoscaleResourceUsage {
	if len(expectedReplicasNumber) == 0 {
		return buildRequiredLoadResourceByRG(expectedUsage, expectedReplicasNumber)
	}

	loadDeltaForKeptReplica := expectedUsage.shortageFrom(currentUsage)
	requiredByRG := make(map[string]autoscaleResourceUsage)
	for rgName, expectedNum := range expectedReplicasNumber {
		if expectedNum <= 0 {
			continue
		}
		currentNum := currentReplicasNumber[rgName]
		required := autoscaleResourceUsage{}

		keptNum := min(currentNum, expectedNum)
		if keptNum > 0 {
			required.add(loadDeltaForKeptReplica.multiply(int64(keptNum)))
		}

		incomingNum := expectedNum - currentNum
		if incomingNum > 0 {
			required.add(expectedUsage.multiply(int64(incomingNum)))
		}
		if !required.isZero() {
			requiredByRG[rgName] = required
		}
	}
	return requiredByRG
}

func (s *Server) currentQueryNodeResourceStatus(ctx context.Context, requiredByRG map[string]autoscaleResourceUsage, includeGlobal bool) (map[string]autoscaleResourceUsage, map[string]autoscaleResourceUsage, autoscaleResourceUsage, error) {
	nodesByRG := make(map[string][]*session.NodeInfo, len(requiredByRG))
	nodesByID := make(map[int64]*session.NodeInfo)
	for rgName := range requiredByRG {
		nodes, err := s.queryNodesInResourceGroup(ctx, rgName)
		if err != nil {
			return nil, nil, autoscaleResourceUsage{}, err
		}
		nodesByRG[rgName] = nodes
		for _, node := range nodes {
			nodesByID[node.ID()] = node
		}
	}

	globalNodes := make([]*session.NodeInfo, 0)
	if includeGlobal {
		globalNodes = s.onlineQueryNodes()
		for _, node := range globalNodes {
			nodesByID[node.ID()] = node
		}
	}

	snapshot, err := s.queryNodeResourceSnapshot(ctx, lo.Values(nodesByID))
	if err != nil {
		return nil, nil, autoscaleResourceUsage{}, err
	}

	currentByRG := make(map[string]autoscaleResourceUsage, len(requiredByRG))
	capacityByRG := make(map[string]autoscaleResourceUsage, len(requiredByRG))
	for rgName, nodes := range nodesByRG {
		available, capacity, ok := queryNodeResourceStatusFromSnapshot(nodes, snapshot)
		if !ok {
			return nil, nil, autoscaleResourceUsage{}, newQueryNodeResourceMetricsUnavailableError("resource-group/" + rgName)
		}
		inflight := s.currentQueryNodeInflightLoadResource(nodes)
		mlog.Info(ctx, "deduct in-flight load resource from resource group available resource",
			mlog.String("rgName", rgName),
			mlog.Float64("inflightMemoryMB", bytesToMB(inflight.memoryBytes)),
			mlog.Float64("inflightDiskMB", bytesToMB(inflight.diskBytes)),
		)
		available.memoryBytes -= inflight.memoryBytes
		available.diskBytes -= inflight.diskBytes
		currentByRG[rgName] = available
		capacityByRG[rgName] = capacity
	}

	globalCapacity := autoscaleResourceUsage{}
	if includeGlobal {
		_, capacity, ok := queryNodeResourceStatusFromSnapshot(globalNodes, snapshot)
		if !ok {
			return nil, nil, autoscaleResourceUsage{}, newQueryNodeResourceMetricsUnavailableError("global")
		}
		globalCapacity = capacity
	}
	return currentByRG, capacityByRG, globalCapacity, nil
}

func (s *Server) currentQueryNodeInflightLoadResource(nodes []*session.NodeInfo) autoscaleResourceUsage {
	nodeIDs := lo.Map(nodes, func(node *session.NodeInfo, _ int) int64 {
		return node.ID()
	})
	usage := s.taskScheduler.GetAutoscaleInflightLoadResource(nodeIDs)
	return autoscaleResourceUsage{
		memoryBytes: usage.MemoryBytes,
		diskBytes:   usage.DiskBytes,
	}
}

func (s *Server) queryNodesInResourceGroup(ctx context.Context, rgName string) ([]*session.NodeInfo, error) {
	nodeIDs, err := s.meta.GetNodes(ctx, rgName)
	if err != nil {
		return nil, err
	}
	nodes := make([]*session.NodeInfo, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node := s.nodeMgr.Get(nodeID)
		if node == nil || node.IsStoppingState() {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (s *Server) onlineQueryNodes() []*session.NodeInfo {
	if s.nodeMgr == nil {
		return nil
	}
	return lo.Filter(s.nodeMgr.GetAll(), func(node *session.NodeInfo, _ int) bool {
		return node != nil && !node.IsStoppingState()
	})
}

func (s *Server) queryNodeResourceSnapshot(ctx context.Context, nodes []*session.NodeInfo) (map[int64]queryNodeResourceStatus, error) {
	snapshot := make(map[int64]queryNodeResourceStatus, len(nodes))
	if len(nodes) == 0 {
		return snapshot, nil
	}
	if s.cluster == nil {
		diskCapacityPerNode := paramtable.Get().QueryNodeCfg.DiskCacheCapacityLimit.GetAsInt64()
		memoryHighWatermark := paramtable.Get().QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat()
		for _, node := range nodes {
			capacity := autoscaleResourceUsage{diskBytes: diskCapacityPerNode}
			if node.MemCapacity() > 0 {
				capacity.memoryBytes = int64(node.MemCapacity() * 1024 * 1024 * memoryHighWatermark)
			}
			snapshot[node.ID()] = queryNodeResourceStatus{available: capacity, capacity: capacity}
		}
		return snapshot, nil
	}

	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to construct query node resource metrics request")
	}

	statuses := make([]queryNodeResourceStatus, len(nodes))
	metricsCtx := retry.WithMaxAttemptsContext(ctx, 1)
	group, metricsCtx := errgroup.WithContext(metricsCtx)
	group.SetLimit(min(maxConcurrentQueryNodeMetrics, len(nodes)))
	for index, node := range nodes {
		index, node := index, node
		group.Go(func() error {
			resp, err := s.cluster.GetMetrics(metricsCtx, node.ID(), req)
			if err := merr.CheckRPCCall(resp, err); err != nil {
				mlog.Warn(ctx, "failed to get query node resource metrics",
					mlog.Int64("nodeID", node.ID()),
					mlog.Err(err),
				)
				return merr.WrapErrServiceUnavailableMsg("query node resource metrics unavailable, nodeID=%d", node.ID())
			}
			nodeInfo := &metricsinfo.QueryNodeInfos{}
			if err := metricsinfo.UnmarshalComponentInfos(resp.GetResponse(), nodeInfo); err != nil {
				mlog.Warn(ctx, "failed to parse query node resource metrics",
					mlog.Int64("nodeID", node.ID()),
					mlog.Err(err),
				)
				return merr.WrapErrServiceInternalMsg("failed to parse query node resource metrics, nodeID=%d", node.ID())
			}
			hardware := nodeInfo.HardwareInfos
			if hardware.Memory == 0 {
				mlog.Warn(ctx, "query node resource metrics missing memory capacity",
					mlog.Int64("nodeID", node.ID()),
				)
				return merr.WrapErrServiceInternalMsg("query node resource metrics missing memory capacity, nodeID=%d", node.ID())
			}
			availableMemoryBytes, capacityMemoryBytes := memoryResourceFromMetrics(hardware.Memory, hardware.MemoryUsage)

			availableDiskBytes, capacityDiskBytes, ok := diskResourceFromMetrics(hardware.Disk, hardware.DiskUsage)
			if !ok {
				mlog.Warn(ctx, "query node resource metrics missing disk capacity",
					mlog.Int64("nodeID", node.ID()),
					mlog.Float64("diskGB", hardware.Disk),
					mlog.Float64("diskUsageGB", hardware.DiskUsage),
				)
				return merr.WrapErrServiceInternalMsg("query node resource metrics missing disk capacity, nodeID=%d", node.ID())
			}
			statuses[index] = queryNodeResourceStatus{
				available: autoscaleResourceUsage{memoryBytes: availableMemoryBytes, diskBytes: availableDiskBytes},
				capacity:  autoscaleResourceUsage{memoryBytes: capacityMemoryBytes, diskBytes: capacityDiskBytes},
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for index, node := range nodes {
		snapshot[node.ID()] = statuses[index]
	}
	return snapshot, nil
}

func queryNodeResourceStatusFromSnapshot(nodes []*session.NodeInfo, snapshot map[int64]queryNodeResourceStatus) (autoscaleResourceUsage, autoscaleResourceUsage, bool) {
	available := autoscaleResourceUsage{}
	capacity := autoscaleResourceUsage{}
	for _, node := range nodes {
		status, ok := snapshot[node.ID()]
		if !ok {
			return autoscaleResourceUsage{}, autoscaleResourceUsage{}, false
		}
		available.add(status.available)
		capacity.add(status.capacity)
	}
	return available, capacity, true
}

func memoryResourceFromMetrics(totalMemory, usedMemory uint64) (int64, int64) {
	highWatermark := paramtable.Get().QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat()
	memoryCapacity := uint64ToInt64(uint64(float64(totalMemory) * highWatermark))
	return memoryCapacity - uint64ToInt64(usedMemory), memoryCapacity
}

func diskResourceFromMetrics(totalDiskGB, usedDiskGB float64) (int64, int64, bool) {
	physicalDiskBytes := int64(totalDiskGB * 1e9)
	if physicalDiskBytes <= 0 {
		return 0, 0, false
	}
	configuredDiskCapacityBytes := paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64()
	if configuredDiskCapacityBytes <= 0 {
		return 0, 0, true
	}
	diskCapacityBytes := physicalDiskBytes
	if configuredDiskCapacityBytes > 0 && configuredDiskCapacityBytes < diskCapacityBytes {
		diskCapacityBytes = configuredDiskCapacityBytes
	}
	diskLimit := int64(float64(diskCapacityBytes) * paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())
	if diskLimit <= 0 {
		return 0, 0, true
	}
	usedDiskBytes := int64(usedDiskGB * 1e9)
	if diskLimit <= usedDiskBytes {
		return 0, diskLimit, true
	}
	return diskLimit - usedDiskBytes, diskLimit, true
}

func uint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}
