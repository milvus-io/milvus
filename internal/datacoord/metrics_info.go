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

package datacoord

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

// getQuotaMetrics returns DataCoordQuotaMetrics.
func (s *Server) getQuotaMetrics() *metricsinfo.DataCoordQuotaMetrics {
	info := s.meta.GetQuotaInfo()
	// Just generate the metrics data regularly
	_ = s.meta.SetStoredIndexFileSizeMetric()
	return info
}

func (s *Server) getCollectionMetrics(ctx context.Context) *metricsinfo.DataCoordCollectionMetrics {
	totalNumRows := s.meta.GetAllCollectionNumRows()
	ret := &metricsinfo.DataCoordCollectionMetrics{
		Collections: make(map[int64]*metricsinfo.DataCoordCollectionInfo, len(totalNumRows)),
	}
	for collectionID, total := range totalNumRows {
		if _, ok := ret.Collections[collectionID]; !ok {
			ret.Collections[collectionID] = &metricsinfo.DataCoordCollectionInfo{
				NumEntitiesTotal: 0,
				IndexInfo:        make([]*metricsinfo.DataCoordIndexInfo, 0),
			}
		}
		ret.Collections[collectionID].NumEntitiesTotal = total
		indexInfo, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
			CollectionID: collectionID,
			IndexName:    "",
			Timestamp:    0,
		})
		if err == merr.ErrIndexNotFound {
			log.Ctx(ctx).Debug("index not found, ignore to report index metrics",
				zap.Int64("collection", collectionID),
				zap.Error(err),
			)
			continue
		}
		if err := merr.CheckRPCCall(indexInfo, err); err != nil {
			log.Ctx(ctx).Warn("failed to describe index, ignore to report index metrics",
				zap.Int64("collection", collectionID),
				zap.Error(err),
			)
			continue
		}
		for _, info := range indexInfo.GetIndexInfos() {
			ret.Collections[collectionID].IndexInfo = append(ret.Collections[collectionID].IndexInfo, &metricsinfo.DataCoordIndexInfo{
				NumEntitiesIndexed: info.GetIndexedRows(),
				IndexName:          info.GetIndexName(),
				FieldID:            info.GetFieldID(),
			})
		}
	}
	return ret
}

func (s *Server) getChannelsJSON(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	channels, err := getMetrics[*metricsinfo.Channel](s, ctx, req)
	// fill checkpoint timestamp
	channel2Checkpoints := s.meta.GetChannelCheckpoints()
	for _, channel := range channels {
		if cp, ok := channel2Checkpoints[channel.Name]; ok {
			channel.CheckpointTS = tsoutil.PhysicalTimeFormat(cp.GetTimestamp())
		} else {
			log.Warn("channel not found in meta cache", zap.String("channel", channel.Name))
		}
	}
	return metricsinfo.MarshalGetMetricsValues(channels, err)
}

// mergeChannels merges the channel metrics from data nodes and channel watch infos from channel manager
// dnChannels: a slice of Channel metrics from data nodes
// dcChannels: a map of channel watch infos from the channel manager, keyed by node ID and channel name
func mergeChannels(dnChannels []*metricsinfo.Channel, dcChannels map[int64]map[string]*datapb.ChannelWatchInfo) []*metricsinfo.Channel {
	mergedChannels := make([]*metricsinfo.Channel, 0)

	// Add or update channels from data nodes
	for _, dnChannel := range dnChannels {
		if dcChannelMap, ok := dcChannels[dnChannel.NodeID]; ok {
			if dcChannel, ok := dcChannelMap[dnChannel.Name]; ok {
				dnChannel.WatchState = dcChannel.State.String()
				delete(dcChannelMap, dnChannel.Name)
			}
		}
		mergedChannels = append(mergedChannels, dnChannel)
	}

	// Add remaining channels from channel manager
	for nodeID, dcChannelMap := range dcChannels {
		for _, dcChannel := range dcChannelMap {
			mergedChannels = append(mergedChannels, &metricsinfo.Channel{
				Name:         dcChannel.Vchan.ChannelName,
				CollectionID: dcChannel.Vchan.CollectionID,
				WatchState:   dcChannel.State.String(),
				NodeID:       nodeID,
			})
		}
	}

	return mergedChannels
}

func (s *Server) getSegmentsJSON(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
	v := jsonReq.Get(metricsinfo.MetricRequestParamINKey)
	if !v.Exists() {
		// default to get all segments from datanode
		return s.getDataNodeSegmentsJSON(ctx, req)
	}

	in := v.String()
	if in == metricsinfo.MetricsRequestParamsInDN {
		return s.getDataNodeSegmentsJSON(ctx, req)
	}

	if in == metricsinfo.MetricsRequestParamsInDC {
		collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
		segments := s.meta.getSegmentsMetrics(collectionID)
		for _, seg := range segments {
			isIndexed, indexedFields := s.meta.indexMeta.GetSegmentIndexedFields(seg.CollectionID, seg.SegmentID)
			seg.IndexedFields = indexedFields
			seg.IsIndexed = isIndexed
		}

		bs, err := json.Marshal(segments)
		if err != nil {
			log.Ctx(ctx).Warn("marshal segment value failed", zap.Int64("collectionID", collectionID), zap.String("err", err.Error()))
			return "", nil
		}
		return string(bs), nil
	}
	return "", fmt.Errorf("invalid param value in=[%s], it should be dc or dn", in)
}

func (s *Server) getDistJSON(ctx context.Context, req *milvuspb.GetMetricsRequest) string {
	segments := s.meta.getSegmentsMetrics(-1)
	var channels []*metricsinfo.DmChannel
	for nodeID, ch := range s.channelManager.GetChannelWatchInfos() {
		for _, chInfo := range ch {
			dmChannel := metrics.NewDMChannelFrom(chInfo.GetVchan())
			dmChannel.NodeID = nodeID
			dmChannel.WatchState = chInfo.State.String()
			dmChannel.StartWatchTS = typeutil.TimestampToString(uint64(chInfo.GetStartTs()))
			channels = append(channels, dmChannel)
		}
	}

	dist := &metricsinfo.DataCoordDist{
		Segments:   segments,
		DMChannels: channels,
	}

	bs, err := json.Marshal(dist)
	if err != nil {
		log.Warn("marshal dist value failed", zap.String("err", err.Error()))
		return ""
	}
	return string(bs)
}

func (s *Server) getDataNodeSegmentsJSON(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	ret, err := getMetrics[*metricsinfo.Segment](s, ctx, req)
	return metricsinfo.MarshalGetMetricsValues(ret, err)
}

func (s *Server) getSyncTaskJSON(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	ret, err := getMetrics[*metricsinfo.SyncTask](s, ctx, req)
	return metricsinfo.MarshalGetMetricsValues(ret, err)
}

// getSystemInfoMetrics composes data cluster metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (string, error) {
	// TODO(dragondriver): add more detail metrics

	// get datacoord info
	nodes := s.cluster.GetSessions()
	clusterTopology := metricsinfo.DataClusterTopology{
		Self:                s.getDataCoordMetrics(ctx),
		ConnectedDataNodes:  make([]metricsinfo.DataNodeInfos, 0, len(nodes)),
		ConnectedIndexNodes: make([]metricsinfo.IndexNodeInfos, 0),
	}

	// for each data node, fetch metrics info
	for _, node := range nodes {
		infos, err := s.getDataNodeMetrics(ctx, req, node)
		if err != nil {
			log.Warn("fails to get DataNode metrics", zap.Error(err))
			continue
		}
		clusterTopology.ConnectedDataNodes = append(clusterTopology.ConnectedDataNodes, infos)
	}

	indexNodes := s.indexNodeManager.GetAllClients()
	for _, node := range indexNodes {
		infos, err := s.getIndexNodeMetrics(ctx, req, node)
		if err != nil {
			log.Warn("fails to get IndexNode metrics", zap.Error(err))
			continue
		}
		clusterTopology.ConnectedIndexNodes = append(clusterTopology.ConnectedIndexNodes, infos)
	}

	// compose topolgoy struct
	coordTopology := metricsinfo.DataCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	ret, err := metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		return "", err
	}
	return ret, nil
}

// getDataCoordMetrics composes datacoord infos
func (s *Server) getDataCoordMetrics(ctx context.Context) metricsinfo.DataCoordInfos {
	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}

	ret := metricsinfo.DataCoordInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:               s.session.GetAddress(),
				CPUCoreCount:     hardware.GetCPUNum(),
				CPUCoreUsage:     hardware.GetCPUUsage(),
				Memory:           hardware.GetMemoryCount(),
				MemoryUsage:      hardware.GetUsedMemoryCount(),
				Disk:             total,
				DiskUsage:        used,
				IOWaitPercentage: ioWait,
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: paramtable.GetCreateTime().String(),
			UpdatedTime: paramtable.GetUpdateTime().String(),
			Type:        typeutil.DataCoordRole,
			ID:          paramtable.GetNodeID(),
		},
		SystemConfigurations: metricsinfo.DataCoordConfiguration{
			SegmentMaxSize: Params.DataCoordCfg.SegmentMaxSize.GetAsFloat(),
		},
		QuotaMetrics:      s.getQuotaMetrics(),
		CollectionMetrics: s.getCollectionMetrics(ctx),
	}

	metricsinfo.FillDeployMetricsWithEnv(&ret.BaseComponentInfos.SystemInfo)

	return ret
}

// getDataNodeMetrics composes DataNode infos
// this function will invoke GetMetrics with DataNode specified in NodeInfo
func (s *Server) getDataNodeMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *session.Session) (metricsinfo.DataNodeInfos, error) {
	infos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HasError: true,
			ID:       int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		},
	}
	if node == nil {
		return infos, errors.New("DataNode is nil")
	}

	cli, err := node.GetOrCreateClient(ctx)
	if err != nil {
		return infos, err
	}

	metrics, err := cli.GetMetrics(ctx, req)
	if err != nil {
		log.Warn("invalid metrics of DataNode was found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		// err handled, returns nil
		return infos, nil
	}
	infos.BaseComponentInfos.Name = metrics.GetComponentName()

	if metrics.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("invalid metrics of DataNode was found",
			zap.Any("error_code", metrics.GetStatus().GetErrorCode()),
			zap.Any("error_reason", metrics.GetStatus().GetReason()))
		infos.BaseComponentInfos.ErrorReason = metrics.GetStatus().GetReason()
		return infos, nil
	}

	err = metricsinfo.UnmarshalComponentInfos(metrics.GetResponse(), &infos)
	if err != nil {
		log.Warn("invalid metrics of DataNode found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		return infos, nil
	}
	infos.BaseComponentInfos.HasError = false
	return infos, nil
}

func (s *Server) getIndexNodeMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node types.IndexNodeClient) (metricsinfo.IndexNodeInfos, error) {
	infos := metricsinfo.IndexNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HasError: true,
			ID:       int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		},
	}
	if node == nil {
		return infos, errors.New("IndexNode is nil")
	}

	metrics, err := node.GetMetrics(ctx, req)
	if err != nil {
		log.Warn("invalid metrics of IndexNode was found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		// err handled, returns nil
		return infos, nil
	}
	infos.BaseComponentInfos.Name = metrics.GetComponentName()

	if metrics.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("invalid metrics of DataNode was found",
			zap.Any("error_code", metrics.GetStatus().GetErrorCode()),
			zap.Any("error_reason", metrics.GetStatus().GetReason()))
		infos.BaseComponentInfos.ErrorReason = metrics.GetStatus().GetReason()
		return infos, nil
	}

	err = metricsinfo.UnmarshalComponentInfos(metrics.GetResponse(), &infos)
	if err != nil {
		log.Warn("invalid metrics of IndexNode found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		return infos, nil
	}
	infos.BaseComponentInfos.HasError = false
	return infos, nil
}

// getMetrics retrieves and aggregates the metrics of the datanode to a slice
func getMetrics[T any](s *Server, ctx context.Context, req *milvuspb.GetMetricsRequest) ([]T, error) {
	var metrics []T
	var mu sync.Mutex
	errorGroup, ctx := errgroup.WithContext(ctx)

	nodes := s.cluster.GetSessions()
	for _, node := range nodes {
		errorGroup.Go(func() error {
			cli, err := node.GetOrCreateClient(ctx)
			if err != nil {
				return err
			}
			resp, err := cli.GetMetrics(ctx, req)
			if err != nil {
				log.Warn("failed to get metric from DataNode", zap.Int64("nodeID", node.NodeID()))
				return err
			}

			if resp.Response == "" {
				return nil
			}

			var infos []T
			err = json.Unmarshal([]byte(resp.Response), &infos)
			if err != nil {
				log.Warn("invalid metrics of data node was found", zap.Error(err))
				return err
			}

			mu.Lock()
			metrics = append(metrics, infos...)
			mu.Unlock()
			return nil
		})
	}

	err := errorGroup.Wait()
	return metrics, err
}
