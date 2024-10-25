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
	"encoding/json"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
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

// GetSyncTaskMetrics retrieves and aggregates the sync task metrics of the datanode.
func (s *Server) GetSyncTaskMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (string, error) {
	resp, err := s.requestDataNodeGetMetrics(ctx, req)
	if err != nil {
		return "", err
	}

	tasks := make(map[string][]*metricsinfo.SyncTask, resp.Len())
	resp.Range(func(key string, value *milvuspb.GetMetricsResponse) bool {
		if value.Response != "" {
			var sts []*metricsinfo.SyncTask
			if err1 := json.Unmarshal([]byte(value.Response), &sts); err1 != nil {
				log.Warn("failed to unmarshal sync task metrics")
				err = err1
				return false
			}
			tasks[key] = sts
		}
		return true
	})

	if err != nil {
		return "", err
	}

	if len(tasks) == 0 {
		return "", nil
	}

	bs, err := json.Marshal(tasks)
	if err != nil {
		return "", err
	}
	return (string)(bs), nil
}

func (s *Server) requestDataNodeGetMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (*typeutil.ConcurrentMap[string, *milvuspb.GetMetricsResponse], error) {
	nodes := s.cluster.GetSessions()

	rets := typeutil.NewConcurrentMap[string, *milvuspb.GetMetricsResponse]()
	wg, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		wg.Go(func() error {
			cli, err := node.GetOrCreateClient(ctx)
			if err != nil {
				return err
			}
			ret, err := cli.GetMetrics(ctx, req)
			if err != nil {
				return err
			}
			key := metricsinfo.ConstructComponentName(typeutil.DataNodeRole, node.NodeID())
			rets.Insert(key, ret)
			return nil
		})
	}

	err := wg.Wait()
	if err != nil {
		return nil, err
	}
	return rets, nil
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
	ret := metricsinfo.DataCoordInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           s.session.GetAddress(),
				CPUCoreCount: hardware.GetCPUNum(),
				CPUCoreUsage: hardware.GetCPUUsage(),
				Memory:       hardware.GetMemoryCount(),
				MemoryUsage:  hardware.GetUsedMemoryCount(),
				Disk:         hardware.GetDiskCount(),
				DiskUsage:    hardware.GetDiskUsage(),
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
