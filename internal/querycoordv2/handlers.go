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
	"sync"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

// checkAnyReplicaAvailable checks if the collection has enough distinct available shards. These shards
// may come from different replica group. We only need these shards to form a replica that serves query
// requests.
func (s *Server) checkAnyReplicaAvailable(collectionID int64) bool {
	return s.anyReplicaAvailable(s.meta.GetByCollection(s.ctx, collectionID))
}

// checkAnyReplicaAvailableInResourceGroup is the answer a ShowLoadCollections
// scoped to a resource group gives: whether THAT group can serve the
// collection, which is the group's shard-leader readiness - every shard of
// the collection has a serviceable leader in the group's replicas, on a node
// the coordinator knows (utils.ShardLeaderReadinessByResourceGroup, the same
// verdict the scoped load waits for). It is not the collection-wide rule
// restricted to one group: that rule reads a replica with no read-only node
// as available, so a replica just spawned into the group, holding nothing,
// would read true beside a progress of 0.
func (s *Server) checkAnyReplicaAvailableInResourceGroup(ctx context.Context, collectionID int64, rgName string) bool {
	readiness, err := utils.ShardLeaderReadinessByResourceGroup(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr, collectionID, rgName)
	if err != nil {
		mlog.Warn(ctx, "the resource group's shard-leader readiness cannot be answered, reporting it as not serving",
			mlog.Int64("collectionID", collectionID), mlog.String("resourceGroup", rgName), mlog.Err(err))
		return false
	}
	return readiness.Ready
}

// anyReplicaAvailable is the collection-wide rule: a replica is available when
// every one of its read-only nodes is still known to the node manager, and
// the answer is yes as soon as one replica is.
func (s *Server) anyReplicaAvailable(replicas []*meta.Replica) bool {
	for _, replica := range replicas {
		isAvailable := true
		for _, node := range replica.GetRONodes() {
			if s.nodeMgr.Get(node) == nil {
				isAvailable = false
				break
			}
		}
		if isAvailable {
			return true
		}
	}
	return false
}

func (s *Server) getCollectionSegmentInfo(ctx context.Context, collection int64) []*querypb.SegmentInfo {
	segments := s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collection))
	currentTargetSegmentsMap := s.targetMgr.GetSealedSegmentsByCollection(ctx, collection, meta.CurrentTarget)
	infos := make(map[int64]*querypb.SegmentInfo)
	for _, segment := range segments {
		if _, existCurrentTarget := currentTargetSegmentsMap[segment.GetID()]; !existCurrentTarget {
			// if one segment exists in distMap but doesn't exist in currentTargetMap
			// in order to guarantee that get segment request launched by sdk could get
			// consistent result, for example
			// sdk insert three segments:A, B, D, then A + B----compact--> C
			// In this scenario, we promise that clients see either 2 segments(C,D) or 3 segments(A, B, D)
			// rather than 4 segments(A, B, C, D), in which query nodes are loading C but have completed loading process
			mlog.Info(context.TODO(), "filtered segment being in the intermediate status",
				mlog.Int64("segmentID", segment.GetID()))
			continue
		}
		info, ok := infos[segment.GetID()]
		if !ok {
			info = &querypb.SegmentInfo{}
			infos[segment.GetID()] = info
		}
		utils.MergeMetaSegmentIntoSegmentInfo(info, segment)
	}

	return lo.Values(infos)
}

func getMetrics[T any](ctx context.Context, s *Server, req *milvuspb.GetMetricsRequest) ([]T, error) {
	var metrics []T
	var mu sync.Mutex
	errorGroup, ctx := errgroup.WithContext(ctx)

	for _, node := range s.nodeMgr.GetAll() {
		node := node
		errorGroup.Go(func() error {
			resp, err := s.cluster.GetMetrics(ctx, node.ID(), req)
			if err := merr.CheckRPCCall(resp, err); err != nil {
				mlog.Warn(ctx, "failed to get metric from QueryNode", mlog.Int64("nodeID", node.ID()))
				return err
			}

			if resp.Response == "" {
				return nil
			}

			infos := make([]T, 0)
			err = json.Unmarshal([]byte(resp.Response), &infos)
			if err != nil {
				mlog.Warn(context.TODO(), "invalid metrics of query node was found", mlog.Err(err))
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

func (s *Server) getChannelsFromQueryNode(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	channels, err := getMetrics[*metricsinfo.Channel](ctx, s, req)
	return metricsinfo.MarshalGetMetricsValues(channels, err)
}

func (s *Server) getSegmentsFromQueryNode(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	segments, err := getMetrics[*metricsinfo.Segment](ctx, s, req)
	return metricsinfo.MarshalGetMetricsValues(segments, err)
}

func (s *Server) getSegmentsJSON(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
	v := jsonReq.Get(metricsinfo.MetricRequestParamINKey)
	if !v.Exists() {
		// default to get all segments from dataanode
		return s.getSegmentsFromQueryNode(ctx, req)
	}

	in := v.String()
	if in == metricsinfo.MetricsRequestParamsInQN {
		// TODO: support filter by collection id
		return s.getSegmentsFromQueryNode(ctx, req)
	}

	if in == metricsinfo.MetricsRequestParamsInQC {
		collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
		filteredSegments := s.dist.SegmentDistManager.GetSegmentDist(collectionID)
		bs, err := json.Marshal(filteredSegments)
		if err != nil {
			mlog.Warn(context.TODO(), "marshal segment value failed", mlog.Int64("collectionID", collectionID), mlog.String("err", err.Error()))
			return "", nil
		}
		return string(bs), nil
	}
	return "", merr.WrapErrParameterInvalidMsg("invalid param value in=[%s], it should be qc or qn", in)
}

// TODO(dragondriver): add more detail metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (string, error) {
	coordTopology := s.getQueryCoordTopology(ctx, req)
	resp, err := metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		return "", err
	}
	return resp, nil
}

// getQueryCoordTopology returns QueryCoord topology directly without JSON serialization
// This is optimized for in-process calls in MixCoord mode to avoid marshal/unmarshal overhead
func (s *Server) getQueryCoordTopology(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) metricsinfo.QueryCoordTopology {
	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		mlog.Warn(ctx, "get disk usage failed", mlog.Err(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		mlog.Warn(ctx, "get iowait failed", mlog.Err(err))
	}

	clusterTopology := metricsinfo.QueryClusterTopology{
		Self: metricsinfo.QueryCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, paramtable.GetNodeID()),
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
				Type:        typeutil.QueryCoordRole,
				ID:          paramtable.GetNodeID(),
			},
			SystemConfigurations: metricsinfo.QueryCoordConfiguration{},
		},
		ConnectedNodes: make([]metricsinfo.QueryNodeInfos, 0),
	}
	metricsinfo.FillDeployMetricsWithEnv(&clusterTopology.Self.SystemInfo)
	nodesMetrics := s.tryGetNodesMetrics(ctx, req, s.nodeMgr.GetAll()...)
	s.fillMetricsWithNodes(&clusterTopology, nodesMetrics)

	return metricsinfo.QueryCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, paramtable.GetNodeID()),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}
}

func (s *Server) fillMetricsWithNodes(topo *metricsinfo.QueryClusterTopology, nodeMetrics []*metricResp) {
	for _, metric := range nodeMetrics {
		if metric.err != nil {
			mlog.Warn(context.TODO(), "invalid metrics of query node was found",
				mlog.Err(metric.err))
			topo.ConnectedNodes = append(topo.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: metric.err.Error(),
					// Name doesn't matter here because we can't get it when error occurs, using address as the Name?
					Name: "",
					ID:   int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}

		if metric.resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			mlog.Warn(context.TODO(), "invalid metrics of query node was found",
				mlog.Any("error_code", metric.resp.GetStatus().GetErrorCode()),
				mlog.Any("error_reason", metric.resp.GetStatus().GetReason()))
			topo.ConnectedNodes = append(topo.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: metric.resp.GetStatus().GetReason(),
					Name:        metric.resp.ComponentName,
					ID:          int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}

		infos := metricsinfo.QueryNodeInfos{}
		err := metricsinfo.UnmarshalComponentInfos(metric.resp.Response, &infos)
		if err != nil {
			mlog.Warn(context.TODO(), "invalid metrics of query node was found",
				mlog.Err(err))
			topo.ConnectedNodes = append(topo.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: err.Error(),
					Name:        metric.resp.ComponentName,
					ID:          int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}
		// If this query node is embedded in a streaming node, relabel it as streamingnode.
		if nodeInfo := s.nodeMgr.Get(infos.ID); nodeInfo != nil && nodeInfo.IsEmbeddedQueryNodeInStreamingNode() {
			infos.Type = typeutil.StreamingNodeRole
			infos.Name = metricsinfo.ConstructComponentName(typeutil.StreamingNodeRole, infos.ID)
		}
		topo.ConnectedNodes = append(topo.ConnectedNodes, infos)
	}
}

type metricResp struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

func (s *Server) tryGetNodesMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, nodes ...*session.NodeInfo) []*metricResp {
	wg := sync.WaitGroup{}
	ret := make([]*metricResp, 0, len(nodes))
	retCh := make(chan *metricResp, len(nodes))
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := s.cluster.GetMetrics(ctx, node.ID(), req)
			if err != nil {
				mlog.Warn(ctx, "failed to get metric from QueryNode",
					mlog.Int64("nodeID", node.ID()))
				return
			}
			retCh <- &metricResp{
				resp: resp,
				err:  err,
			}
		}()
	}
	wg.Wait()
	close(retCh)
	for resp := range retCh {
		ret = append(ret, resp)
	}
	return ret
}

// GetQueryCoordTopology returns QueryCoord topology directly without JSON serialization
// This is optimized for in-process calls in MixCoord mode to avoid marshal/unmarshal overhead
func (s *Server) GetQueryCoordTopology(ctx context.Context, req *milvuspb.GetMetricsRequest) (*metricsinfo.QueryCoordTopology, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return nil, err
	}
	topology := s.getQueryCoordTopology(ctx, req)
	return &topology, nil
}
