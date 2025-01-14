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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

// checkAnyReplicaAvailable checks if the collection has enough distinct available shards. These shards
// may come from different replica group. We only need these shards to form a replica that serves query
// requests.
func (s *Server) checkAnyReplicaAvailable(collectionID int64) bool {
	for _, replica := range s.meta.ReplicaManager.GetByCollection(s.ctx, collectionID) {
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
			log.Info("filtered segment being in the intermediate status",
				zap.Int64("segmentID", segment.GetID()))
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

// generate balance segment task and submit to scheduler
// if sync is true, this func call will wait task to finish, until reach the segment task timeout
// if copyMode is true, this func call will generate a load segment task, instead a balance segment task
func (s *Server) balanceSegments(ctx context.Context,
	collectionID int64,
	replica *meta.Replica,
	srcNode int64,
	dstNodes []int64,
	segments []*meta.Segment,
	sync bool,
	copyMode bool,
) error {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.Int64("srcNode", srcNode))
	plans := s.getBalancerFunc().AssignSegment(ctx, collectionID, segments, dstNodes, true)
	for i := range plans {
		plans[i].From = srcNode
		plans[i].Replica = replica
	}
	tasks := make([]task.Task, 0, len(plans))
	for _, plan := range plans {
		log.Info("manually balance segment...",
			zap.Int64("replica", plan.Replica.GetID()),
			zap.String("channel", plan.Segment.InsertChannel),
			zap.Int64("from", plan.From),
			zap.Int64("to", plan.To),
			zap.Int64("segmentID", plan.Segment.GetID()),
		)
		actions := make([]task.Action, 0)
		loadAction := task.NewSegmentActionWithScope(plan.To, task.ActionTypeGrow, plan.Segment.GetInsertChannel(), plan.Segment.GetID(), querypb.DataScope_Historical)
		actions = append(actions, loadAction)
		if !copyMode {
			// if in copy mode, the release action will be skip
			releaseAction := task.NewSegmentActionWithScope(plan.From, task.ActionTypeReduce, plan.Segment.GetInsertChannel(), plan.Segment.GetID(), querypb.DataScope_Historical)
			actions = append(actions, releaseAction)
		}

		t, err := task.NewSegmentTask(ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
			utils.ManualBalance,
			collectionID,
			plan.Replica,
			actions...,
		)
		if err != nil {
			log.Warn("create segment task for balance failed",
				zap.Int64("replica", plan.Replica.GetID()),
				zap.String("channel", plan.Segment.InsertChannel),
				zap.Int64("from", plan.From),
				zap.Int64("to", plan.To),
				zap.Int64("segmentID", plan.Segment.GetID()),
				zap.Error(err),
			)
			continue
		}
		t.SetReason("manual balance")
		// set manual balance to normal, to avoid manual balance be canceled by other segment task
		t.SetPriority(task.TaskPriorityNormal)
		err = s.taskScheduler.Add(t)
		if err != nil {
			t.Cancel(err)
			log.Info("skip balance segment task", zap.Int64("segmentID", plan.Segment.GetID()), zap.Error(err))
			continue
		}
		tasks = append(tasks, t)
	}

	if sync {
		err := task.Wait(ctx, Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), tasks...)
		if err != nil {
			msg := "failed to wait all balance task finished"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
	}

	return nil
}

// generate balance channel task and submit to scheduler
// if sync is true, this func call will wait task to finish, until reach the channel task timeout
// if copyMode is true, this func call will generate a load channel task, instead a balance channel task
func (s *Server) balanceChannels(ctx context.Context,
	collectionID int64,
	replica *meta.Replica,
	srcNode int64,
	dstNodes []int64,
	channels []*meta.DmChannel,
	sync bool,
	copyMode bool,
) error {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	plans := s.getBalancerFunc().AssignChannel(ctx, collectionID, channels, dstNodes, true)
	for i := range plans {
		plans[i].From = srcNode
		plans[i].Replica = replica
	}

	tasks := make([]task.Task, 0, len(plans))
	for _, plan := range plans {
		log.Info("manually balance channel...",
			zap.Int64("replica", plan.Replica.GetID()),
			zap.String("channel", plan.Channel.GetChannelName()),
			zap.Int64("from", plan.From),
			zap.Int64("to", plan.To),
		)

		actions := make([]task.Action, 0)
		loadAction := task.NewChannelAction(plan.To, task.ActionTypeGrow, plan.Channel.GetChannelName())
		actions = append(actions, loadAction)
		if !copyMode {
			// if in copy mode, the release action will be skip
			releaseAction := task.NewChannelAction(plan.From, task.ActionTypeReduce, plan.Channel.GetChannelName())
			actions = append(actions, releaseAction)
		}
		t, err := task.NewChannelTask(s.ctx,
			Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond),
			utils.ManualBalance,
			collectionID,
			plan.Replica,
			actions...,
		)
		if err != nil {
			log.Warn("create channel task for balance failed",
				zap.Int64("replica", plan.Replica.GetID()),
				zap.String("channel", plan.Channel.GetChannelName()),
				zap.Int64("from", plan.From),
				zap.Int64("to", plan.To),
				zap.Error(err),
			)
			continue
		}
		t.SetReason("manual balance")
		// set manual balance channel to high, to avoid manual balance be canceled by other channel task
		t.SetPriority(task.TaskPriorityHigh)
		err = s.taskScheduler.Add(t)
		if err != nil {
			t.Cancel(err)
			log.Info("skip balance channel task", zap.String("channel", plan.Channel.GetChannelName()), zap.Error(err))
			continue
		}
		tasks = append(tasks, t)
	}

	if sync {
		err := task.Wait(ctx, Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), tasks...)
		if err != nil {
			msg := "failed to wait all balance task finished"
			log.Warn(msg, zap.Error(err))
			return errors.Wrap(err, msg)
		}
	}

	return nil
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
				log.Warn("failed to get metric from QueryNode", zap.Int64("nodeID", node.ID()))
				return err
			}

			if resp.Response == "" {
				return nil
			}

			infos := make([]T, 0)
			err = json.Unmarshal([]byte(resp.Response), &infos)
			if err != nil {
				log.Warn("invalid metrics of query node was found", zap.Error(err))
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
			log.Warn("marshal segment value failed", zap.Int64("collectionID", collectionID), zap.String("err", err.Error()))
			return "", nil
		}
		return string(bs), nil
	}
	return "", fmt.Errorf("invalid param value in=[%s], it should be qc or qn", in)
}

// TODO(dragondriver): add more detail metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (string, error) {
	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
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

	coordTopology := metricsinfo.QueryCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, paramtable.GetNodeID()),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		return "", err
	}

	return resp, nil
}

func (s *Server) fillMetricsWithNodes(topo *metricsinfo.QueryClusterTopology, nodeMetrics []*metricResp) {
	for _, metric := range nodeMetrics {
		if metric.err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(metric.err))
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
			log.Warn("invalid metrics of query node was found",
				zap.Any("error_code", metric.resp.GetStatus().GetErrorCode()),
				zap.Any("error_reason", metric.resp.GetStatus().GetReason()))
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
			log.Warn("invalid metrics of query node was found",
				zap.Error(err))
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
				log.Warn("failed to get metric from QueryNode",
					zap.Int64("nodeID", node.ID()))
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

func (s *Server) fillReplicaInfo(ctx context.Context, replica *meta.Replica, withShardNodes bool) *milvuspb.ReplicaInfo {
	info := &milvuspb.ReplicaInfo{
		ReplicaID:         replica.GetID(),
		CollectionID:      replica.GetCollectionID(),
		NodeIds:           replica.GetNodes(),
		ResourceGroupName: replica.GetResourceGroup(),
		NumOutboundNode:   s.meta.GetOutgoingNodeNumByReplica(ctx, replica),
	}

	channels := s.targetMgr.GetDmChannelsByCollection(ctx, replica.GetCollectionID(), meta.CurrentTarget)
	if len(channels) == 0 {
		log.Warn("failed to get channels, collection may be not loaded or in recovering", zap.Int64("collectionID", replica.GetCollectionID()))
		return info
	}
	shardReplicas := make([]*milvuspb.ShardReplica, 0, len(channels))

	var segments []*meta.Segment
	if withShardNodes {
		segments = s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()))
	}

	for _, channel := range channels {
		leader, ok := s.dist.ChannelDistManager.GetShardLeader(replica, channel.GetChannelName())
		var leaderInfo *session.NodeInfo
		if ok {
			leaderInfo = s.nodeMgr.Get(leader)
		}
		if leaderInfo == nil {
			log.Warn("failed to get shard leader for shard",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replica", replica.GetID()),
				zap.String("shard", channel.GetChannelName()))
			return info
		}

		shard := &milvuspb.ShardReplica{
			LeaderID:      leader,
			LeaderAddr:    leaderInfo.Addr(),
			DmChannelName: channel.GetChannelName(),
			NodeIds:       []int64{leader},
		}
		if withShardNodes {
			shardNodes := lo.FilterMap(segments, func(segment *meta.Segment, _ int) (int64, bool) {
				if replica.Contains(segment.Node) {
					return segment.Node, true
				}
				return 0, false
			})
			shard.NodeIds = typeutil.NewUniqueSet(shardNodes...).Collect()
		}
		shardReplicas = append(shardReplicas, shard)
	}
	info.ShardReplicas = shardReplicas
	return info
}
