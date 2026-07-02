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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	log := mlog.With()
	log.Info(ctx, "list checkers request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, "failed to list checkers", mlog.Err(err))
		return &querypb.ListCheckersResponse{
			Status: merr.Status(err),
		}, nil
	}
	checkers := s.checkerController.Checkers()
	checkerIDSet := typeutil.NewSet(req.CheckerIDs...)

	resp := &querypb.ListCheckersResponse{
		Status: merr.Success(),
	}
	for _, checker := range checkers {
		if checkerIDSet.Len() == 0 || checkerIDSet.Contain(int32(checker.ID())) {
			resp.CheckerInfos = append(resp.CheckerInfos, &querypb.CheckerInfo{
				Id:        int32(checker.ID()),
				Activated: checker.IsActive(),
				Desc:      checker.ID().String(),
				Found:     true,
			})
			checkerIDSet.Remove(int32(checker.ID()))
		}
	}

	for _, id := range checkerIDSet.Collect() {
		resp.CheckerInfos = append(resp.CheckerInfos, &querypb.CheckerInfo{
			Id:    id,
			Found: false,
		})
	}

	return resp, nil
}

func (s *Server) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	log := mlog.With()
	log.Info(ctx, "activate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, "failed to activate checker", mlog.Err(err))
		return merr.Status(err), nil
	}
	if err := s.checkerController.Activate(utils.CheckerType(req.CheckerID)); err != nil {
		log.Warn(ctx, "failed to activate checker", mlog.Err(err))
		return merr.Status(merr.WrapErrParameterInvalidMsg("invalid checker type %d: %v", req.CheckerID, err)), nil
	}
	return merr.Success(), nil
}

func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	log := mlog.With()
	log.Info(ctx, "deactivate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, "failed to deactivate checker", mlog.Err(err))
		return merr.Status(err), nil
	}
	if err := s.checkerController.Deactivate(utils.CheckerType(req.CheckerID)); err != nil {
		log.Warn(ctx, "failed to deactivate checker", mlog.Err(err))
		return merr.Status(merr.WrapErrParameterInvalidMsg("invalid checker type %d: %v", req.CheckerID, err)), nil
	}
	return merr.Success(), nil
}

// ListQueryNode return all available node list, for each node, return it's (nodeID, ip_address)
func (s *Server) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	log := mlog.With()
	log.Info(ctx, "ListQueryNode request received")

	errMsg := "failed to list querynode state"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.ListQueryNodeResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", errMsg)),
		}, nil
	}

	nodes := lo.Map(s.nodeMgr.GetAll(), func(nodeInfo *session.NodeInfo, _ int) *querypb.NodeInfo {
		return &querypb.NodeInfo{
			ID:      nodeInfo.ID(),
			Address: nodeInfo.Addr(),
			State:   nodeInfo.GetState().String(),
		}
	})

	// 2. Second Pass: Filter and Map the converted nodes to get their IDs
	nodeIDs := lo.FilterMap(nodes, func(nodeInfo *querypb.NodeInfo, _ int) (int64, bool) {
		if nodeInfo.State != session.StoppingStateName {
			return nodeInfo.ID, true
		}
		return 0, false // Discard this node
	})

	nodesSuspended := s.meta.GetNodesSuspended(nodeIDs)

	// Loop through each node in the `nodes` slice.
	for _, node := range nodes {
		// Check the `nodesSuspended` map for the current node's ID.
		// The `ok` variable ensures the key actually exists in the map.
		if isSuspended, ok := nodesSuspended[node.ID]; ok && isSuspended {
			// If the node ID is in the map and the status is true, update the State.
			node.State = session.SuspendStateName
		}
	}

	return &querypb.ListQueryNodeResponse{
		Status:    merr.Success(),
		NodeInfos: nodes,
	}, nil
}

// GetQueryNodeDistribution return query node's data distribution, for given nodeID, return it's (channel_name_list, sealed_segment_list)
func (s *Server) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	log := mlog.With(mlog.Int64("nodeID", req.GetNodeID()))
	log.Info(ctx, "GetQueryNodeDistribution request received")

	errMsg := "failed to get query node distribution"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.GetQueryNodeDistributionResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", errMsg)),
		}, nil
	}

	node := s.nodeMgr.Get(req.GetNodeID())
	if node == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.GetQueryNodeDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}

	segments := s.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(req.GetNodeID()))
	channels := s.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(req.GetNodeID()))
	return &querypb.GetQueryNodeDistributionResponse{
		Status:                   merr.Success(),
		ChannelNames:             lo.Map(channels, func(c *meta.DmChannel, _ int) string { return c.GetChannelName() }),
		SealedSegmentIDs:         lo.Map(segments, func(s *meta.Segment, _ int) int64 { return s.GetID() }),
		CacheShardDiskUsageStats: node.CacheShardDiskUsageStats(),
	}, nil
}

func (s *Server) SuspendChannelBalance(ctx context.Context) error {
	log := mlog.With()
	log.Info(ctx, "SuspendChannelBalance request received")

	errMsg := "failed to suspend channel balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return err
	}
	if err := paramtable.Get().Save(Params.QueryCoordCfg.AutoBalanceChannel.Key, "false"); err != nil {
		log.Warn(ctx, err.Error(), mlog.Err(err))
		return err
	}
	log.Info(ctx, "SuspendChannelBalance request finished successfully")
	return nil
}

func (s *Server) ResumeChannelBalance(ctx context.Context) error {
	log := mlog.With()
	log.Info(ctx, "ResumeChannelBalance request received")

	errMsg := "failed to resume channel balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return err
	}

	if err := paramtable.Get().Save(Params.QueryCoordCfg.AutoBalanceChannel.Key, "true"); err != nil {
		log.Warn(ctx, err.Error(), mlog.Err(err))
		return err
	}
	log.Info(ctx, "ResumeChannelBalance request finished successfully")

	return nil
}

// SuspendBalance background balance for all query node, include stopping balance and auto balance
func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	log := mlog.With()
	log.Info(ctx, "SuspendBalance request received")

	errMsg := "failed to suspend balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := s.checkerController.Deactivate(utils.BalanceChecker)
	if err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// ResumeBalance background balance for all query node, include stopping balance and auto balance
func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "ResumeBalance request received")

	errMsg := "failed to resume balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := s.checkerController.Activate(utils.BalanceChecker)
	if err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// CheckBalanceStatus checks whether balance is active or suspended
func (s *Server) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	log := mlog.With()
	log.Info(ctx, "CheckBalanceStatus request received")

	errMsg := "failed to check balance status"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.CheckBalanceStatusResponse{
			Status: merr.Status(err),
		}, nil
	}

	isActive, err := s.checkerController.IsActive(utils.BalanceChecker)
	if err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.CheckBalanceStatusResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &querypb.CheckBalanceStatusResponse{
		Status:   merr.Success(),
		IsActive: isActive,
	}, nil
}

func (s *Server) CheckChannelBalanceActive(ctx context.Context) (bool, error) {
	log := mlog.With()
	log.Info(ctx, "ResumeChannelBalance request received")

	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, "failed to resume channel balance", mlog.Err(err))
		return false, err
	}
	active := paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool()

	log.Info(ctx, "ResumeChannelBalance request finished successfully", mlog.Bool("active", active))

	return active, nil
}

// IsNodeSuspended checks if a specific node is suspended based on the provided request.
// It returns true if the node is suspended, false otherwise.
func (s *Server) IsNodeSuspended(ctx context.Context, nodeID int64) (bool, error) {
	log := mlog.With()

	log.Info(ctx, "IsNodeSuspended request received", mlog.Int64("nodeID", nodeID))

	errMsg := "failed to call IsNodeSuspended for query node"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return false, err
	}

	if s.nodeMgr.Get(nodeID) == nil {
		err := merr.WrapErrNodeNotFound(nodeID, errMsg)
		log.Warn(ctx, errMsg, mlog.Err(err))
		return false, err
	}
	isSuspended := s.meta.IsNodeSuspended(nodeID)
	return isSuspended, nil
}

// suspend node from resource operation, for given node, suspend load_segment/sub_channel operations
func (s *Server) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "SuspendNode request received", mlog.Int64("nodeID", req.GetNodeID()))

	errMsg := "failed to suspend query node"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	if s.nodeMgr.Get(req.GetNodeID()) == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	s.meta.HandleNodeDown(ctx, req.GetNodeID())
	return merr.Success(), nil
}

// resume node from resource operation, for given node, resume load_segment/sub_channel operations
func (s *Server) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	log := mlog.With()
	log.Info(ctx, "ResumeNode request received", mlog.Int64("nodeID", req.GetNodeID()))

	errMsg := "failed to resume query node"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", errMsg)), nil
	}

	info := s.nodeMgr.Get(req.GetNodeID())
	if info == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	if info.IsEmbeddedQueryNodeInStreamingNode() {
		return merr.Status(
			merr.WrapErrParameterInvalidMsg("embedded query node in streaming node can't be resumed")), nil
	}

	s.meta.HandleNodeUp(ctx, req.GetNodeID())

	return merr.Success(), nil
}

// transfer segment from source to target,
// if no segment_id specified, default to transfer all segment on the source node.
// if no target_nodeId specified, default to move segment to all other nodes
func (s *Server) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "TransferSegment request received",
		mlog.Int64("source", req.GetSourceNodeID()),
		mlog.Int64("dest", req.GetTargetNodeID()),
		mlog.Int64("segment", req.GetSegmentID()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		log.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	// check whether srcNode is healthy
	srcNode := req.GetSourceNodeID()
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		err := merr.WrapErrNodeNotAvailable(srcNode, "the source node is invalid")
		return merr.Status(err), nil
	}

	replicas := s.meta.GetByNode(ctx, req.GetSourceNodeID())
	for _, replica := range replicas {
		// when no dst node specified, default to use all other nodes in same
		dstNodeSet := typeutil.NewUniqueSet()
		if req.GetToAllNodes() {
			dstNodeSet.Insert(replica.GetRWNodes()...)
		} else {
			// check whether dstNode is healthy
			if err := s.isStoppingNode(ctx, req.GetTargetNodeID()); err != nil {
				err := merr.WrapErrNodeNotAvailable(srcNode, "the target node is invalid")
				return merr.Status(err), nil
			}
			if streamingutil.IsStreamingServiceEnabled() {
				sqn := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs()
				if sqn.Contain(req.GetTargetNodeID()) {
					return merr.Status(
						merr.WrapErrParameterInvalidMsg("embedded query node in streaming node can't be the destination of transfer segment")), nil
				}
			}
			dstNodeSet.Insert(req.GetTargetNodeID())
		}
		dstNodeSet.Remove(srcNode)

		// check sealed segment list
		segments := s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(srcNode))

		toBalance := typeutil.NewSet[*meta.Segment]()
		if req.GetTransferAll() {
			toBalance.Insert(segments...)
		} else {
			// check whether sealed segment exist
			segment, ok := lo.Find(segments, func(s *meta.Segment) bool { return s.GetID() == req.GetSegmentID() })
			if !ok {
				err := merr.WrapErrSegmentNotFound(req.GetSegmentID(), "segment not found in source node")
				return merr.Status(err), nil
			}

			existInTarget := s.targetMgr.GetSealedSegment(ctx, segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
			if !existInTarget {
				log.Info(ctx, "segment doesn't exist in current target, skip it", mlog.Int64("segmentID", req.GetSegmentID()))
			} else {
				toBalance.Insert(segment)
			}
		}

		err := s.balanceSegments(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), false, req.GetCopyMode())
		if err != nil {
			msg := "failed to balance segments"
			log.Warn(ctx, msg, mlog.Err(err))
			return merr.Status(merr.Wrapf(err, "%s", msg)), nil
		}
	}
	return merr.Success(), nil
}

// transfer channel from source to target,
// if no channel_name specified, default to transfer all channel on the source node.
// if no target_nodeId specified, default to move channel to all other nodes
func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "TransferChannel request received",
		mlog.Int64("source", req.GetSourceNodeID()),
		mlog.Int64("dest", req.GetTargetNodeID()),
		mlog.String("channel", req.GetChannelName()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		log.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	// check whether srcNode is healthy
	srcNode := req.GetSourceNodeID()
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		err := merr.WrapErrNodeNotAvailable(srcNode, "the source node is invalid")
		return merr.Status(err), nil
	}

	replicas := s.meta.GetByNode(ctx, req.GetSourceNodeID())
	for _, replica := range replicas {
		// when no dst node specified, default to use all other nodes in same
		dstNodeSet := typeutil.NewUniqueSet()
		if req.GetToAllNodes() {
			if streamingutil.IsStreamingServiceEnabled() {
				dstNodeSet.Insert(replica.GetRWSQNodes()...)
			} else {
				dstNodeSet.Insert(replica.GetRWNodes()...)
			}
		} else {
			// check whether dstNode is healthy
			if err := s.isStoppingNode(ctx, req.GetTargetNodeID()); err != nil {
				err := merr.WrapErrNodeNotAvailable(srcNode, "the target node is invalid")
				return merr.Status(err), nil
			}
			dstNodeSet.Insert(req.GetTargetNodeID())
		}
		dstNodeSet.Remove(srcNode)

		// check sealed segment list
		channels := s.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(srcNode))
		toBalance := typeutil.NewSet[*meta.DmChannel]()
		if req.GetTransferAll() {
			toBalance.Insert(channels...)
		} else {
			// check whether sealed segment exist
			channel, ok := lo.Find(channels, func(ch *meta.DmChannel) bool { return ch.GetChannelName() == req.GetChannelName() })
			if !ok {
				err := merr.WrapErrChannelNotFound(req.GetChannelName(), "channel not found in source node")
				return merr.Status(err), nil
			}
			existInTarget := s.targetMgr.GetDmChannel(ctx, channel.GetCollectionID(), channel.GetChannelName(), meta.CurrentTarget) != nil
			if !existInTarget {
				log.Info(ctx, "channel doesn't exist in current target, skip it", mlog.String("channelName", channel.GetChannelName()))
			} else {
				toBalance.Insert(channel)
			}
		}

		err := s.balanceChannels(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), false, req.GetCopyMode())
		if err != nil {
			msg := "failed to balance channels"
			log.Warn(ctx, msg, mlog.Err(err))
			return merr.Status(merr.Wrapf(err, "%s", msg)), nil
		}
	}
	return merr.Success(), nil
}

func (s *Server) ClearReadTaskQueue(ctx context.Context, req *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	mlog.Info(ctx, "ClearReadTaskQueue request received",
		mlog.String("taskType", req.GetTaskType()),
		mlog.String("reason", req.GetReason()))

	resp := &internalpb.ClearReadTaskQueueResponse{Status: merr.Success()}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	nodes := s.nodeMgr.GetAll()
	group := &errgroup.Group{}
	results := make(chan *internalpb.ClearReadTaskQueueComponentResult, len(nodes))
	for _, node := range nodes {
		if node.IsStoppingState() {
			continue
		}
		nodeID := node.ID()
		group.Go(func() error {
			nodeResp, err := s.cluster.ClearReadTaskQueue(ctx, nodeID, req)
			if errors.Is(err, merr.ErrServiceUnimplemented) {
				return nil
			}
			if err != nil {
				results <- &internalpb.ClearReadTaskQueueComponentResult{
					Status: merr.Status(err),
					Role:   typeutil.QueryNodeRole,
					NodeID: nodeID,
				}
				return merr.Wrapf(err, "ClearReadTaskQueue failed, queryNodeID = %d", nodeID)
			}

			if len(nodeResp.GetResults()) > 0 {
				for _, result := range nodeResp.GetResults() {
					results <- result
				}
			} else {
				results <- &internalpb.ClearReadTaskQueueComponentResult{
					Status:          nodeResp.GetStatus(),
					Role:            typeutil.QueryNodeRole,
					NodeID:          nodeID,
					QueuedCleared:   nodeResp.GetQuerynodeQueuedCleared(),
					QueuedNqCleared: nodeResp.GetQueuedNqCleared(),
				}
			}
			if !merr.Ok(nodeResp.GetStatus()) {
				return merr.Wrapf(merr.Error(nodeResp.GetStatus()), "ClearReadTaskQueue failed, queryNodeID = %d", nodeID)
			}
			return nil
		})
	}

	err := group.Wait()
	close(results)
	for result := range results {
		resp.Results = append(resp.Results, result)
		if result.GetRole() == typeutil.QueryNodeRole && merr.Ok(result.GetStatus()) {
			resp.QuerynodeQueuedCleared += result.GetQueuedCleared()
			resp.QueuedNqCleared += result.GetQueuedNqCleared()
		}
	}
	if err != nil {
		resp.Status = merr.Status(err)
	}

	mlog.Info(ctx, "cleared querynode read task queues",
		mlog.String("taskType", req.GetTaskType()),
		mlog.String("reason", req.GetReason()),
		mlog.Int("queryNodes", len(resp.GetResults())),
		mlog.Int64("queuedCleared", resp.GetQuerynodeQueuedCleared()),
		mlog.Int64("queuedNQCleared", resp.GetQueuedNqCleared()),
		mlog.Err(err))
	return resp, nil
}

func (s *Server) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "CheckQueryNodeDistribution request received",
		mlog.Int64("source", req.GetSourceNodeID()),
		mlog.Int64("dest", req.GetTargetNodeID()))

	errMsg := "failed to check query node distribution"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	sourceNode := s.nodeMgr.Get(req.GetSourceNodeID())
	if sourceNode == nil {
		err := merr.WrapErrNodeNotFound(req.GetSourceNodeID(), "source node not found")
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	targetNode := s.nodeMgr.Get(req.GetTargetNodeID())
	if targetNode == nil {
		err := merr.WrapErrNodeNotFound(req.GetTargetNodeID(), "target node not found")
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	// check channel list
	channelOnSrc := s.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(req.GetSourceNodeID()))
	channelOnDst := s.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(req.GetTargetNodeID()))
	channelDstMap := lo.SliceToMap(channelOnDst, func(ch *meta.DmChannel) (string, *meta.DmChannel) {
		return ch.GetChannelName(), ch
	})
	for _, ch := range channelOnSrc {
		if s.targetMgr.GetDmChannel(ctx, ch.GetCollectionID(), ch.GetChannelName(), meta.CurrentTargetFirst) == nil {
			continue
		}

		if _, ok := channelDstMap[ch.GetChannelName()]; !ok {
			return merr.Status(merr.WrapErrChannelLack(ch.GetChannelName())), nil
		}
	}

	// check whether all segments exist in source node has been loaded in target node
	segmentOnSrc := s.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(req.GetSourceNodeID()))
	segmentOnDst := s.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(req.GetTargetNodeID()))
	segmentDstMap := lo.SliceToMap(segmentOnDst, func(s *meta.Segment) (int64, *meta.Segment) {
		return s.GetID(), s
	})
	for _, segment := range segmentOnSrc {
		if s.targetMgr.GetSealedSegment(ctx, segment.GetCollectionID(), segment.GetID(), meta.CurrentTargetFirst) == nil {
			continue
		}

		if _, ok := segmentDstMap[segment.GetID()]; !ok {
			return merr.Status(merr.WrapErrSegmentLack(segment.GetID())), nil
		}
	}
	return merr.Success(), nil
}
