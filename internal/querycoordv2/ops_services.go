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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	log := log.Ctx(ctx)
	log.Info("list checkers request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to list checkers", zap.Error(err))
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
	log := log.Ctx(ctx)
	log.Info("activate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to activate checker", zap.Error(err))
		return merr.Status(err), nil
	}
	if err := s.checkerController.Activate(utils.CheckerType(req.CheckerID)); err != nil {
		log.Warn("failed to activate checker", zap.Error(err))
		return merr.Status(merr.WrapErrServiceInternal(err.Error())), nil
	}
	return merr.Success(), nil
}

func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("deactivate checker request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to deactivate checker", zap.Error(err))
		return merr.Status(err), nil
	}
	if err := s.checkerController.Deactivate(utils.CheckerType(req.CheckerID)); err != nil {
		log.Warn("failed to deactivate checker", zap.Error(err))
		return merr.Status(merr.WrapErrServiceInternal(err.Error())), nil
	}
	return merr.Success(), nil
}

// return all available node list, for each node, return it's (nodeID, ip_address)
func (s *Server) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	log := log.Ctx(ctx)
	log.Info("ListQueryNode request received")

	errMsg := "failed to list querynode state"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return &querypb.ListQueryNodeResponse{
			Status: merr.Status(errors.Wrap(err, errMsg)),
		}, nil
	}

	nodes := lo.Map(s.nodeMgr.GetAll(), func(nodeInfo *session.NodeInfo, _ int) *querypb.NodeInfo {
		return &querypb.NodeInfo{
			ID:      nodeInfo.ID(),
			Address: nodeInfo.Addr(),
			State:   nodeInfo.GetState().String(),
		}
	})

	return &querypb.ListQueryNodeResponse{
		Status:    merr.Success(),
		NodeInfos: nodes,
	}, nil
}

// return query node's data distribution, for given nodeID, return it's (channel_name_list, sealed_segment_list)
func (s *Server) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	log := log.Ctx(ctx).With(zap.Int64("nodeID", req.GetNodeID()))
	log.Info("GetQueryNodeDistribution request received")

	errMsg := "failed to get query node distribution"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return &querypb.GetQueryNodeDistributionResponse{
			Status: merr.Status(errors.Wrap(err, errMsg)),
		}, nil
	}

	if s.nodeMgr.Get(req.GetNodeID()) == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(errMsg, zap.Error(err))
		return &querypb.GetQueryNodeDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}

	segments := s.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(req.GetNodeID()))
	channels := s.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(req.GetNodeID()))
	return &querypb.GetQueryNodeDistributionResponse{
		Status:           merr.Success(),
		ChannelNames:     lo.Map(channels, func(c *meta.DmChannel, _ int) string { return c.GetChannelName() }),
		SealedSegmentIDs: lo.Map(segments, func(s *meta.Segment, _ int) int64 { return s.GetID() }),
	}, nil
}

// suspend background balance for all query node, include stopping balance and auto balance
func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("SuspendBalance request received")

	errMsg := "failed to suspend balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := s.checkerController.Deactivate(utils.BalanceChecker)
	if err != nil {
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// resume background balance for all query node, include stopping balance and auto balance
func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)

	log.Info("ResumeBalance request received")

	errMsg := "failed to resume balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := s.checkerController.Activate(utils.BalanceChecker)
	if err != nil {
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// CheckBalanceStatus checks whether balance is active or suspended
func (s *Server) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	log := log.Ctx(ctx)
	log.Info("CheckBalanceStatus request received")

	errMsg := "failed to check balance status"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return &querypb.CheckBalanceStatusResponse{
			Status: merr.Status(err),
		}, nil
	}

	isActive, err := s.checkerController.IsActive(utils.BalanceChecker)
	if err != nil {
		log.Warn(errMsg, zap.Error(err))
		return &querypb.CheckBalanceStatusResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &querypb.CheckBalanceStatusResponse{
		Status:   merr.Success(),
		IsActive: isActive,
	}, nil
}

// suspend node from resource operation, for given node, suspend load_segment/sub_channel operations
func (s *Server) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)

	log.Info("SuspendNode request received", zap.Int64("nodeID", req.GetNodeID()))

	errMsg := "failed to suspend query node"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	if s.nodeMgr.Get(req.GetNodeID()) == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	s.meta.ResourceManager.HandleNodeDown(ctx, req.GetNodeID())
	return merr.Success(), nil
}

// resume node from resource operation, for given node, resume load_segment/sub_channel operations
func (s *Server) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("ResumeNode request received", zap.Int64("nodeID", req.GetNodeID()))

	errMsg := "failed to resume query node"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(errors.Wrap(err, errMsg)), nil
	}

	info := s.nodeMgr.Get(req.GetNodeID())
	if info == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	if info.IsEmbeddedQueryNodeInStreamingNode() {
		return merr.Status(
			merr.WrapErrParameterInvalidMsg("embedded query node in streaming node can't be resumed")), nil
	}

	s.meta.ResourceManager.HandleNodeUp(ctx, req.GetNodeID())

	return merr.Success(), nil
}

// transfer segment from source to target,
// if no segment_id specified, default to transfer all segment on the source node.
// if no target_nodeId specified, default to move segment to all other nodes
func (s *Server) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)

	log.Info("TransferSegment request received",
		zap.Int64("source", req.GetSourceNodeID()),
		zap.Int64("dest", req.GetTargetNodeID()),
		zap.Int64("segment", req.GetSegmentID()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	// check whether srcNode is healthy
	srcNode := req.GetSourceNodeID()
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		err := merr.WrapErrNodeNotAvailable(srcNode, "the source node is invalid")
		return merr.Status(err), nil
	}

	replicas := s.meta.ReplicaManager.GetByNode(ctx, req.GetSourceNodeID())
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
				log.Info("segment doesn't exist in current target, skip it", zap.Int64("segmentID", req.GetSegmentID()))
			} else {
				toBalance.Insert(segment)
			}
		}

		err := s.balanceSegments(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), false, req.GetCopyMode())
		if err != nil {
			msg := "failed to balance segments"
			log.Warn(msg, zap.Error(err))
			return merr.Status(errors.Wrap(err, msg)), nil
		}
	}
	return merr.Success(), nil
}

// transfer channel from source to target,
// if no channel_name specified, default to transfer all channel on the source node.
// if no target_nodeId specified, default to move channel to all other nodes
func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)

	log.Info("TransferChannel request received",
		zap.Int64("source", req.GetSourceNodeID()),
		zap.Int64("dest", req.GetTargetNodeID()),
		zap.String("channel", req.GetChannelName()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	// check whether srcNode is healthy
	srcNode := req.GetSourceNodeID()
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		err := merr.WrapErrNodeNotAvailable(srcNode, "the source node is invalid")
		return merr.Status(err), nil
	}

	replicas := s.meta.ReplicaManager.GetByNode(ctx, req.GetSourceNodeID())
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
		channels := s.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(srcNode))
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
				log.Info("channel doesn't exist in current target, skip it", zap.String("channelName", channel.GetChannelName()))
			} else {
				toBalance.Insert(channel)
			}
		}

		err := s.balanceChannels(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), false, req.GetCopyMode())
		if err != nil {
			msg := "failed to balance channels"
			log.Warn(msg, zap.Error(err))
			return merr.Status(errors.Wrap(err, msg)), nil
		}
	}
	return merr.Success(), nil
}

func (s *Server) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)

	log.Info("CheckQueryNodeDistribution request received",
		zap.Int64("source", req.GetSourceNodeID()),
		zap.Int64("dest", req.GetTargetNodeID()))

	errMsg := "failed to check query node distribution"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	sourceNode := s.nodeMgr.Get(req.GetSourceNodeID())
	if sourceNode == nil {
		err := merr.WrapErrNodeNotFound(req.GetSourceNodeID(), "source node not found")
		log.Warn(errMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	targetNode := s.nodeMgr.Get(req.GetTargetNodeID())
	if targetNode == nil {
		err := merr.WrapErrNodeNotFound(req.GetTargetNodeID(), "target node not found")
		log.Warn(errMsg, zap.Error(err))
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

	// check whether all segment exist in source node has been loaded in target node
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
