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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	walbalancer "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	walbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Deprecated: QueryView no longer uses independently controlled checkers.
func (s *Server) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	return &querypb.ListCheckersResponse{
		Status: merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "ListCheckers is deprecated; use CheckBalanceStatus for QueryView balance configuration")),
	}, nil
}

// Deprecated: use ResumeBalance to enable optional QueryView balancing.
func (s *Server) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "ActivateChecker is deprecated; use ResumeBalance for QueryView balancing")), nil
}

// Deprecated: use SuspendBalance to disable optional QueryView balancing.
func (s *Server) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "DeactivateChecker is deprecated; use SuspendBalance for QueryView balancing")), nil
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

	if s.nodeMgr.Get(req.GetNodeID()) == nil {
		err := merr.WrapErrNodeNotFound(req.GetNodeID(), errMsg)
		log.Warn(ctx, errMsg, mlog.Err(err))
		return &querypb.GetQueryNodeDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}

	// QueryNodes no longer host channel delegators. Read confirmed segment
	// resources from QueryView reports, not the retired distribution heartbeat.
	return &querypb.GetQueryNodeDistributionResponse{
		Status:           merr.Success(),
		SealedSegmentIDs: s.qviewsRuntime.shardViewRegistry.LoadedSegmentsOnNode(req.GetNodeID()),
	}, nil
}

// SuspendBalance disables optional QueryView placement optimization.
func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	log := mlog.With()
	log.Info(ctx, "SuspendBalance request received")

	errMsg := "failed to suspend balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := alterBalanceConfig(false)
	if err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// ResumeBalance enables optional QueryView placement optimization.
func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	log := mlog.With()

	log.Info(ctx, "ResumeBalance request received")

	errMsg := "failed to resume balance for all querynode"
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}

	err := alterBalanceConfig(true)
	if err != nil {
		log.Warn(ctx, errMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// alterBalanceConfig uses the same persistent update and synchronous local
// refresh as AlterConfig. A process-local Save would mask later etcd updates.
func alterBalanceConfig(enabled bool) error {
	manager := paramtable.GetBaseTable().Manager()
	source, ok := manager.GetEtcdSource()
	if !ok {
		return merr.WrapErrServiceUnavailableMsg("cannot alter balance configuration: etcd source is not enabled")
	}
	return manager.UpdateConfigInEtcd(source, paramtable.Get().QueryViewCfg.BalancerOptimizationEnabled.Key, strconv.FormatBool(enabled))
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

	return &querypb.CheckBalanceStatusResponse{
		Status:   merr.Success(),
		IsActive: paramtable.Get().QueryViewCfg.BalancerOptimizationEnabled.GetAsBool(),
	}, nil
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
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}
	// CopyMode is retained on the wire but ignored: QueryView only supports move.
	var destinations, segments []int64
	if !req.GetToAllNodes() {
		destinations = []int64{req.GetTargetNodeID()}
	}
	if !req.GetTransferAll() {
		segments = []int64{req.GetSegmentID()}
	}
	return merr.Status(s.manualMoveSegments(ctx, 0, req.GetSourceNodeID(), destinations, segments)), nil
}

// TransferChannel moves whole PChannels between StreamingNodes and waits for
// their target WAL assignments to be confirmed. VChannel input selects its
// PChannel, including the other collections on that PChannel.
func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Status(err), nil
	}
	if req.GetCopyMode() {
		return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "WAL transfer does not support copy mode")), nil
	}
	if req.GetSourceNodeID() <= 0 || (!req.GetToAllNodes() && (req.GetTargetNodeID() <= 0 || req.GetTargetNodeID() == req.GetSourceNodeID())) {
		return merr.Status(merr.WrapErrParameterInvalidMsg("expected a positive source SN and a different positive target SN")), nil
	}
	if !req.GetTransferAll() && req.GetChannelName() == "" {
		return merr.Status(merr.WrapErrParameterInvalidMsg("channel_name is required unless transfer_all is set")), nil
	}
	transfer := walbalancer.TransferChannelsRequest{SourceNodeID: req.GetSourceNodeID()}
	if !req.GetToAllNodes() {
		transfer.TargetNodeID = req.GetTargetNodeID()
	}
	if !req.GetTransferAll() {
		transfer.ChannelName = req.GetChannelName()
	}
	b, err := walbalance.GetWithContext(ctx)
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Status(b.TransferChannels(ctx, transfer)), nil
}

// Deprecated: the legacy scheduler cannot clear QueryView execution queues.
func (s *Server) ClearReadTaskQueue(ctx context.Context, req *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	return &internalpb.ClearReadTaskQueueResponse{
		Status: merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "ClearReadTaskQueue is deprecated; no queues were cleared")),
	}, nil
}

// Deprecated: QueryView migration APIs wait for the accepted view to become Up.
// Resource-ID coverage cannot establish migration completion or safe node drain.
func (s *Server) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "CheckQueryNodeDistribution is deprecated; LoadBalance and TransferSegment wait for QueryView handoff, but do not guarantee safe node shutdown")), nil
}
