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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

// return all available node list, for each node, return it's (nodeID, ip_address)
func (s *Server) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	return nil, nil
}

// return query node's workload state, for given nodeID, return it's (channel_name_list, sealed_segment_list)
func (s *Server) GetQueryNodeState(ctx context.Context, req *querypb.GetQueryNodeStateRequest) (*querypb.GetQueryNodeStatesResponse, error) {
	return nil, nil
}

// suspend background balance for all query node, include stopping balance and auto balance
func (s *Server) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	return nil, nil
}

// resume background balance for all query node, include stopping balance and auto balance
func (s *Server) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	return nil, nil
}

// suspend node from resource operation, for given node, suspend load_segment/release_segment/sub_channel/unsub_channel operations
func (s *Server) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	return nil, nil
}

// resume node from resource operation, for given node, resume load_segment/release_segment/sub_channel/unsub_channel operations
func (s *Server) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	return nil, nil
}

// transfer segment from source to target,
// if no segment_id specified, default to transfer all segment on the source node.
// if no target_nodeId specified, default to move segment to all other nodes
func (s *Server) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	return nil, nil
}

// transfer channel from source to target,
// if no channel_name specified, default to transfer all channel on the source node.
// if no target_nodeId specified, default to move channel to all other nodes
func (s *Server) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	return nil, nil
}

// check whether two nodes has same distribution, includes (channel_name_list, sealed_segment_list)
func (s *Server) HasSameDistribution(ctx context.Context, req *querypb.ShowCollectionsRequest) (*commonpb.Status, error) {
	return nil, nil
}
