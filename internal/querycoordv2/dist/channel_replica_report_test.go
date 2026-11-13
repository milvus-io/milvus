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

package dist

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The replica a QueryNode reports for its delegator reaches the channel
// distribution through both full and delta reports.
func TestChannelDistributionCarriesReportedReplica(t *testing.T) {
	ctx := context.Background()
	nodeID := time.Now().UnixNano()%1000000 + 1000000
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID, Address: "localhost:19530", Hostname: "localhost"}))
	target := meta.NewMockTargetManager(t)
	target.EXPECT().GetDmChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	target.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(int64(101)).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	handler := &distHandler{nodeID: nodeID, nodeManager: nodeManager, dist: dist, target: target}
	defer metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(nodeID))

	const channel = "test-channel-replica"
	report := func(ts int64, delta bool, replicaID int64) *querypb.GetDataDistributionResponse {
		return &querypb.GetDataDistributionResponse{
			Status: merr.Success(), NodeID: nodeID, LastModifyTs: ts, IsDelta: delta,
			TotalChannelCount: 1,
			Channels:          []*querypb.ChannelVersionInfo{{Channel: channel, Collection: 10, Version: ts, ReplicaID: replicaID}},
			LeaderViews: []*querypb.LeaderView{{
				Collection: 10, Channel: channel, TargetVersion: 101,
				Status: &querypb.LeaderViewStatus{Serviceable: true},
			}},
		}
	}
	reported := func() int64 {
		channels := dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
		require.Len(t, channels, 1)
		return channels[0].ReplicaID
	}

	handler.handleDistResp(ctx, report(1, false, 7))
	require.Equal(t, int64(7), reported())
	// The node's replica replaced the delegator.
	handler.handleDistResp(ctx, report(2, true, 8))
	require.Equal(t, int64(8), reported())
}
