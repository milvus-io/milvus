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

package utils

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"go.uber.org/zap"
)

// In a replica, a shard is available, if and only if:
// 1. The leader is online
// 2. All QueryNodes in the distribution are online
// 3. The last heartbeat response time is within HeartbeatAvailableInterval for all QueryNodes(include leader) in the distribution
// 4. All segments of the shard in target should be in the distribution
func CheckLeaderAvailable(nodeMgr *session.NodeManager, leader *meta.LeaderView, currentTargets map[int64]*datapb.SegmentInfo) error {
	log := log.Ctx(context.TODO()).
		WithRateGroup("utils.CheckLeaderAvailable", 1, 60).
		With(zap.Int64("leaderID", leader.ID))
	info := nodeMgr.Get(leader.ID)

	// Check whether leader is online
	err := CheckNodeAvailable(leader.ID, info)
	if err != nil {
		log.Info("leader is not available", zap.Error(err))
		return fmt.Errorf("leader not available: %w", err)
	}

	for id, version := range leader.Segments {
		info := nodeMgr.Get(version.GetNodeID())
		err = CheckNodeAvailable(version.GetNodeID(), info)
		if err != nil {
			log.Info("leader is not available due to QueryNode unavailable",
				zap.Int64("segmentID", id),
				zap.Error(err))
			return err
		}
	}

	// Check whether segments are fully loaded
	for segmentID, info := range currentTargets {
		if info.GetInsertChannel() != leader.Channel {
			continue
		}

		_, exist := leader.Segments[segmentID]
		if !exist {
			log.RatedInfo(10, "leader is not available due to lack of segment", zap.Int64("segmentID", segmentID))
			return merr.WrapErrSegmentLack(segmentID)
		}
	}
	return nil
}

func CheckNodeAvailable(nodeID int64, info *session.NodeInfo) error {
	if info == nil {
		return merr.WrapErrNodeOffline(nodeID)
	}
	return nil
}
