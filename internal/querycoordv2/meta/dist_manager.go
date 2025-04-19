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

package meta

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

type DistributionManager struct {
	SegmentDistManager SegmentDistManagerInterface
	ChannelDistManager ChannelDistManagerInterface
}

func NewDistributionManager() *DistributionManager {
	return &DistributionManager{
		SegmentDistManager: NewSegmentDistManager(),
		ChannelDistManager: NewChannelDistManager(),
	}
}

// GetDistributionJSON returns a JSON representation of the current distribution state.
// It includes segments, DM channels, and leader views.
// If there are no segments, channels, or leader views, it returns an empty string.
// In case of an error during JSON marshaling, it returns the error.
func (dm *DistributionManager) GetDistributionJSON(collectionID int64) string {
	segments := dm.SegmentDistManager.GetSegmentDist(collectionID)
	channels := dm.ChannelDistManager.GetChannelDist(collectionID)
	leaderView := dm.ChannelDistManager.GetLeaderView(collectionID)

	dist := &metricsinfo.QueryCoordDist{
		Segments:    segments,
		DMChannels:  channels,
		LeaderViews: leaderView,
	}

	v, err := json.Marshal(dist)
	if err != nil {
		log.Warn("failed to marshal dist", zap.Error(err))
		return ""
	}
	return string(v)
}
