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

package querycoord

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

// generateFullWatchDmChannelsRequest fill the WatchDmChannelsRequest by get segment infos from Meta
func generateFullWatchDmChannelsRequest(broker *globalMetaBroker, request *querypb.WatchDmChannelsRequest) (*querypb.WatchDmChannelsRequest, error) {
	cloned := proto.Clone(request).(*querypb.WatchDmChannelsRequest)
	vChannels := cloned.GetInfos()

	// fill segmentInfos
	segmentIds := make([]int64, 0)
	for _, vChannel := range vChannels {
		segmentIds = append(segmentIds, vChannel.FlushedSegmentIds...)
		segmentIds = append(segmentIds, vChannel.UnflushedSegmentIds...)
		segmentIds = append(segmentIds, vChannel.DroppedSegmentIds...)
	}
	segmentInfos, err := broker.getDataSegmentInfosByIDs(segmentIds)
	if err != nil {
		log.Error("Get Vchannel SegmentInfos failed", zap.Error(err))
		return nil, err
	}
	segmentDict := make(map[int64]*datapb.SegmentInfo)
	for _, info := range segmentInfos {
		segmentDict[info.ID] = info
	}
	cloned.SegmentInfos = segmentDict

	return cloned, err
}

// thinWatchDmChannelsRequest will return a thin version of WatchDmChannelsRequest
// the thin version is used for storage because the complete version may be too large
func thinWatchDmChannelsRequest(request *querypb.WatchDmChannelsRequest) *querypb.WatchDmChannelsRequest {
	cloned := proto.Clone(request).(*querypb.WatchDmChannelsRequest)
	cloned.SegmentInfos = make(map[int64]*datapb.SegmentInfo)
	return cloned
}
