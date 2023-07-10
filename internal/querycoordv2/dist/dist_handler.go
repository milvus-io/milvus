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
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

const (
	distReqTimeout         = 3 * time.Second
	heartBeatLagBehindWarn = 3 * time.Second
	maxFailureTimes        = 3
)

type distHandler struct {
	nodeID      int64
	c           chan struct{}
	wg          sync.WaitGroup
	client      session.Cluster
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	dist        *meta.DistributionManager
	target      *meta.TargetManager
	mu          sync.Mutex
	stopOnce    sync.Once
}

func (dh *distHandler) start(ctx context.Context) {
	defer dh.wg.Done()
	log := log.Ctx(ctx).With(zap.Int64("nodeID", dh.nodeID)).WithRateGroup("qcv2.distHandler", 1, 60)
	log.Info("start dist handler")
	ticker := time.NewTicker(Params.QueryCoordCfg.DistPullInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("close dist handler due to context done")
			return
		case <-dh.c:
			log.Info("close dist handler")
			return
		case <-ticker.C:
			resp, err := dh.getDistribution(ctx)
			if err != nil {
				node := dh.nodeManager.Get(dh.nodeID)
				fields := []zap.Field{zap.Int("times", failures)}
				if node != nil {
					fields = append(fields, zap.Time("lastHeartbeat", node.LastHeartbeat()))
				}
				fields = append(fields, zap.Error(err))
				log.RatedWarn(30.0, "failed to get data distribution", fields...)
			} else {
				failures = 0
				dh.handleDistResp(resp)
			}
		}
	}
}

func (dh *distHandler) handleDistResp(resp *querypb.GetDataDistributionResponse) {
	node := dh.nodeManager.Get(resp.GetNodeID())
	if node != nil {
		node.UpdateStats(
			session.WithSegmentCnt(len(resp.GetSegments())),
			session.WithChannelCnt(len(resp.GetChannels())),
		)
		if time.Since(node.LastHeartbeat()) > heartBeatLagBehindWarn {
			log.Warn("node last heart beat time lag too behind", zap.Time("now", time.Now()),
				zap.Time("lastHeartBeatTime", node.LastHeartbeat()), zap.Int64("nodeID", node.ID()))
		}
		node.SetLastHeartbeat(time.Now())
	}

	dh.updateSegmentsDistribution(resp)
	dh.updateChannelsDistribution(resp)
	dh.updateLeaderView(resp)

	dh.scheduler.Dispatch(dh.nodeID)
}

func (dh *distHandler) updateSegmentsDistribution(resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.Segment, 0, len(resp.GetSegments()))
	for _, s := range resp.GetSegments() {
		// for collection which is already loaded
		segmentInfo := dh.target.GetHistoricalSegment(s.GetCollection(), s.GetID(), meta.CurrentTarget)
		if segmentInfo == nil {
			// for collection which is loading
			segmentInfo = dh.target.GetHistoricalSegment(s.GetCollection(), s.GetID(), meta.NextTarget)
		}
		var segment *meta.Segment
		if segmentInfo == nil {
			segment = &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            s.GetID(),
					CollectionID:  s.GetCollection(),
					PartitionID:   s.GetPartition(),
					InsertChannel: s.GetChannel(),
				},
				Node:               resp.GetNodeID(),
				Version:            s.GetVersion(),
				LastDeltaTimestamp: s.GetLastDeltaTimestamp(),
				IndexInfo:          s.GetIndexInfo(),
			}
		} else {
			segment = &meta.Segment{
				SegmentInfo:        proto.Clone(segmentInfo).(*datapb.SegmentInfo),
				Node:               resp.GetNodeID(),
				Version:            s.GetVersion(),
				LastDeltaTimestamp: s.GetLastDeltaTimestamp(),
				IndexInfo:          s.GetIndexInfo(),
			}
		}
		updates = append(updates, segment)
	}

	dh.dist.SegmentDistManager.Update(resp.GetNodeID(), updates...)
}

func (dh *distHandler) updateChannelsDistribution(resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.DmChannel, 0, len(resp.GetChannels()))
	for _, ch := range resp.GetChannels() {
		channelInfo := dh.target.GetDmChannel(ch.GetCollection(), ch.GetChannel(), meta.CurrentTarget)
		var channel *meta.DmChannel
		if channelInfo == nil {
			channel = &meta.DmChannel{
				VchannelInfo: &datapb.VchannelInfo{
					ChannelName:  ch.GetChannel(),
					CollectionID: ch.GetCollection(),
				},
				Node:    resp.GetNodeID(),
				Version: ch.GetVersion(),
			}
		} else {
			channel = channelInfo.Clone()
		}
		updates = append(updates, channel)
	}

	dh.dist.ChannelDistManager.Update(resp.GetNodeID(), updates...)
}

func (dh *distHandler) updateLeaderView(resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.LeaderView, 0, len(resp.GetLeaderViews()))
	for _, lview := range resp.GetLeaderViews() {
		segments := make(map[int64]*meta.Segment)

		for ID, position := range lview.GrowingSegments {
			segments[ID] = &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            ID,
					CollectionID:  lview.GetCollection(),
					StartPosition: position,
					InsertChannel: lview.GetChannel(),
				},
				Node: resp.NodeID,
			}
		}

		var version int64
		for _, channel := range resp.GetChannels() {
			if channel.GetChannel() == lview.GetChannel() {
				version = channel.GetVersion()
				break
			}
		}

		view := &meta.LeaderView{
			ID:              resp.GetNodeID(),
			CollectionID:    lview.GetCollection(),
			Channel:         lview.GetChannel(),
			Version:         version,
			Segments:        lview.GetSegmentDist(),
			GrowingSegments: segments,
			TargetVersion:   lview.TargetVersion,
		}
		updates = append(updates, view)
	}

	dh.dist.LeaderViewManager.Update(resp.GetNodeID(), updates...)
}

func (dh *distHandler) getDistribution(ctx context.Context) (*querypb.GetDataDistributionResponse, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	channels := make(map[string]*msgpb.MsgPosition)
	for _, channel := range dh.dist.ChannelDistManager.GetByNode(dh.nodeID) {
		targetChannel := dh.target.GetDmChannel(channel.GetCollectionID(), channel.GetChannelName(), meta.CurrentTarget)
		if targetChannel == nil {
			continue
		}

		channels[channel.GetChannelName()] = targetChannel.GetSeekPosition()
	}

	ctx, cancel := context.WithTimeout(ctx, distReqTimeout)
	defer cancel()
	resp, err := dh.client.GetDataDistribution(ctx, dh.nodeID, &querypb.GetDataDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetDistribution),
		),
		Checkpoints: channels,
	})

	if err != nil {
		return nil, err
	}
	if !merr.Ok(resp.GetStatus()) {
		return nil, merr.Error(resp.GetStatus())
	}
	return resp, nil
}

func (dh *distHandler) stop() {
	dh.stopOnce.Do(func() {
		close(dh.c)
		dh.wg.Wait()
	})
}

func newDistHandler(
	ctx context.Context,
	nodeID int64,
	client session.Cluster,
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
) *distHandler {
	h := &distHandler{
		nodeID:      nodeID,
		c:           make(chan struct{}),
		client:      client,
		nodeManager: nodeManager,
		scheduler:   scheduler,
		dist:        dist,
		target:      targetMgr,
	}
	h.wg.Add(1)
	go h.start(ctx)
	return h
}
