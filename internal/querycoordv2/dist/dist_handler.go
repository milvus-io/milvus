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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TriggerUpdateTargetVersion = func(collectionID int64)

type distHandler struct {
	nodeID       int64
	c            chan struct{}
	wg           sync.WaitGroup
	client       session.Cluster
	nodeManager  *session.NodeManager
	scheduler    task.Scheduler
	dist         *meta.DistributionManager
	target       meta.TargetManagerInterface
	mu           sync.Mutex
	stopOnce     sync.Once
	lastUpdateTs int64

	syncTargetVersionFn TriggerUpdateTargetVersion
}

func (dh *distHandler) start(ctx context.Context) {
	defer dh.wg.Done()
	log := log.Ctx(ctx).With(zap.Int64("nodeID", dh.nodeID)).WithRateGroup("qcv2.distHandler", 1, 60)
	log.Info("start dist handler")
	ticker := time.NewTicker(Params.QueryCoordCfg.DistPullInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	checkExecutedFlagTicker := time.NewTicker(Params.QueryCoordCfg.CheckExecutedFlagInterval.GetAsDuration(time.Millisecond))
	defer checkExecutedFlagTicker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("close dist handler due to context done")
			return
		case <-dh.c:
			log.Info("close dist handler")
			return
		case <-checkExecutedFlagTicker.C:
			executedFlagChan := dh.scheduler.GetExecutedFlag(dh.nodeID)
			if executedFlagChan != nil {
				select {
				case <-executedFlagChan:
					dh.pullDist(ctx, &failures, false)
				default:
				}
			}
		case <-ticker.C:
			dh.pullDist(ctx, &failures, true)
		}
	}
}

func (dh *distHandler) pullDist(ctx context.Context, failures *int, dispatchTask bool) {
	tr := timerecord.NewTimeRecorder("")
	resp, err := dh.getDistribution(ctx)
	d1 := tr.RecordSpan()
	if err != nil {
		node := dh.nodeManager.Get(dh.nodeID)
		*failures = *failures + 1
		fields := []zap.Field{zap.Int("times", *failures)}
		if node != nil {
			fields = append(fields, zap.Time("lastHeartbeat", node.LastHeartbeat()))
		}
		fields = append(fields, zap.Error(err))
		log.Ctx(ctx).WithRateGroup("distHandler.pullDist", 1, 60).
			RatedWarn(30.0, "failed to get data distribution", fields...)
	} else {
		*failures = 0
		dh.handleDistResp(resp, dispatchTask)
	}
	log.Ctx(ctx).WithRateGroup("distHandler.pullDist", 1, 120).
		RatedInfo(120.0, "pull and handle distribution done",
			zap.Int("respSize", proto.Size(resp)), zap.Duration("pullDur", d1), zap.Duration("handleDur", tr.RecordSpan()))
}

func (dh *distHandler) handleDistResp(resp *querypb.GetDataDistributionResponse, dispatchTask bool) {
	node := dh.nodeManager.Get(resp.GetNodeID())
	if node == nil {
		return
	}

	if time.Since(node.LastHeartbeat()) > paramtable.Get().QueryCoordCfg.HeartBeatWarningLag.GetAsDuration(time.Millisecond) {
		log.Warn("node last heart beat time lag too behind", zap.Time("now", time.Now()),
			zap.Time("lastHeartBeatTime", node.LastHeartbeat()), zap.Int64("nodeID", node.ID()))
	}
	node.SetLastHeartbeat(time.Now())

	// skip  update dist if no distribution change happens in query node
	if resp.GetLastModifyTs() != 0 && resp.GetLastModifyTs() <= dh.lastUpdateTs {
		log.RatedInfo(30, "skip update dist due to no distribution change", zap.Int64("lastModifyTs", resp.GetLastModifyTs()), zap.Int64("lastUpdateTs", dh.lastUpdateTs))
	} else {
		dh.lastUpdateTs = resp.GetLastModifyTs()

		node.UpdateStats(
			session.WithSegmentCnt(len(resp.GetSegments())),
			session.WithChannelCnt(len(resp.GetChannels())),
			session.WithMemCapacity(resp.GetMemCapacityInMB()),
		)
		dh.updateSegmentsDistribution(resp)
		dh.updateChannelsDistribution(resp)
		dh.updateLeaderView(resp)
	}

	if dispatchTask {
		dh.scheduler.Dispatch(dh.nodeID)
	}
}

func (dh *distHandler) updateSegmentsDistribution(resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.Segment, 0, len(resp.GetSegments()))
	for _, s := range resp.GetSegments() {
		segmentInfo := dh.target.GetSealedSegment(s.GetCollection(), s.GetID(), meta.CurrentTargetFirst)
		if segmentInfo == nil {
			segmentInfo = &datapb.SegmentInfo{
				ID:            s.GetID(),
				CollectionID:  s.GetCollection(),
				PartitionID:   s.GetPartition(),
				InsertChannel: s.GetChannel(),
				Level:         s.GetLevel(),
			}
		}
		updates = append(updates, &meta.Segment{
			SegmentInfo:        segmentInfo,
			Node:               resp.GetNodeID(),
			Version:            s.GetVersion(),
			LastDeltaTimestamp: s.GetLastDeltaTimestamp(),
			IndexInfo:          s.GetIndexInfo(),
		})
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
			channel = &meta.DmChannel{
				VchannelInfo: channelInfo.VchannelInfo,
				Node:         resp.GetNodeID(),
				Version:      ch.GetVersion(),
			}
		}
		updates = append(updates, channel)
	}

	dh.dist.ChannelDistManager.Update(resp.GetNodeID(), updates...)
}

func (dh *distHandler) updateLeaderView(resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.LeaderView, 0, len(resp.GetLeaderViews()))

	channels := lo.SliceToMap(resp.GetChannels(), func(channel *querypb.ChannelVersionInfo) (string, *querypb.ChannelVersionInfo) {
		return channel.GetChannel(), channel
	})

	collectionsToSync := typeutil.NewUniqueSet()
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
		channel, ok := channels[lview.GetChannel()]
		if ok {
			version = channel.GetVersion()
		}

		view := &meta.LeaderView{
			ID:                     resp.GetNodeID(),
			CollectionID:           lview.GetCollection(),
			Channel:                lview.GetChannel(),
			Version:                version,
			Segments:               lview.GetSegmentDist(),
			GrowingSegments:        segments,
			TargetVersion:          lview.TargetVersion,
			NumOfGrowingRows:       lview.GetNumOfGrowingRows(),
			PartitionStatsVersions: lview.PartitionStatsVersions,
		}
		updates = append(updates, view)

		// check leader serviceable
		if err := utils.CheckDelegatorDataReady(dh.nodeManager, dh.target, view, meta.CurrentTarget); err != nil {
			view.UnServiceableError = err
			log.Ctx(context.TODO()).
				WithRateGroup(fmt.Sprintf("distHandler.updateLeaderView.%s", view.Channel), 1, 60).
				RatedInfo(10, "leader is not available due to distribution not ready",
					zap.Int64("collectionID", view.CollectionID),
					zap.Int64("nodeID", view.ID),
					zap.String("channel", view.Channel),
					zap.Error(err))
			continue
		}

		// if target version hasn't been synced, delegator will get empty readable segment list
		// so shard leader should be unserviceable until target version is synced
		currentTargetVersion := dh.target.GetCollectionTargetVersion(lview.GetCollection(), meta.CurrentTarget)
		if lview.TargetVersion <= 0 {
			err := merr.WrapErrServiceInternal(fmt.Sprintf("target version mismatch, collection: %d, channel: %s,  current target version: %v, leader version: %v",
				lview.GetCollection(), lview.GetChannel(), currentTargetVersion, lview.TargetVersion))

			view.UnServiceableError = err
			// make dist handler pull next distribution until all delegator is serviceable
			dh.lastUpdateTs = 0
			collectionsToSync.Insert(lview.Collection)
			log.Ctx(context.TODO()).
				WithRateGroup(fmt.Sprintf("distHandler.updateLeaderView.%s", view.Channel), 1, 60).
				RatedInfo(10, "leader is not available due to target version not ready",
					zap.Int64("collectionID", view.CollectionID),
					zap.Int64("nodeID", view.ID),
					zap.String("channel", view.Channel),
					zap.Error(err))
		}
	}

	dh.dist.LeaderViewManager.Update(resp.GetNodeID(), updates...)

	// segment and channel already loaded, trigger target observer to update
	collectionsToSync.Range(func(collection int64) bool {
		dh.syncTargetVersionFn(collection)
		return true
	})
}

func (dh *distHandler) getDistribution(ctx context.Context) (*querypb.GetDataDistributionResponse, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.DistributionRequestTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := dh.client.GetDataDistribution(ctx, dh.nodeID, &querypb.GetDataDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetDistribution),
		),
		LastUpdateTs: dh.lastUpdateTs,
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

		// clear dist
		dh.dist.ChannelDistManager.Update(dh.nodeID)
		dh.dist.SegmentDistManager.Update(dh.nodeID)
	})
}

func newDistHandler(
	ctx context.Context,
	nodeID int64,
	client session.Cluster,
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	syncTargetVersionFn TriggerUpdateTargetVersion,
) *distHandler {
	h := &distHandler{
		nodeID:              nodeID,
		c:                   make(chan struct{}),
		client:              client,
		nodeManager:         nodeManager,
		scheduler:           scheduler,
		dist:                dist,
		target:              targetMgr,
		syncTargetVersionFn: syncTargetVersionFn,
	}
	h.wg.Add(1)
	go h.start(ctx)
	return h
}
