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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/samber/lo"
)

const (
	interval   = 1 * time.Second
	RPCTimeout = 3 * time.Second
)

// LeaderObserver is to sync the distribution with leader
type LeaderObserver struct {
	wg          sync.WaitGroup
	closeCh     chan struct{}
	dist        *meta.DistributionManager
	meta        *meta.Meta
	target      *meta.TargetManager
	broker      meta.Broker
	cluster     session.Cluster
	manualCheck chan checkRequest

	stopOnce sync.Once
}

func (o *LeaderObserver) Start(ctx context.Context) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-o.closeCh:
				log.Info("stop leader observer")
				return
			case <-ctx.Done():
				log.Info("stop leader observer due to ctx done")
				return
			case req := <-o.manualCheck:
				log.Info("triggering manual check")
				ret := o.observeCollection(ctx, req.CollectionID)
				req.Notifier <- ret
				log.Info("manual check done", zap.Bool("result", ret))

			case <-ticker.C:
				o.observe(ctx)
			}
		}
	}()
}

func (o *LeaderObserver) Stop() {
	o.stopOnce.Do(func() {
		close(o.closeCh)
		o.wg.Wait()
	})
}

func (o *LeaderObserver) observe(ctx context.Context) {
	o.observeSegmentsDist(ctx)
}

func (o *LeaderObserver) observeSegmentsDist(ctx context.Context) {
	collectionIDs := o.meta.CollectionManager.GetAll()
	for _, cid := range collectionIDs {
		o.observeCollection(ctx, cid)
	}
}

func (o *LeaderObserver) observeCollection(ctx context.Context, collection int64) bool {
	replicas := o.meta.ReplicaManager.GetByCollection(collection)
	result := true
	for _, replica := range replicas {
		leaders := o.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
		for ch, leaderID := range leaders {
			leaderView := o.dist.LeaderViewManager.GetLeaderShardView(leaderID, ch)
			if leaderView == nil {
				continue
			}
			dists := o.dist.SegmentDistManager.GetByShardWithReplica(ch, replica)

			actions := o.findNeedLoadedSegments(leaderView, dists)
			actions = append(actions, o.findNeedRemovedSegments(leaderView, dists)...)
			updateVersionAction := o.checkNeedUpdateTargetVersion(leaderView)
			if updateVersionAction != nil {
				actions = append(actions, updateVersionAction)
			}
			success := o.sync(ctx, replica.GetID(), leaderView, actions)
			if !success {
				result = false
			}
		}
	}
	return result
}

func (ob *LeaderObserver) CheckTargetVersion(collectionID int64) bool {
	notifier := make(chan bool)
	ob.manualCheck <- checkRequest{
		CollectionID: collectionID,
		Notifier:     notifier,
	}
	return <-notifier
}

func (o *LeaderObserver) checkNeedUpdateTargetVersion(leaderView *meta.LeaderView) *querypb.SyncAction {
	targetVersion := o.target.GetCollectionTargetVersion(leaderView.CollectionID, meta.CurrentTarget)

	if targetVersion <= leaderView.TargetVersion {
		return nil
	}

	log.Info("Update readable segment version",
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channelName", leaderView.Channel),
		zap.Int64("nodeID", leaderView.ID),
		zap.Int64("oldVersion", leaderView.TargetVersion),
		zap.Int64("newVersion", targetVersion),
	)

	sealedSegments := o.target.GetHistoricalSegmentsByChannel(leaderView.CollectionID, leaderView.Channel, meta.CurrentTarget)
	growingSegments := o.target.GetStreamingSegmentsByChannel(leaderView.CollectionID, leaderView.Channel, meta.CurrentTarget)

	return &querypb.SyncAction{
		Type:            querypb.SyncType_UpdateVersion,
		GrowingInTarget: growingSegments.Collect(),
		SealedInTarget:  lo.Keys(sealedSegments),
		TargetVersion:   targetVersion,
	}
}

func (o *LeaderObserver) findNeedLoadedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*querypb.SyncAction {
	ret := make([]*querypb.SyncAction, 0)
	dists = utils.FindMaxVersionSegments(dists)
	for _, s := range dists {
		version, ok := leaderView.Segments[s.GetID()]
		currentTarget := o.target.GetHistoricalSegment(s.CollectionID, s.GetID(), meta.CurrentTarget)
		existInCurrentTarget := currentTarget != nil
		existInNextTarget := o.target.GetHistoricalSegment(s.CollectionID, s.GetID(), meta.NextTarget) != nil

		if !existInCurrentTarget && !existInNextTarget {
			continue
		}

		if !ok || version.GetVersion() < s.Version { // Leader misses this segment
			ctx := context.Background()
			resp, err := o.broker.GetSegmentInfo(ctx, s.GetID())
			if err != nil || len(resp.GetInfos()) == 0 {
				log.Warn("failed to get segment info from DataCoord", zap.Error(err))
				continue
			}
			loadInfo := utils.PackSegmentLoadInfo(resp, nil)

			ret = append(ret, &querypb.SyncAction{
				Type:        querypb.SyncType_Set,
				PartitionID: s.GetPartitionID(),
				SegmentID:   s.GetID(),
				NodeID:      s.Node,
				Version:     s.Version,
				Info:        loadInfo,
			})
		}
	}
	return ret
}

func (o *LeaderObserver) findNeedRemovedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*querypb.SyncAction {
	ret := make([]*querypb.SyncAction, 0)
	distMap := make(map[int64]struct{})
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
	}
	for sid, s := range leaderView.Segments {
		_, ok := distMap[sid]
		existInCurrentTarget := o.target.GetHistoricalSegment(leaderView.CollectionID, sid, meta.CurrentTarget) != nil
		existInNextTarget := o.target.GetHistoricalSegment(leaderView.CollectionID, sid, meta.NextTarget) != nil
		if ok || existInCurrentTarget || existInNextTarget {
			continue
		}
		log.Debug("leader observer append a segment to remove:", zap.Int64("collectionID", leaderView.CollectionID),
			zap.String("Channel", leaderView.Channel), zap.Int64("leaderViewID", leaderView.ID),
			zap.Int64("segmentID", sid), zap.Bool("distMap_exist", ok),
			zap.Bool("existInCurrentTarget", existInCurrentTarget),
			zap.Bool("existInNextTarget", existInNextTarget))
		ret = append(ret, &querypb.SyncAction{
			Type:      querypb.SyncType_Remove,
			SegmentID: sid,
			NodeID:    s.NodeID,
		})
	}
	return ret
}

func (o *LeaderObserver) sync(ctx context.Context, replicaID int64, leaderView *meta.LeaderView, diffs []*querypb.SyncAction) bool {
	if len(diffs) == 0 {
		return true
	}

	log := log.With(
		zap.Int64("leaderID", leaderView.ID),
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channel", leaderView.Channel),
	)

	schema, err := o.broker.GetCollectionSchema(ctx, leaderView.CollectionID)
	if err != nil {
		log.Error("sync distribution failed, cannot get schema of collection", zap.Error(err))
		return false
	}
	partitions, err := utils.GetPartitions(o.meta.CollectionManager, leaderView.CollectionID)
	if err != nil {
		log.Error("sync distribution failed, cannot get partitions of collection", zap.Error(err))
		return false
	}

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
		),
		CollectionID: leaderView.CollectionID,
		ReplicaID:    replicaID,
		Channel:      leaderView.Channel,
		Actions:      diffs,
		Schema:       schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     o.meta.GetLoadType(leaderView.CollectionID),
			CollectionID: leaderView.CollectionID,
			PartitionIDs: partitions,
		},
		Version: time.Now().UnixNano(),
	}
	resp, err := o.cluster.SyncDistribution(ctx, leaderView.ID, req)
	if err != nil {
		log.Error("failed to sync distribution", zap.Error(err))
		return false
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("failed to sync distribution", zap.String("reason", resp.GetReason()))
		return false
	}

	return true
}

func NewLeaderObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	broker meta.Broker,
	cluster session.Cluster,
) *LeaderObserver {
	return &LeaderObserver{
		closeCh:     make(chan struct{}),
		dist:        dist,
		meta:        meta,
		target:      targetMgr,
		broker:      broker,
		cluster:     cluster,
		manualCheck: make(chan checkRequest, 10),
	}
}
