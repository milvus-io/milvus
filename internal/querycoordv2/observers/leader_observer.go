package observers

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

const interval = 1 * time.Second

// LeaderObserver is to sync the distribution with leader
type LeaderObserver struct {
	wg      sync.WaitGroup
	closeCh chan struct{}
	dist    *meta.DistributionManager
	meta    *meta.Meta
	target  *meta.TargetManager
	cluster session.Cluster
}

func (o *LeaderObserver) Start(ctx context.Context) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-o.closeCh:
				log.Info("stop leader observer")
				return
			case <-ctx.Done():
				log.Info("stop leader observer due to ctx done")
				return
			case <-ticker.C:
				o.observe(ctx)
			}
		}
	}()
}

func (o *LeaderObserver) Stop() {
	close(o.closeCh)
	o.wg.Wait()
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

func (o *LeaderObserver) observeCollection(ctx context.Context, collection int64) {
	replicas := o.meta.ReplicaManager.GetByCollection(collection)
	for _, replica := range replicas {
		leaders := o.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
		for ch, leaderID := range leaders {
			leaderView := o.dist.LeaderViewManager.GetLeaderShardView(leaderID, ch)
			if leaderView == nil {
				continue
			}
			dists := o.dist.SegmentDistManager.GetByShard(ch)
			needLoaded, needRemoved := o.findNeedLoadedSegments(leaderView, dists),
				o.findNeedRemovedSegments(leaderView, dists)
			o.sync(ctx, leaderView, append(needLoaded, needRemoved...))
		}
	}
}

func (o *LeaderObserver) findNeedLoadedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*querypb.SyncAction {
	ret := make([]*querypb.SyncAction, 0)
	dists = utils.FindMaxVersionSegments(dists)
	for _, s := range dists {
		node, ok := leaderView.Segments[s.GetID()]
		consistentOnLeader := ok && node == s.Node
		if consistentOnLeader || !o.target.ContainSegment(s.GetID()) {
			continue
		}
		ret = append(ret, &querypb.SyncAction{
			Type:        querypb.SyncType_Set,
			PartitionID: s.GetPartitionID(),
			SegmentID:   s.GetID(),
			NodeID:      s.Node,
		})
	}
	return ret
}

func (o *LeaderObserver) findNeedRemovedSegments(leaderView *meta.LeaderView, dists []*meta.Segment) []*querypb.SyncAction {
	ret := make([]*querypb.SyncAction, 0)
	distMap := make(map[int64]struct{})
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
	}
	for sid := range leaderView.Segments {
		_, ok := distMap[sid]
		if ok || o.target.ContainSegment(sid) {
			continue
		}
		ret = append(ret, &querypb.SyncAction{
			Type:      querypb.SyncType_Remove,
			SegmentID: sid,
		})
	}
	return ret
}

func (o *LeaderObserver) sync(ctx context.Context, leaderView *meta.LeaderView, diffs []*querypb.SyncAction) {
	log := log.With(
		zap.Int64("leaderID", leaderView.ID),
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.String("channel", leaderView.Channel),
	)
	req := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SyncDistribution,
		},
		CollectionID: leaderView.CollectionID,
		Channel:      leaderView.Channel,
		Actions:      diffs,
	}
	resp, err := o.cluster.SyncDistribution(ctx, leaderView.ID, req)
	if err != nil {
		log.Error("failed to sync distribution", zap.Error(err))
		return
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("failed to sync distribution", zap.String("reason", resp.GetReason()))
	}
}

func NewLeaderObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	cluster session.Cluster,
) *LeaderObserver {
	return &LeaderObserver{
		closeCh: make(chan struct{}),
		dist:    dist,
		meta:    meta,
		target:  targetMgr,
		cluster: cluster,
	}
}
