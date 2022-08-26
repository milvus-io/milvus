package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
)

// TODO(sunby): have too much similar codes with SegmentChecker
type ChannelChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
) *ChannelChecker {
	return &ChannelChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r)...)
		}
	}

	channels := c.dist.ChannelDistManager.GetAll()
	released := utils.FilterReleased(channels, collectionIDs)
	tasks = append(tasks, c.createChannelReduceTasks(ctx, released, -1)...)
	return tasks
}

func (c *ChannelChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	targets := c.targetMgr.GetDmChannelsByCollection(replica.GetCollectionID())
	dists := c.getChannelDist(replica)

	lacks, redundancies := diffChannels(targets, dists)
	tasks := c.createChannelLoadTask(ctx, lacks, replica)
	ret = append(ret, tasks...)
	tasks = c.createChannelReduceTasks(ctx, redundancies, replica.GetID())
	ret = append(ret, tasks...)

	repeated := findRepeatedChannels(dists)
	tasks = c.createChannelReduceTasks(ctx, repeated, replica.GetID())
	ret = append(ret, tasks...)
	return ret
}

func (c *ChannelChecker) getChannelDist(replica *meta.Replica) []*meta.DmChannel {
	dists := make([]*meta.DmChannel, 0)
	for _, nodeID := range replica.Nodes.Collect() {
		dists = append(dists, c.dist.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nodeID)...)
	}
	return dists
}

func diffChannels(targets, dists []*meta.DmChannel) (lacks, redundancies []*meta.DmChannel) {
	distMap := make(map[string]struct{})
	targetMap := make(map[string]struct{})
	for _, ch := range targets {
		targetMap[ch.GetChannelName()] = struct{}{}
	}
	for _, ch := range dists {
		distMap[ch.GetChannelName()] = struct{}{}
		if _, ok := targetMap[ch.GetChannelName()]; !ok {
			redundancies = append(redundancies, ch)
		}
	}
	for _, ch := range targets {
		if _, ok := distMap[ch.GetChannelName()]; !ok {
			lacks = append(lacks, ch)
		}
	}
	return
}

func findRepeatedChannels(dists []*meta.DmChannel) []*meta.DmChannel {
	ret := make([]*meta.DmChannel, 0)
	versionsMap := make(map[string]*meta.DmChannel)
	for _, ch := range dists {
		maxVer, ok := versionsMap[ch.GetChannelName()]
		if !ok {
			versionsMap[ch.GetChannelName()] = ch
			continue
		}
		if maxVer.Version <= ch.Version {
			ret = append(ret, maxVer)
			versionsMap[ch.GetChannelName()] = ch
		} else {
			ret = append(ret, ch)
		}
	}
	return ret
}

func (c *ChannelChecker) createChannelLoadTask(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	plans := c.balancer.AssignChannel(channels, replica.Replica.GetNodes())
	for i := range plans {
		plans[i].ReplicaID = replica.GetID()
	}
	// log.Debug("try to subscribe channels",
	// 	zap.Any("channels", channels),
	// 	zap.Any("plans", plans))
	return balance.CreateChannelTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.ChannelTaskTimeout, plans)
}

func (c *ChannelChecker) createChannelReduceTasks(ctx context.Context, channels []*meta.DmChannel, replicaID int64) []task.Task {
	ret := make([]task.Task, 0, len(channels))
	for _, ch := range channels {
		action := task.NewChannelAction(ch.Node, task.ActionTypeReduce, ch.GetChannelName())
		task := task.NewChannelTask(ctx, Params.QueryCoordCfg.ChannelTaskTimeout, c.ID(), ch.GetCollectionID(), replicaID, action)
		ret = append(ret, task)
	}
	return ret
}
