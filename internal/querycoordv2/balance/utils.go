package balance

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

func CreateSegmentTasksFromPlans(ctx context.Context, checkerID int64, timeout time.Duration, plans []SegmentAssignPlan) []task.Task {
	ret := make([]task.Task, 0)
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewSegmentAction(p.To, task.ActionTypeGrow, p.Segment.GetID())
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewSegmentAction(p.From, task.ActionTypeReduce, p.Segment.GetID())
			actions = append(actions, action)
		}
		task := task.NewSegmentTask(
			ctx,
			timeout,
			checkerID,
			p.Segment.GetCollectionID(),
			p.ReplicaID,
			actions...,
		)
		ret = append(ret, task)
	}
	return ret
}

func CreateChannelTasksFromPlans(ctx context.Context, checkerID int64, timeout time.Duration, plans []ChannelAssignPlan) []task.Task {
	ret := make([]task.Task, 0, len(plans))
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewChannelAction(p.To, task.ActionTypeGrow, p.Channel.GetChannelName())
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewChannelAction(p.From, task.ActionTypeReduce, p.Channel.GetChannelName())
			actions = append(actions, action)
		}
		task := task.NewChannelTask(ctx, timeout, checkerID, p.Channel.GetCollectionID(), p.ReplicaID, actions...)
		ret = append(ret, task)
	}
	return ret
}
