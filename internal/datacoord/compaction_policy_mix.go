package datacoord

import (
	"context"
	"time"
)

var _ CompactionPolicy = (*mixCompactionPolicy)(nil)

type mixCompactionPolicy struct {
	meta   *meta
	view   *FullViews
	ticker *time.Ticker

	emptyLoopCount int
}

func NewMixCompactionPolicy(meta *meta, view *FullViews) *mixCompactionPolicy {
	interval := Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	return &mixCompactionPolicy{
		meta:   meta,
		view:   view,
		ticker: ticker,
	}
}

func (policy *mixCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *mixCompactionPolicy) Stop() {
	policy.ticker.Stop()
}

func (policy *mixCompactionPolicy) Ticker() *time.Ticker {
	return policy.ticker
}

// todo to finish
func (policy *mixCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	// support config hot refresh
	defer policy.ticker.Reset(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
	if !policy.Enable() {
		return make(map[CompactionTriggerType][]CompactionView, 0), nil
	}

	return make(map[CompactionTriggerType][]CompactionView, 0), nil
}
