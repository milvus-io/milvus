package datacoord

import (
	"context"
	"time"
)

type CompactionPolicy interface {
	Enable() bool
	Ticker() *time.Ticker
	Stop()
	Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error)
}
