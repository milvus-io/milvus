package resource

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// SplitLifecycle is DataCoord's cluster-local data plane. The combined
// coordinator injects its recovered Server directly, without a second RPC or
// a replicated adoption message.
type SplitLifecycle interface {
	ListShardSplits() []*datapb.SplitShardTask
	AdoptShardSplit(context.Context, int64) error
	ResumeShardSplitL0(context.Context, int64) error
	ReleaseShardSplitPins(context.Context, int64) (bool, error)
	RetireShardSplit(context.Context, int64) error
}
