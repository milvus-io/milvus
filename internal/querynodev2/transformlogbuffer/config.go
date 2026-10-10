package transformlogbuffer

import (
	"context"
	"fmt"
	"math"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// New follows the owner's configuration lifetime; view references continue to
// own subscriptions and registrations independently of the config watcher.
func New(ctx context.Context, streams wal.TransformLogStreamManager) *Buffer {
	params := paramtable.Get()
	ratioParam := &params.QueryViewCfg.TransformLogCatchupConcurrencyRatio
	b := newBuffer(streams, max(1, hardware.GetCPUNum()/4))
	refresh := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		// Read under the same lock as resizing so concurrent events cannot
		// overwrite a newer limit with an earlier snapshot.
		ratio := ratioParam.GetAsFloat()
		concurrency, valid := catchupConcurrencyFromRatio(hardware.GetCPUNum(), ratio)
		if !valid {
			mlog.Warn(ctx, "ignore invalid TransformLog catch-up concurrency ratio", mlog.Float64("ratio", ratio))
			return
		}
		b.catchupConcurrency = concurrency
		for pchannel, queue := range b.drainQueues {
			b.startDrainWorkersLocked(pchannel, queue)
		}
	}
	handler := config.NewHandler(fmt.Sprintf("queryview.transformlog.catchup.%p", b), func(event *config.Event) {
		if event.HasUpdated {
			refresh()
		}
	})
	params.Watch(ratioParam.Key, handler)
	context.AfterFunc(ctx, func() { params.Unwatch(ratioParam.Key, handler) })
	// Register first, then read, so an update racing construction is not lost.
	refresh()
	return b
}

func catchupConcurrencyFromRatio(cpu int, ratio float64) (int, bool) {
	capacity := float64(cpu) * ratio
	if cpu <= 0 || ratio <= 0 || math.IsNaN(capacity) || math.IsInf(capacity, 0) || capacity >= float64(math.MaxInt) {
		return 0, false
	}
	return max(1, int(capacity)), true
}
