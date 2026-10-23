package recovery

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// observeQueryBarriers passively consumes an ordered MVCCBarrier.
// It never drives writes or waits for a requested MVCC. Caller holds r.mu.
func (r *recoveryStorageImpl) observeQueryBarriers(ctx context.Context, msg message.ImmutableMessage) {
	if msg.MessageType() != message.MessageTypeMVCCBarrier {
		return
	}
	tick := msg.TimeTick()
	for _, channel := range message.MustAsImmutableMVCCBarrierMessageV2(msg).Header().GetVchannels() {
		module := r.vchannelManager.Module(channel)
		if module == nil {
			continue
		}
		// Summary coverage precedes any runtime bootstrap at the new base.
		r.summaryManager.ObserveQueryBarrier(channel, tick)
		module.ObserveQueryBarrier(ctx, tick)
	}
}
