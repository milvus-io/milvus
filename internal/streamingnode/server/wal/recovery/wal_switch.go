package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EnableWALSwitch publishes the reader capability fence before a linked record
// can become durable. It changes no replay position and does not wait for the
// AlterWAL message's observation (which would deadlock its own append).
func (r *recoveryStorageImpl) EnableWALSwitch(ctx context.Context) error {
	r.mu.Lock()
	if r.checkpoint.Magic >= utility.RecoveryMagicHotWALSwitch {
		r.mu.Unlock()
		return nil
	}
	if r.walSwitchReady == nil {
		r.walSwitchReady = make(chan struct{})
		r.minimumRecoveryMagic = utility.RecoveryMagicHotWALSwitch
	}
	ready := r.walSwitchReady
	r.mu.Unlock()
	r.notifyPersist()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.backgroundTaskNotifier.FinishChan():
		return merr.WrapErrServiceUnavailableMsg("recovery publisher stopped before WAL switch capability was persisted")
	case <-ready:
		return nil
	}
}
