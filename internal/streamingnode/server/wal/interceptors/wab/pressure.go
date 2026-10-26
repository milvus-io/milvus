package wab

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const pressureSource = "writeAheadBuffer"

// SetPressureController binds the WAL's producer notifications and installs live
// configuration watchers. Call once during WAL open, before publishing the owner.
func (w *WriteAheadBuffer) SetPressureController(registry *ratelimit.MuxRateLimitObserverRegistryImpl) {
	w.pressureRegistry = registry
	w.pressureWatcher = config.NewHandler(fmt.Sprintf("wab-pressure-%p", w), func(*config.Event) { w.refreshPressure() })
	for _, key := range pressureConfigKeys() {
		paramtable.Get().Watch(key, w.pressureWatcher)
	}
	w.refreshPressure()
}

func pressureConfigKeys() []string {
	cfg := &paramtable.Get().StreamingCfg
	return []string{cfg.WALRateLimitWABSlowdownRatio.Key, cfg.WALRateLimitWABSlowdownRate.Key, cfg.WALRateLimitWABRejectRatio.Key}
}

// pressureState uses the current backlog ratio, never time spent in a mode.
// The rate falls linearly from baseRate at threshold to zero at rejectRatio.
func pressureState(size int, threshold float64, baseRate int64, rejectRatio float64) ratelimit.RateLimitState {
	if float64(size) <= threshold {
		return ratelimit.NewNormalRateLimitState()
	}
	ratio := float64(size) / threshold
	if threshold <= 0 || ratio >= rejectRatio {
		return ratelimit.RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT}
	}
	return ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  max(1, int64(float64(baseRate)*(rejectRatio-ratio)/(rejectRatio-1))),
	}
}

func (w *WriteAheadBuffer) refreshPressure() {
	// Serialize publications, but never notify observers under the queue lock.
	// Read the current size after taking this lock so a delayed append callback
	// cannot overwrite a newer release with stale pressure.
	w.pressureMu.Lock()
	defer w.pressureMu.Unlock()
	w.cond.L.Lock()
	registry := w.pressureRegistry
	size, capacity, closed := w.pendingMessages.Size(), w.pendingMessages.capacity, w.closed
	w.cond.L.Unlock()
	if registry == nil {
		return
	}
	state := ratelimit.NewNormalRateLimitState()
	if !closed {
		cfg := &paramtable.Get().StreamingCfg
		threshold := float64(capacity) * cfg.WALRateLimitWABSlowdownRatio.GetAsFloat()
		baseRate := cfg.WALRateLimitWABSlowdownRate.GetAsSize()
		// Invalid live rates retain the documented safe default.
		if baseRate <= 0 {
			baseRate = 32 * 1024 * 1024
		}
		state = pressureState(size, threshold, baseRate, cfg.WALRateLimitWABRejectRatio.GetAsFloat())
	}
	if state != w.pressureState {
		w.pressureState = state
		registry.NotifySourceRateLimitState(pressureSource, state)
	}
}
