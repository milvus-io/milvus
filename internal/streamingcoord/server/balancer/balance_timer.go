package balancer

import (
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// newBalanceTimer creates a new balanceTimer
func newBalanceTimer() *balanceTimer {
	return &balanceTimer{
		backoff:            backoff.NewExponentialBackOff(),
		newIncomingBackOff: false,
	}
}

// balanceTimer is a timer for balance operation
type balanceTimer struct {
	backoff            *backoff.ExponentialBackOff
	newIncomingBackOff bool
	enableBackoff      bool
}

// EnableBackoffOrNot enables or disables backoff
func (t *balanceTimer) EnableBackoff() {
	t.enableBackoff = true
	t.newIncomingBackOff = true
}

// DisableBackoff disables backoff
func (t *balanceTimer) DisableBackoff() {
	t.enableBackoff = false
}

// NextTimer returns the next timer and the duration of the timer
func (t *balanceTimer) NextTimer() (<-chan time.Time, time.Duration) {
	if !t.enableBackoff {
		balanceInterval := paramtable.Get().StreamingCoordCfg.AutoBalanceTriggerInterval.GetAsDurationByParse()
		return time.After(balanceInterval), balanceInterval
	}
	if t.newIncomingBackOff {
		t.newIncomingBackOff = false
		// reconfig backoff
		t.backoff.InitialInterval = paramtable.Get().StreamingCoordCfg.AutoBalanceBackoffInitialInterval.GetAsDurationByParse()
		t.backoff.Multiplier = paramtable.Get().StreamingCoordCfg.AutoBalanceBackoffMultiplier.GetAsFloat()
		t.backoff.MaxInterval = paramtable.Get().StreamingCoordCfg.AutoBalanceTriggerInterval.GetAsDurationByParse()
		t.backoff.Reset()
	}
	nextBackoff := t.backoff.NextBackOff()
	return time.After(nextBackoff), nextBackoff
}
