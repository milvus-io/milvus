package typeutil

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

var _ BackoffTimerConfigFetcher = BackoffTimerConfig{}

// BackoffTimerConfigFetcher is the interface to fetch backoff timer configuration
type BackoffTimerConfigFetcher interface {
	DefaultInterval() time.Duration
	BackoffConfig() BackoffConfig
}

// BackoffTimerConfig is the configuration for backoff timer
// It's also used to be const config fetcher.
// Every DefaultInterval is a fetch loop.
type BackoffTimerConfig struct {
	Default time.Duration
	Backoff BackoffConfig
}

// BackoffConfig is the configuration for backoff
type BackoffConfig struct {
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
}

func (c BackoffTimerConfig) DefaultInterval() time.Duration {
	return c.Default
}

func (c BackoffTimerConfig) BackoffConfig() BackoffConfig {
	return c.Backoff
}

// NewBackoffTimer creates a new balanceTimer
func NewBackoffTimer(configFetcher BackoffTimerConfigFetcher) *BackoffTimer {
	return &BackoffTimer{
		configFetcher: configFetcher,
		backoff:       nil,
	}
}

// BackoffTimer is a timer for balance operation
type BackoffTimer struct {
	configFetcher BackoffTimerConfigFetcher
	backoff       *backoff.ExponentialBackOff
}

// EnableBackoff enables the backoff
func (t *BackoffTimer) EnableBackoff() {
	if t.backoff == nil {
		cfg := t.configFetcher.BackoffConfig()
		defaultInterval := t.configFetcher.DefaultInterval()
		backoff := backoff.NewExponentialBackOff()
		backoff.InitialInterval = cfg.InitialInterval
		backoff.Multiplier = cfg.Multiplier
		backoff.MaxInterval = cfg.MaxInterval
		backoff.MaxElapsedTime = defaultInterval
		backoff.Stop = defaultInterval
		backoff.Reset()
		t.backoff = backoff
	}
}

// DisableBackoff disables the backoff
func (t *BackoffTimer) DisableBackoff() {
	t.backoff = nil
}

// IsBackoffStopped returns the elapsed time of backoff
func (t *BackoffTimer) IsBackoffStopped() bool {
	if t.backoff != nil {
		return t.backoff.GetElapsedTime() > t.backoff.MaxElapsedTime
	}
	return true
}

// NextTimer returns the next timer and the duration of the timer
func (t *BackoffTimer) NextTimer() (<-chan time.Time, time.Duration) {
	nextBackoff := t.NextInterval()
	return time.After(nextBackoff), nextBackoff
}

// NextInterval returns the next interval
func (t *BackoffTimer) NextInterval() time.Duration {
	// if the backoff is enabled, use backoff
	if t.backoff != nil {
		return t.backoff.NextBackOff()
	}
	return t.configFetcher.DefaultInterval()
}
