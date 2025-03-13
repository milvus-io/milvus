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
		backoff := backoff.NewExponentialBackOff()
		backoff.InitialInterval = cfg.InitialInterval
		backoff.Multiplier = cfg.Multiplier
		backoff.MaxInterval = cfg.MaxInterval
		backoff.MaxElapsedTime = 0
		backoff.Reset()
		t.backoff = backoff
	}
}

// DisableBackoff disables the backoff
func (t *BackoffTimer) DisableBackoff() {
	t.backoff = nil
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

// NewBackoffWithInstant creates a new backoff with instant
func NewBackoffWithInstant(fetcher BackoffTimerConfigFetcher) *BackoffWithInstant {
	cfg := fetcher.BackoffConfig()
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = cfg.InitialInterval
	backoff.Multiplier = cfg.Multiplier
	backoff.MaxInterval = cfg.MaxInterval
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	return &BackoffWithInstant{
		backoff:     backoff,
		nextInstant: time.Now(),
	}
}

// BackoffWithInstant is a backoff with instant.
// A instant can be recorded with `UpdateInstantWithNextBackOff`
// NextInstant can be used to make priority decision.
type BackoffWithInstant struct {
	backoff     *backoff.ExponentialBackOff
	nextInstant time.Time
}

// NextInstant returns the next instant
func (t *BackoffWithInstant) NextInstant() time.Time {
	return t.nextInstant
}

// NextInterval returns the next interval
func (t *BackoffWithInstant) NextInterval() time.Duration {
	return time.Until(t.nextInstant)
}

// NextTimer returns the next timer and the duration of the timer
func (t *BackoffWithInstant) NextTimer() (<-chan time.Time, time.Duration) {
	next := time.Until(t.nextInstant)
	return time.After(next), next
}

// UpdateInstantWithNextBackOff updates the next instant with next backoff
func (t *BackoffWithInstant) UpdateInstantWithNextBackOff() {
	t.nextInstant = time.Now().Add(t.backoff.NextBackOff())
}
