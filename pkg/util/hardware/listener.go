package hardware

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// systemMetricsWatcher is a hardware monitor that can be used to monitor hardware information.
var (
	defaultMetricMonitorInterval = 1 * time.Second
	systemMericsWatcherOnce      sync.Once
	systemMetricsWatcher         *SystemMericsWatcher
)

// SystemMetrics is the system metrics.
type SystemMetrics struct {
	UsedMemoryBytes  uint64
	TotalMemoryBytes uint64
}

// UsedRatio returns the used ratio of the memory.
func (s SystemMetrics) UsedRatio() float64 {
	if s.TotalMemoryBytes == 0 {
		return 1.0
	}
	return float64(s.UsedMemoryBytes) / float64(s.TotalMemoryBytes)
}

// String returns the string representation of the SystemMetrics.
func (s SystemMetrics) String() string {
	UsedMemoryBytes := float64(s.UsedMemoryBytes) / 1024 / 1024
	TotalMemoryBytes := float64(s.TotalMemoryBytes) / 1024 / 1024
	return fmt.Sprintf("used: %.2fMB, total: %.2fMB", UsedMemoryBytes, TotalMemoryBytes)
}

// SystemMetricsListener is a listener that listens for system metrics.
type SystemMetricsListener struct {
	nextTriggerInstant time.Time
	Context            any
	Cooldown           time.Duration
	Condition          func(SystemMetrics, *SystemMetricsListener) bool // condition to trigger the callback
	Callback           func(SystemMetrics, *SystemMetricsListener)      // callback function if the condition met, should be non-blocking.
}

// RegisterSystemMetricsListener registers a listener into global default systemMetricsWatcher.
func RegisterSystemMetricsListener(listener *SystemMetricsListener) {
	getSystemMetricsWatcher().RegisterListener(listener)
}

// UnregisterSystemMetricsListener unregisters a listener into global default systemMetricsWatcher.
func UnregisterSystemMetricsListener(listener *SystemMetricsListener) {
	getSystemMetricsWatcher().UnregisterListener(listener)
}

// getSystemMetricsWatcher returns the systemMetricsWatcher instance.
func getSystemMetricsWatcher() *SystemMericsWatcher {
	systemMericsWatcherOnce.Do(func() {
		systemMetricsWatcher = NewSystemMetricsWatcher(defaultMetricMonitorInterval)
		logger := log.With(log.FieldComponent("system-metrics"))
		warningLoggerListener := &SystemMetricsListener{
			Cooldown: 1 * time.Minute,
			Condition: func(stats SystemMetrics, listener *SystemMetricsListener) bool {
				return stats.UsedRatio() > 0.9
			},
			Callback: func(sm SystemMetrics, listener *SystemMetricsListener) {
				logger.Warn("memory used ratio is extremely high", zap.String("memory", sm.String()), zap.Float64("usedRatio", sm.UsedRatio()))
			},
		}
		systemMetricsWatcher.RegisterListener(warningLoggerListener)
	})
	return systemMetricsWatcher
}

// NewSystemMetricsWatcher creates a new SystemMericsWatcher.
func NewSystemMetricsWatcher(interval time.Duration) *SystemMericsWatcher {
	w := &SystemMericsWatcher{
		listener: make(map[*SystemMetricsListener]struct{}),
		closed:   make(chan struct{}),
		finished: make(chan struct{}),
	}
	go w.loop(interval)
	return w
}

// SystemMericsWatcher is a hardware monitor that can be used to monitor hardware information.
type SystemMericsWatcher struct {
	mu       sync.Mutex
	listener map[*SystemMetricsListener]struct{}
	closed   chan struct{}
	finished chan struct{}
}

// RegisterListener registers a listener.
func (w *SystemMericsWatcher) RegisterListener(listener *SystemMetricsListener) {
	w.mu.Lock()
	defer w.mu.Unlock()
	newListeners := make(map[*SystemMetricsListener]struct{})
	for l := range w.listener {
		newListeners[l] = struct{}{}
	}
	newListeners[listener] = struct{}{}
	w.listener = newListeners
}

// UnregisterListener unregisters a listener.
func (w *SystemMericsWatcher) UnregisterListener(listener *SystemMetricsListener) {
	w.mu.Lock()
	defer w.mu.Unlock()
	newListeners := make(map[*SystemMetricsListener]struct{})
	for l := range w.listener {
		newListeners[l] = struct{}{}
	}
	delete(newListeners, listener)
	w.listener = newListeners
}

// loop is the main loop of the SystemMericsWatcher.
func (w *SystemMericsWatcher) loop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		close(w.finished)
	}()
	for {
		select {
		case <-w.closed:
			return
		case <-ticker.C:
			w.updateMetrics()
		}
	}
}

func (w *SystemMericsWatcher) updateMetrics() {
	stats := SystemMetrics{
		UsedMemoryBytes:  GetUsedMemoryCount(),
		TotalMemoryBytes: GetMemoryCount(),
	}
	now := time.Now()
	w.mu.Lock()
	listener := w.listener
	w.mu.Unlock()

	for l := range listener {
		if now.Before(l.nextTriggerInstant) {
			// cool down.
			continue
		}
		if l.Condition(stats, l) {
			l.Callback(stats, l)
			l.nextTriggerInstant = now.Add(l.Cooldown)
		}
	}
}

func (s *SystemMericsWatcher) Close() {
	close(s.closed)
	<-s.finished
}
