package cgo

import (
	"runtime"
	"time"

	"github.com/milvus-io/milvus/pkg/metrics"
)

var caller *cgoCaller

// getCGOCaller returns the cgoCaller instance.
func getCGOCaller() *cgoCaller {
	return caller
}

// cgoCaller is a limiter to restrict the number of concurrent cgo calls.
type cgoCaller struct {
	ch     chan struct{}
	nodeID string
}

// call calls the work function with a lock to restrict the number of concurrent cgo calls.
// it collect some metrics too.
func (c *cgoCaller) call(name string, work func()) {
	start := time.Now()
	c.ch <- struct{}{}
	queueTime := time.Since(start)
	metrics.CGOQueueDuration.WithLabelValues(c.nodeID).Observe(queueTime.Seconds())

	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		<-c.ch

		metrics.RunningCgoCallTotal.WithLabelValues(c.nodeID).Dec()
		total := time.Since(start) - queueTime
		metrics.CGODuration.WithLabelValues(c.nodeID, name).Observe(total.Seconds())
	}()
	metrics.RunningCgoCallTotal.WithLabelValues(c.nodeID).Inc()
	work()
}
