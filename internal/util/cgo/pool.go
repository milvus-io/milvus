package cgo

import (
	"math"
	"runtime"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var caller *cgoCaller

func initCaller(nodeID string) {
	chSize := int64(math.Ceil(float64(hardware.GetCPUNum()) * paramtable.Get().QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
	if chSize <= 0 {
		chSize = 1
	}
	caller = &cgoCaller{
		ch:     make(chan struct{}, chSize),
		nodeID: nodeID,
	}
}

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
