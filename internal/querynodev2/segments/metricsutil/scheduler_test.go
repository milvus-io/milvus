package metricsutil

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestScheduler(t *testing.T) {
	paramtable.Get().Save("queryNode.metricScrapeInterval", "0.1")
	scheduler := newScheduler()
	scheduler.Run()

	// small qps
	for i := 0; i < 1000; i++ {
		NewQuerySegmentAccessRecord(testLabel).Finish(nil)
		time.Sleep(1 * time.Millisecond)
	}

	scheduler.Stop()

	StartScheduler()
}
