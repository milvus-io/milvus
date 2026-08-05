package segmentutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestMergeRequestCost(t *testing.T) {
	assert.Nil(t, MergeRequestCost(nil))
	assert.Nil(t, MergeRequestCost([]*internalpb.CostAggregation{nil}))

	slow := &internalpb.CostAggregation{ResponseTime: 800, ServiceTime: 300, TotalNQ: 10, TotalRelatedDataSize: 64}
	backlogged := &internalpb.CostAggregation{ResponseTime: 200, ServiceTime: 100, TotalNQ: 2000, TotalRelatedDataSize: 32}

	// Latency fields follow the slowest snapshot; totalNQ takes the per-field
	// maximum so the backlogged worker stays visible to the balancer.
	merged := MergeRequestCost([]*internalpb.CostAggregation{slow, nil, backlogged})
	assert.Equal(t, int64(800), merged.GetResponseTime())
	assert.Equal(t, int64(300), merged.GetServiceTime())
	assert.Equal(t, int64(2000), merged.GetTotalNQ())
	assert.Equal(t, int64(64), merged.GetTotalRelatedDataSize())
	// The overwrite happens on a copy, never on a worker's response message.
	assert.Equal(t, int64(10), slow.GetTotalNQ())

	// When the slowest snapshot already carries the max totalNQ, it is
	// returned as-is.
	both := &internalpb.CostAggregation{ResponseTime: 900, ServiceTime: 400, TotalNQ: 3000}
	assert.Same(t, both, MergeRequestCost([]*internalpb.CostAggregation{slow, backlogged, both}))
}
