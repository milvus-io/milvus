package segmentutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// workerCost is one worker's reply cost, and the worker it came from.
type workerCost struct {
	source int64
	cost   *internalpb.CostAggregation
}

func TestAggregateWorkerCosts(t *testing.T) {
	cost := func(rt, related int64) *internalpb.CostAggregation {
		return &internalpb.CostAggregation{ResponseTime: rt, TotalRelatedDataSize: related}
	}
	cases := []struct {
		name         string
		costs        []workerCost
		allWorkers   bool
		wantResponse int64
		wantRelated  int64
	}{
		{name: "no workers", wantRelated: 0},
		{
			name:  "local worker only",
			costs: []workerCost{{1, cost(10, 100)}, {2, cost(50, 200)}},
			// node 1's cost, the related size of both
			wantResponse: 10, wantRelated: 300,
		},
		{
			name:       "all workers: the slowest",
			costs:      []workerCost{{1, cost(10, 100)}, {2, cost(50, 200)}},
			allWorkers: true, wantResponse: 50, wantRelated: 300,
		},
		{
			name:         "no local worker",
			costs:        []workerCost{{2, cost(50, 200)}, {3, nil}},
			wantResponse: 0, wantRelated: 200,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := AggregateWorkerCosts(c.costs, func(w workerCost) (int64, *internalpb.CostAggregation) {
				return w.source, w.cost
			}, 1, c.allWorkers)
			require.NotNil(t, got)
			require.Equal(t, c.wantResponse, got.GetResponseTime())
			require.Equal(t, c.wantRelated, got.GetTotalRelatedDataSize())
		})
	}
}
