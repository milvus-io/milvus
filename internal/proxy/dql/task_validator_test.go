package dql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidateTask(t *testing.T) {
	// Initialize paramtable for testing
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxResultEntries.Key, "1000000")
	defer paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxResultEntries.Key, "-1")

	tests := []struct {
		name        string
		task        any
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid search task",
			task: &SearchTask{
				SearchRequest: &internalpb.SearchRequest{
					Nq:         10,
					Topk:       100,
					IsAdvanced: false,
				},
			},
			expectError: false,
		},
		{
			name: "invalid search task",
			task: &SearchTask{
				SearchRequest: &internalpb.SearchRequest{
					Nq:         1000,
					Topk:       2000,
					IsAdvanced: false,
				},
			},
			expectError: true,
			errorMsg:    "number of result entries is too large",
		},
		{
			name: "invalid search task with group size",
			task: &SearchTask{
				SearchRequest: &internalpb.SearchRequest{
					Nq:         100,
					Topk:       200,
					GroupSize:  100,
					IsAdvanced: false,
				},
			},
			expectError: true,
			errorMsg:    "number of result entries is too large",
		},
		{
			name: "invalid search task with sub-request",
			task: &SearchTask{
				SearchRequest: &internalpb.SearchRequest{
					IsAdvanced: true,
					SubReqs: []*internalpb.SubSearchRequest{
						{
							Nq:        100,
							Topk:      200,
							GroupSize: 100,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "number of result entries is too large",
		},
		{
			name:        "non-search task should return nil",
			task:        "not a search task",
			expectError: false,
		},
		{
			name:        "nil task should return nil",
			task:        nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTask(tt.task)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateHybridSubSearchUsesRequestedGroupSize(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxResultEntries.Key, "1000")
	defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.MaxResultEntries.Key)

	// The sub-request keeps more entities per group than requested. Only the
	// requested size (2) counts: 10 * 50 * 2 = 1000 entries passes, while the
	// internal size (20) would give 10000.
	task := &SearchTask{
		SearchRequest: &internalpb.SearchRequest{
			IsAdvanced: true,
			SubReqs:    []*internalpb.SubSearchRequest{{Nq: 10, Topk: 50, GroupSize: 20}},
		},
		rankParams: &rankParams{groupByFieldIds: []int64{102}, groupSize: 2},
	}
	assert.NoError(t, ValidateTask(task))

	task.rankParams.groupSize = 3
	err := ValidateTask(task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "number of result entries is too large")
}
