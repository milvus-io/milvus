package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
			task: &searchTask{
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
			task: &searchTask{
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
			task: &searchTask{
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
			task: &searchTask{
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
