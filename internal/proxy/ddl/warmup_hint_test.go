package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadCollectionWarmupHintValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		params  map[string]string
		refresh bool
		valid   bool
	}{
		{"omitted", nil, false, true},
		{"true", map[string]string{"warmup_hint": "true"}, false, true},
		{"false", map[string]string{"warmup_hint": "false"}, false, true},
		{"empty", map[string]string{"warmup_hint": ""}, false, false},
		{"not a bool", map[string]string{"warmup_hint": "async"}, false, false},
		{"refresh", map[string]string{"warmup_hint": "true"}, true, false},
		{"refresh without hint", map[string]string{"warmup_hint": "false"}, true, true},
		{"ordinary refresh", nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &LoadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				CollectionName: "warmup_test", LoadParams: tc.params, Refresh: tc.refresh,
			}}
			err := task.PreExecute(context.Background())
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			}
		})
	}
}
