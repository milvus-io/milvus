package tasks

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestMaterializeSelectedSegmentsStopsOnTaskFailure(t *testing.T) {
	for _, preExecute := range []bool{true, false} {
		t.Run(map[bool]string{true: "prepare", false: "execute"}[preExecute], func(t *testing.T) {
			var prepareErr error
			if preExecute {
				prepareErr = context.Canceled
			}
			prepare := mockey.Mock((*QueryTask).PreExecute).Return(prepareErr).Build()
			defer prepare.UnPatch()
			execute := mockey.Mock((*QueryTask).ExecuteOnSegments).Return(context.Canceled).Build()
			defer execute.UnPatch()
			result, err := ExecuteMaterializeOnSegments(t.Context(), nil, nil, &querypb.QueryRequest{})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			if preExecute {
				require.Zero(t, execute.Times())
			} else {
				require.EqualValues(t, 1, execute.Times())
			}
		})
	}
}
