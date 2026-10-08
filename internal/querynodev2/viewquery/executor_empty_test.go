package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDirectExecutorRejectsEmptyTasks(t *testing.T) {
	// Nil runners also prove that no execution or native access is attempted.
	executor := &DirectSegmentTaskExecutor{}
	for _, tasks := range [][]qnview.QNSearchSegmentTask{nil, {}} {
		result, err := executor.Search(context.Background(), tasks)
		require.Nil(t, result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
	for _, tasks := range [][]qnview.QNQuerySegmentTask{nil, {}} {
		result, err := executor.Query(context.Background(), tasks)
		require.Nil(t, result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}
