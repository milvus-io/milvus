package viewerror

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBusinessErrorDetailsPreserveCodeAndClassification(t *testing.T) {
	for _, source := range []error{
		merr.WrapErrFieldNotFound(int64(100), "field removed"),
		merr.WrapErrServiceNotReadyMsg("query resources are preparing"),
		merr.WrapErrServiceInternalMsg("execution failed"),
	} {
		wire := NewGRPCStatusFromError(source)
		decoded := ConvertViewError("QueryOnView", wire.Err())
		require.Equal(t, merr.Code(source), merr.Code(decoded))
		require.Equal(t, merr.GetErrorType(source), merr.GetErrorType(decoded))
		require.Equal(t, merr.IsRetryableErr(source), merr.IsRetryableErr(decoded))
		// The ViewError cause remains part of the current client error text.
		require.NotNil(t, decoded.(*ViewClientStatus).TryIntoViewError())
		require.Equal(t, source.Error(), AsViewError(decoded).Cause)
		require.Contains(t, decoded.Error(), source.Error())
	}
	for _, source := range []*ViewError{NewViewInvalidated("replaced"), NewOnShutdownError("moving")} {
		wire := NewGRPCStatusFromError(source)
		require.Len(t, wire.Details(), 1)
		require.Equal(t, source.Code, AsViewError(ConvertViewError("QueryOnView", wire.Err())).Code)
	}
	require.NoError(t, NewGRPCStatusFromError(nil).Err())
}
