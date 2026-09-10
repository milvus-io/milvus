package status

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestStreamingStatus(t *testing.T) {
	err := ConvertStreamingError("test", nil)
	assert.Nil(t, err)
	err = ConvertStreamingError("test", errors.Wrap(context.DeadlineExceeded, "test"))
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	err = ConvertStreamingError("test", errors.New("test"))
	assert.NotNil(t, err)
	streamingErr := AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test; rpc error: code = Unknown, desc = test")

	err = ConvertStreamingError("test", NewGRPCStatusFromStreamingError(NewOnShutdownError("test")).Err())
	assert.NotNil(t, err)
	streamingErr = AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test")
	assert.Contains(t, err.Error(), "streaming error")
}

func TestNewGRPCStatusFromStreamingError(t *testing.T) {
	st := NewGRPCStatusFromStreamingError(nil)
	assert.Equal(t, codes.OK, st.Code())

	st = NewGRPCStatusFromStreamingError(
		NewOnShutdownError("test"),
	)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	st = NewGRPCStatusFromStreamingError(
		NewUnrecoverableError("test"),
	)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	st = NewGRPCStatusFromStreamingError(
		NewSchemaVersionMismatch("test"),
	)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	st = NewGRPCStatusFromStreamingError(
		NewRateLimitRejected("test"),
	)
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	st = NewGRPCStatusFromStreamingError(
		New(10086, "test"),
	)
	assert.Equal(t, codes.Unknown, st.Code())
}

func TestPartialUpdateCASErrorsGRPCMapping(t *testing.T) {
	cases := []struct {
		name string
		err  *StreamingError
		code codes.Code
	}{
		{"retryable", NewPartialUpdateRetryable("retry"), codes.Aborted},
		{"malformed", NewUnrecoverableError("bad"), codes.FailedPrecondition},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := NewGRPCStatusFromStreamingError(tc.err)
			require.Equal(t, tc.code, st.Code())
			roundTrip := ConvertStreamingError("test", st.Err())
			se := AsStreamingError(roundTrip)
			require.Equal(t, tc.err.Code, se.Code)
		})
	}
}

// TestShardFencedSurvivesTheWire pins the property the split coordinator's crash
// recovery rests on.
//
// A re-fence is rejected with SHARD_FENCED carrying T_switch, and the
// coordinator reads T_switch back off that error. In-process the error IS the
// *StreamingError, so every field survives no matter what the conversion does --
// which is exactly why this has to be tested through a real grpc round trip:
// rebuilding the error from (code, cause) alone loses T_switch and the loss is
// invisible until the coordinator and the streamingnode sit in different
// processes, i.e. in every cluster deployment.
func TestShardFencedSurvivesTheWire(t *testing.T) {
	original := NewShardFenced("by-dev-rootcoord-dml_0_100v0", 447856455348518913, 0)

	st := NewGRPCStatusFromStreamingError(original)
	// A terminal condition must not look like an unknown transport failure.
	require.Equal(t, codes.FailedPrecondition, st.Code())

	roundTrip := AsStreamingError(ConvertStreamingError("test", st.Err()))
	require.NotNil(t, roundTrip)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_SHARD_FENCED, roundTrip.Code)
	assert.True(t, roundTrip.IsShardFenced())
	assert.True(t, roundTrip.IsUnrecoverable())
	assert.Contains(t, roundTrip.Cause, "by-dev-rootcoord-dml_0_100v0")
	assert.Equal(t, uint64(447856455348518913), roundTrip.FencedTimeTick,
		"T_switch must survive the wire; without it a coordinator that crashed after fencing cannot recover it")
}

func TestRoutingStaleGRPCMapping(t *testing.T) {
	st := NewGRPCStatusFromStreamingError(NewRoutingStale("stale"))
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	roundTrip := AsStreamingError(ConvertStreamingError("test", st.Err()))
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ROUTING_STALE, roundTrip.Code)
}

// TestCauseWithFormatVerbSurvivesTheWire guards the conversion against causes
// that contain printf verbs: the cause is data, never a format string.
func TestCauseWithFormatVerbSurvivesTheWire(t *testing.T) {
	original := NewUnrecoverableError("%s")
	st := NewGRPCStatusFromStreamingError(original)
	roundTrip := AsStreamingError(ConvertStreamingError("test", st.Err()))
	assert.Contains(t, roundTrip.Cause, "%s")
	assert.NotContains(t, roundTrip.Cause, "%!s(MISSING)")
}

// TestNewFromPBErrorCarriesTheWholeMessage guards the helper both conversion
// paths now share: the streaming response body and the gRPC status details.
func TestNewFromPBErrorCarriesTheWholeMessage(t *testing.T) {
	// Never nil: the caller dereferences the result through pointer receivers.
	empty := NewFromPBError(nil)
	require.NotNil(t, empty)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN, empty.Code)

	original := NewShardFenced("by-dev-rootcoord-dml_0_100v0", 447856455348518913, 0)
	pb := original.AsPBError()
	converted := NewFromPBError(pb)
	require.NotNil(t, converted)
	assert.Equal(t, original.Code, converted.Code)
	assert.Equal(t, original.Cause, converted.Cause)
	assert.Equal(t, original.FencedTimeTick, converted.FencedTimeTick)

	// Cloned, not aliased: the transport may reuse the response message.
	pb.FencedTimeTick = 0
	assert.Equal(t, uint64(447856455348518913), converted.FencedTimeTick)
}
