package status

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

var streamingErrorToGRPCStatus = map[streamingpb.StreamingCode]codes.Code{
	streamingpb.StreamingCode_STREAMING_CODE_OK:                     codes.OK,
	streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_EXIST:          codes.AlreadyExists,
	streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST:      codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED:         codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN:            codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ:    codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM: codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION:      codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_INNER:                  codes.Unavailable,
	streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN:                codes.Unknown,
}

// NewGRPCStatusFromStreamingError converts StreamingError to grpc status.
func NewGRPCStatusFromStreamingError(e *StreamingError) *status.Status {
	if e == nil || e.Code == streamingpb.StreamingCode_STREAMING_CODE_OK {
		return status.New(codes.OK, "")
	}

	code, ok := streamingErrorToGRPCStatus[e.Code]
	if !ok {
		code = codes.Unknown
	}

	// Attach log error to detail.
	st := status.New(code, "")
	newST, err := st.WithDetails(e.AsPBError())
	if err != nil {
		return status.New(code, fmt.Sprintf("convert log error failed, detail: %s", e.Cause))
	}
	return newST
}
