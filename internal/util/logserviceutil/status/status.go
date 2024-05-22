package status

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var logErrorToGRPCStatus = map[logpb.LogCode]codes.Code{
	logpb.LogCode_LOG_CODE_OK:                     codes.OK,
	logpb.LogCode_LOG_CODE_CHANNEL_EXIST:          codes.AlreadyExists,
	logpb.LogCode_LOG_CODE_CHANNEL_NOT_EXIST:      codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_CHANNEL_FENCED:         codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_ON_SHUTDOWN:            codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_INVALID_REQUEST_SEQ:    codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_UNMATCHED_CHANNEL_TERM: codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_IGNORED_OPERATION:      codes.FailedPrecondition,
	logpb.LogCode_LOG_CODE_INNER:                  codes.Unavailable,
	logpb.LogCode_LOG_CODE_UNKNOWN:                codes.Unknown,
}

// NewGRPCStatusFromLogError converts LogError to grpc status.
func NewGRPCStatusFromLogError(e *LogError) *status.Status {
	if e == nil || e.Code == logpb.LogCode_LOG_CODE_OK {
		return status.New(codes.OK, "")
	}

	code, ok := logErrorToGRPCStatus[e.Code]
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
