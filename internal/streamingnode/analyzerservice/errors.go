package analyzerservice

import (
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PublicError translates only streaming/transport failures at the Proxy boundary.
// Analyzer statuses bypass this adapter and are forwarded unchanged.
func PublicError(err error) error {
	if err == nil || status.IsCanceled(err) {
		return err
	}
	se := status.AsStreamingError(err)
	if se.IsInvalidArgument() {
		return merr.WrapErrParameterInvalidErr(err, "run analyzer")
	}
	if grpcstatus.Code(err) == codes.Unimplemented {
		return merr.Wrap(merr.ErrServiceUnimplemented, err.Error())
	}
	return merr.WrapErrServiceUnavailableErr(err, "run analyzer")
}

func Retryable(err error) bool {
	if err == nil || status.IsCanceled(err) {
		return false
	}
	se := status.AsStreamingError(err)
	return se.IsWrongStreamingNode() || se.IsOnShutdown() || se.Code == streamingpb.StreamingCode_STREAMING_CODE_INNER || grpcstatus.Code(err) == codes.Unavailable
}
