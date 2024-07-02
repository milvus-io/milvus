package status

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Check if the error is canceled.
// Used in client side.
func IsCanceled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}

	if se, ok := err.(interface {
		GRPCStatus() *status.Status
	}); ok {
		switch se.GRPCStatus().Code() {
		case codes.Canceled, codes.DeadlineExceeded:
			return true
			// It may be a special unavailable error, but we don't enable here.
			// From etcd implementation:
			// case codes.Unavailable:
			// 	msg := se.GRPCStatus().Message()
			// 	// client-side context cancel or deadline exceeded with TLS ("http2.errClientDisconnected")
			// 	// "rpc error: code = Unavailable desc = client disconnected"
			// 	if msg == "client disconnected" {
			// 		return true
			// 	}
			// 	// "grpc/transport.ClientTransport.CloseStream" on canceled streams
			// 	// "rpc error: code = Unavailable desc = stream error: stream ID 21; CANCEL")
			// 	if strings.HasPrefix(msg, "stream error: ") && strings.HasSuffix(msg, "; CANCEL") {
			// 		return true
			// 	}
		}
	}
	return false
}
