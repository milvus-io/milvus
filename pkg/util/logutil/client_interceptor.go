package logutil

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/log"
)

func UnaryClientTraceInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	newCtx := wrapFields(ctx)
	err := invoker(newCtx, method, req, reply, cc, opts...)
	return err
}

func StreamClientTraceInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	newCtx := wrapFields(ctx)
	stream, err := streamer(newCtx, desc, cc, method, opts...)
	return stream, err
}

func wrapFields(ctx context.Context) context.Context {
	md := metadata.New(make(map[string]string))
	if ctxLogger, ok := ctx.Value(log.CtxLogKey).(*log.MLogger); ok {
		for k, v := range ctxLogger.Fields {
			md.Append(k, fmt.Sprintf("%v", v))
		}
	}
	newCtx := metadata.NewOutgoingContext(ctx, md)
	return newCtx
}
