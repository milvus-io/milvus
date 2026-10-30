package interceptor

import (
	"context"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewServiceUnaryClientInterceptor converts internal service errors.
// An optional QN/SN role enables request context from the actual gRPC pick.
func NewServiceUnaryClientInterceptor(nodeRole ...string) grpc.UnaryClientInterceptor {
	role := ""
	if len(nodeRole) > 0 {
		role = nodeRole[0]
	}
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = contextutil.EnsurePickServerID(ctx)
		err := invoker(ctx, method, req, reply, cc, opts...)
		return finishRPC(ctx, method, err, role)
	}
}

// NewServiceStreamClientInterceptor also handles errors returned after
// stream creation; each stream retains only its own pick recorder.
func NewServiceStreamClientInterceptor(nodeRole ...string) grpc.StreamClientInterceptor {
	role := ""
	if len(nodeRole) > 0 {
		role = nodeRole[0]
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = contextutil.EnsurePickServerID(ctx)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return stream, finishRPC(ctx, method, err, role)
		}
		return &serviceClientStream{ClientStream: stream, method: method, nodeRole: role, ctx: ctx}, nil
	}
}

func finishRPC(ctx context.Context, method string, err error, nodeRole string) error {
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	switch {
	case strings.HasPrefix(method, streamingpb.ServiceMethodPrefix):
		err = status.ConvertStreamingError(method, err)
	case strings.HasPrefix(method, viewpb.ServiceMethodPrefix):
		err = viewerror.ConvertViewError(method, err)
	default:
		return err
	}
	serverID := contextutil.GetPickedServerID(ctx)
	if serverID == contextutil.UnspecifiedServerID {
		return err
	}
	info := &viewpb.RPCRequestContext{Method: method}
	switch nodeRole {
	case typeutil.QueryNodeRole:
		info.Target = &viewpb.RPCRequestContext_QueryNode{QueryNode: &viewpb.QueryNodeRequestContext{NodeId: serverID}}
	case typeutil.StreamingNodeRole:
		info.Target = &viewpb.RPCRequestContext_StreamingNode{StreamingNode: &viewpb.StreamingNodeRequestContext{NodeId: serverID}}
	default:
		return err
	}
	return viewerror.AttachRPCContext(err, info)
}

type serviceClientStream struct {
	grpc.ClientStream
	method   string
	nodeRole string
	ctx      context.Context
}

func (s *serviceClientStream) Header() (metadata.MD, error) {
	md, err := s.ClientStream.Header()
	return md, finishRPC(s.ctx, s.method, err, s.nodeRole)
}

func (s *serviceClientStream) SendMsg(msg interface{}) error {
	return finishRPC(s.ctx, s.method, s.ClientStream.SendMsg(msg), s.nodeRole)
}

func (s *serviceClientStream) RecvMsg(msg interface{}) error {
	return finishRPC(s.ctx, s.method, s.ClientStream.RecvMsg(msg), s.nodeRole)
}

func (s *serviceClientStream) CloseSend() error {
	return finishRPC(s.ctx, s.method, s.ClientStream.CloseSend(), s.nodeRole)
}
