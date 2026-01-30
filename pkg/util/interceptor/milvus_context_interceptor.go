package interceptor

import (
	"context"
	"crypto/rand"
	"strconv"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func NewMilvusContextUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, mctx := mcontext.NewMilvusContext(ctx)
		if err := validateMilvusContext(mctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func NewMilvusContextStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		ctx, mctx := mcontext.NewMilvusContext(ctx)
		if err := validateMilvusContext(mctx); err != nil {
			return err
		}
		wrappedStream := grpc_middleware.WrapServerStream(ss)
		wrappedStream.WrappedContext = ctx
		return handler(srv, wrappedStream)
	}
}

func NewMilvusContextUnaryClientInterceptor(dstNodeID int64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = mcontext.AppendOutgoingContext(ctx)
		if dstNodeID != 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, mcontext.DestinationServerIDKey, strconv.FormatInt(dstNodeID, 10))
		}
		ctx = withRandomTraceIDIfNotStart(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NewMilvusContextStreamClientInterceptor(dstNodeID int64) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = mcontext.AppendOutgoingContext(ctx)
		if dstNodeID != 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, mcontext.DestinationServerIDKey, strconv.FormatInt(dstNodeID, 10))
		}
		ctx = withRandomTraceIDIfNotStart(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// validateMilvusContext validates the MilvusContext.
func validateMilvusContext(mctx *mcontext.MilvusContext) error {
	// Cluster validation.
	clusterPrefix := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	if mctx.DestinationCluster != "" && mctx.DestinationCluster != clusterPrefix {
		return merr.WrapErrServiceCrossClusterRouting(clusterPrefix, mctx.DestinationCluster)
	}

	// Server ID validation.
	nodeID := paramtable.GetNodeID()
	if mctx.DestinationNodeID != 0 && mctx.DestinationNodeID != nodeID {
		return merr.WrapErrNodeNotMatch(nodeID, mctx.DestinationNodeID)
	}
	return nil
}

// withRandomTraceIDIfNotStart adds a random trace ID to the context if the trace ID is not set.
func withRandomTraceIDIfNotStart(ctx context.Context) context.Context {
	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx
	}

	var traceID trace.TraceID
	_, _ = rand.Read(traceID[:])

	var spanID trace.SpanID
	_, _ = rand.Read(spanID[:])

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}
