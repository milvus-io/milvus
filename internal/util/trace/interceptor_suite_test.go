package trace

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type mockServerStream struct {
	ctx context.Context
}

func (ss *mockServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (ss *mockServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (ss *mockServerStream) SetTrailer(metadata.MD) {}

func (ss *mockServerStream) Context() context.Context {
	return ss.ctx
}

func (ss *mockServerStream) SendMsg(interface{}) error {
	return nil
}

func (ss *mockServerStream) RecvMsg(interface{}) error {
	return nil
}

func TestTraceInterceptor(t *testing.T) {

	md := metadata.New(map[string]string{
		"log_level":         "error",
		"client_request_id": "test-request-id",
	})
	props, err := baggage.NewKeyValueProperty("prop-key", "prop-val")
	assert.Nil(t, err)
	member, err := baggage.NewMember("mem-key", "mem-val", props)
	assert.Nil(t, err)
	bags, err := baggage.New(member)
	assert.Nil(t, err)
	ctx := baggage.ContextWithBaggage(context.TODO(), bags)
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx = peer.NewContext(ctx, &peer.Peer{
		Addr: &net.TCPAddr{
			IP:   net.ParseIP("192.168.10.1"),
			Port: 12345,
		},
	})
	t.Run("unary interceptor", func(t *testing.T) {
		exp := &mockExporter{spans: make(map[string]trace.ReadOnlySpan)}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("tracer-test-service"),
			WithExporter(exp),
			WithSampleFraction(1.0),
			WithDevelopmentFlag(true))
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()

		info := &grpc.UnaryServerInfo{
			Server:     nil,
			FullMethod: "/milvus.proto.milvus.MilvusService/Search",
		}
		handler := func(ctx context.Context, _ interface{}) (interface{}, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, []string{"error"}, md.Get("log_level"))
			assert.Equal(t, []string{"test-request-id"}, md.Get("client_request_id"))
			return nil, nil
		}
		_, err = UnaryServerInterceptor()(ctx, nil, info, handler)
		assert.Nil(t, err)

		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		sp, ok := exp.spans["milvus.proto.milvus.MilvusService/Search"]
		assert.True(t, ok)
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.peer", "192.168.10.1:12345")))
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.method", "Search")))
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.service", "milvus.proto.milvus.MilvusService")))
	})

	t.Run("stream interceptor", func(t *testing.T) {
		exp := &mockExporter{spans: make(map[string]trace.ReadOnlySpan)}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("tracer-test-service"),
			WithExporter(exp),
			WithSampleFraction(1.0),
			WithDevelopmentFlag(true))
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()
		info := &grpc.StreamServerInfo{
			FullMethod:     "/milvus.proto.milvus.MilvusService/Search",
			IsClientStream: false,
			IsServerStream: true,
		}
		handler := func(_ interface{}, _ grpc.ServerStream) error {
			md, ok := metadata.FromIncomingContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, []string{"error"}, md.Get("log_level"))
			assert.Equal(t, []string{"test-request-id"}, md.Get("client_request_id"))
			return nil
		}
		ss := &mockServerStream{ctx: ctx}
		err = StreamServerInterceptor()(nil, ss, info, handler)
		assert.Nil(t, err)
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		sp, ok := exp.spans["milvus.proto.milvus.MilvusService/Search"]
		assert.True(t, ok)
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.peer", "192.168.10.1:12345")))
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.method", "Search")))
		assert.True(t, containAttributes(sp.Attributes(), attribute.String("rpc.service", "milvus.proto.milvus.MilvusService")))

	})

	t.Run("test filter func", func(t *testing.T) {
		exp := &mockExporter{spans: make(map[string]trace.ReadOnlySpan)}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("tracer-test-service"),
			WithExporter(exp),
			WithSampleFraction(1.0),
			WithDevelopmentFlag(true))
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()
		info := &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.rootcoord.RootCoord/UpdateChannelTimeTick",
		}
		handler := func(context.Context, interface{}) (interface{}, error) {
			return nil, nil
		}
		_, err = UnaryServerInterceptor()(ctx, nil, info, handler)
		assert.Nil(t, err)
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		assert.Equal(t, 0, len(exp.spans))

		ssinfo := &grpc.StreamServerInfo{
			FullMethod: "/milvus.proto.rootcoord.RootCoord/UpdateChannelTimeTick",
		}
		sshandler := func(interface{}, grpc.ServerStream) error {
			return nil
		}
		err = StreamServerInterceptor()(nil, &mockServerStream{}, ssinfo, sshandler)
		assert.Nil(t, err)
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		assert.Equal(t, 0, len(exp.spans))
	})
}

func containAttributes(attrs []attribute.KeyValue, attr attribute.KeyValue) bool {
	for _, a := range attrs {
		if a.Key == attr.Key {
			return a.Value == attr.Value
		}
	}
	return false
}
