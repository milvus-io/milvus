//go:build test

package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/mlog"
)

func Test_extractPropagatedFromMetadata(t *testing.T) {
	// Note: gRPC metadata keys are normalized to lowercase
	md := metadata.New(map[string]string{
		MetadataPrefix + "collectionname": "my_collection",
		MetadataPrefix + "collectionid":   "12345",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractPropagated(ctx)

	props := mlog.GetPropagated(ctx)
	assert.Len(t, props, 2)
	assert.Equal(t, "my_collection", props["collectionname"])
	assert.Equal(t, "12345", props["collectionid"])
}

func Test_extractPropagatedIgnoresNonPrefixedKeys(t *testing.T) {
	md := metadata.New(map[string]string{
		MetadataPrefix + "propagated": "value",
		"other-key":                   "other-value",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractPropagated(ctx)

	props := mlog.GetPropagated(ctx)
	assert.Len(t, props, 1)
	assert.Equal(t, "value", props["propagated"])
}

func Test_extractPropagatedNoMetadata(t *testing.T) {
	ctx := context.Background()
	ctx = extractPropagated(ctx)

	props := mlog.GetPropagated(ctx)
	assert.Nil(t, props)
}

func Test_extractPropagatedEmptyMetadata(t *testing.T) {
	md := metadata.New(map[string]string{})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractPropagated(ctx)

	props := mlog.GetPropagated(ctx)
	assert.Nil(t, props)
}

func Test_injectPropagatedToMetadata(t *testing.T) {
	ctx := context.Background()
	ctx = mlog.WithFields(ctx,
		mlog.PropagatedString("collectionName", "my_collection"),
		mlog.PropagatedInt64("collectionId", 12345),
	)

	ctx = injectPropagated(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, []string{"my_collection"}, md.Get(MetadataPrefix+"collectionName"))
	assert.Equal(t, []string{"12345"}, md.Get(MetadataPrefix+"collectionId"))
}

func Test_injectPropagatedNoPropagatedFields(t *testing.T) {
	ctx := context.Background()
	ctx = injectPropagated(ctx)

	_, ok := metadata.FromOutgoingContext(ctx)
	assert.False(t, ok)
}

func Test_injectPropagatedAppendsToExistingMetadata(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "existing-key", "existing-value")
	ctx = mlog.WithFields(ctx, mlog.PropagatedString("new-key", "new-value"))

	ctx = injectPropagated(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, []string{"existing-value"}, md.Get("existing-key"))
	assert.Equal(t, []string{"new-value"}, md.Get(MetadataPrefix+"new-key"))
}

func TestRoundTripPropagation(t *testing.T) {
	// Simulate client side: create context with propagated fields
	// Note: use lowercase keys since gRPC normalizes metadata keys to lowercase
	clientCtx := context.Background()
	clientCtx = mlog.WithFields(clientCtx,
		mlog.PropagatedString("collectionname", "test_collection"),
		mlog.PropagatedInt64("collectionid", 99999),
	)

	// Client injects into outgoing metadata
	clientCtx = injectPropagated(clientCtx)

	// Simulate network transfer: copy outgoing to incoming
	outMd, _ := metadata.FromOutgoingContext(clientCtx)
	serverCtx := metadata.NewIncomingContext(context.Background(), outMd)

	// Server extracts from incoming metadata
	serverCtx = extractPropagated(serverCtx)

	// Verify propagated fields are preserved
	props := mlog.GetPropagated(serverCtx)
	assert.Len(t, props, 2)
	assert.Equal(t, "test_collection", props["collectionname"])
	assert.Equal(t, "99999", props["collectionid"])
}

func TestMetadataPrefix(t *testing.T) {
	assert.Equal(t, "mlog-", MetadataPrefix)
}

func TestUnaryServerInterceptorExtractsFields(t *testing.T) {
	interceptor := UnaryServerInterceptor("test-module")

	md := metadata.New(map[string]string{
		MetadataPrefix + "key": "value",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var handlerCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCtx = ctx
		return "response", nil
	}

	resp, err := interceptor(ctx, nil, nil, handler)
	assert.NoError(t, err)
	assert.Equal(t, "response", resp)

	props := mlog.GetPropagated(handlerCtx)
	assert.Equal(t, "value", props["key"])
}

func TestUnaryClientInterceptorInjectsFields(t *testing.T) {
	interceptor := UnaryClientInterceptor()

	ctx := context.Background()
	ctx = mlog.WithFields(ctx, mlog.PropagatedString("key", "value"))

	var invokerCtx context.Context
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokerCtx = ctx
		return nil
	}

	err := interceptor(ctx, "/test", nil, nil, nil, invoker)
	assert.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(invokerCtx)
	assert.True(t, ok)
	assert.Equal(t, []string{"value"}, md.Get(MetadataPrefix+"key"))
}

// mockServerStream is a minimal mock for grpc.ServerStream
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func TestStreamServerInterceptorExtractsFields(t *testing.T) {
	interceptor := StreamServerInterceptor("test-module")

	md := metadata.New(map[string]string{
		MetadataPrefix + "key": "value",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	mockStream := &mockServerStream{ctx: ctx}

	var handlerStream grpc.ServerStream
	handler := func(srv any, stream grpc.ServerStream) error {
		handlerStream = stream
		return nil
	}

	err := interceptor(nil, mockStream, nil, handler)
	assert.NoError(t, err)

	// Verify the wrapped stream has the extracted context
	props := mlog.GetPropagated(handlerStream.Context())
	assert.Equal(t, "value", props["key"])
}

func TestStreamServerInterceptorWrappedStreamContext(t *testing.T) {
	interceptor := StreamServerInterceptor("test-module")

	md := metadata.New(map[string]string{
		MetadataPrefix + "a": "1",
		MetadataPrefix + "b": "2",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	mockStream := &mockServerStream{ctx: ctx}

	var handlerStream grpc.ServerStream
	handler := func(srv any, stream grpc.ServerStream) error {
		handlerStream = stream
		return nil
	}

	err := interceptor(nil, mockStream, nil, handler)
	assert.NoError(t, err)

	// The handler receives a wrappedStream with the updated context
	props := mlog.GetPropagated(handlerStream.Context())
	assert.Len(t, props, 2)
	assert.Equal(t, "1", props["a"])
	assert.Equal(t, "2", props["b"])
}

func TestStreamClientInterceptorInjectsFields(t *testing.T) {
	interceptor := StreamClientInterceptor()

	ctx := context.Background()
	ctx = mlog.WithFields(ctx, mlog.PropagatedString("key", "value"))

	var streamerCtx context.Context
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		streamerCtx = ctx
		return nil, nil
	}

	_, err := interceptor(ctx, nil, nil, "/test", streamer)
	assert.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(streamerCtx)
	assert.True(t, ok)
	assert.Equal(t, []string{"value"}, md.Get(MetadataPrefix+"key"))
}

func TestRegularFieldsNotPropagated(t *testing.T) {
	ctx := context.Background()
	ctx = mlog.WithFields(ctx, mlog.String("regular", "value"))

	ctx = injectPropagated(ctx)

	// Regular fields should not be injected into metadata
	_, ok := metadata.FromOutgoingContext(ctx)
	assert.False(t, ok)
}

func Test_extractPropagatedWithTraceContext(t *testing.T) {
	// Create a span context with TraceID and SpanID
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	ctx = extractPropagated(ctx)

	// Verify TraceID and SpanID are added as fields (not propagated)
	fields := mlog.FieldsFromContext(ctx)
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", fieldMap[mlog.KeyTraceID])
	assert.Equal(t, "0102030405060708", fieldMap[mlog.KeySpanID])
}

func Test_extractPropagatedWithTraceContextAndMetadata(t *testing.T) {
	// Create span context
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	// Add gRPC metadata
	md := metadata.New(map[string]string{
		MetadataPrefix + "collectionname": "my_collection",
	})
	ctx = metadata.NewIncomingContext(ctx, md)

	ctx = extractPropagated(ctx)

	// Verify all fields are present
	fields := mlog.FieldsFromContext(ctx)
	assert.Len(t, fields, 3)

	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", fieldMap[mlog.KeyTraceID])
	assert.Equal(t, "0102030405060708", fieldMap[mlog.KeySpanID])

	// Propagated field should be accessible via GetPropagated
	props := mlog.GetPropagated(ctx)
	assert.Equal(t, "my_collection", props["collectionname"])
}

func Test_extractPropagatedNoTraceContext(t *testing.T) {
	ctx := context.Background()
	ctx = extractPropagated(ctx)

	// No fields should be added without trace context or metadata
	fields := mlog.FieldsFromContext(ctx)
	assert.Nil(t, fields)
}

func Test_extractPropagatedWithExtraFields(t *testing.T) {
	md := metadata.New(map[string]string{
		MetadataPrefix + "key": "value",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Pass extra fields to extractPropagated
	ctx = extractPropagated(ctx,
		mlog.String(mlog.KeyModule, "proxy"),
		mlog.Int64("custom", 123),
	)

	// Verify all fields are present in a single WithFields call
	fields := mlog.FieldsFromContext(ctx)
	assert.Len(t, fields, 3) // key, module, custom

	fieldMap := make(map[string]any)
	for _, f := range fields {
		if f.String != "" {
			fieldMap[f.Key] = f.String
		} else {
			fieldMap[f.Key] = f.Integer
		}
	}
	assert.Equal(t, "proxy", fieldMap[mlog.KeyModule])
	assert.Equal(t, int64(123), fieldMap["custom"])

	props := mlog.GetPropagated(ctx)
	assert.Equal(t, "value", props["key"])
}

func Test_extractPropagatedOnlyExtraFields(t *testing.T) {
	ctx := context.Background()

	// Only extra fields, no metadata or trace context
	ctx = extractPropagated(ctx, mlog.String(mlog.KeyModule, "datanode"))

	fields := mlog.FieldsFromContext(ctx)
	assert.Len(t, fields, 1)
	assert.Equal(t, "datanode", fields[0].String)
}

func TestUnaryServerInterceptorWithModule(t *testing.T) {
	interceptor := UnaryServerInterceptor("proxy")

	ctx := context.Background()
	var handlerCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCtx = ctx
		return "response", nil
	}

	resp, err := interceptor(ctx, nil, nil, handler)
	assert.NoError(t, err)
	assert.Equal(t, "response", resp)

	// Verify module field is added
	fields := mlog.FieldsFromContext(handlerCtx)
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "proxy", fieldMap[mlog.KeyModule])
}

func TestUnaryServerInterceptorModuleWithMetadata(t *testing.T) {
	interceptor := UnaryServerInterceptor("querynode")

	md := metadata.New(map[string]string{
		MetadataPrefix + "collectionname": "my_collection",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var handlerCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCtx = ctx
		return "response", nil
	}

	_, err := interceptor(ctx, nil, nil, handler)
	assert.NoError(t, err)

	// Verify both module and propagated fields are present
	fields := mlog.FieldsFromContext(handlerCtx)
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "querynode", fieldMap[mlog.KeyModule])

	props := mlog.GetPropagated(handlerCtx)
	assert.Equal(t, "my_collection", props["collectionname"])
}

func TestStreamServerInterceptorWithModule(t *testing.T) {
	interceptor := StreamServerInterceptor("datanode")

	ctx := context.Background()
	mockStream := &mockServerStream{ctx: ctx}

	var handlerStream grpc.ServerStream
	handler := func(srv any, stream grpc.ServerStream) error {
		handlerStream = stream
		return nil
	}

	err := interceptor(nil, mockStream, nil, handler)
	assert.NoError(t, err)

	// Verify module field is added
	fields := mlog.FieldsFromContext(handlerStream.Context())
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "datanode", fieldMap[mlog.KeyModule])
}

func TestStreamServerInterceptorModuleWithMetadata(t *testing.T) {
	interceptor := StreamServerInterceptor("streamingnode")

	md := metadata.New(map[string]string{
		MetadataPrefix + "key": "value",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	mockStream := &mockServerStream{ctx: ctx}

	var handlerStream grpc.ServerStream
	handler := func(srv any, stream grpc.ServerStream) error {
		handlerStream = stream
		return nil
	}

	err := interceptor(nil, mockStream, nil, handler)
	assert.NoError(t, err)

	// Verify both module and propagated fields are present
	fields := mlog.FieldsFromContext(handlerStream.Context())
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "streamingnode", fieldMap[mlog.KeyModule])

	props := mlog.GetPropagated(handlerStream.Context())
	assert.Equal(t, "value", props["key"])
}
