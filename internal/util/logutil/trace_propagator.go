package logutil

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

type grpcTracePropagator struct{}

var (
	propgateKeys = []string{}
)

var _ propagation.TextMapPropagator = grpcTracePropagator{}

func (p grpcTracePropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return
	}
	for _, key := range propgateKeys {
		vals := md.Get(key)
		if len(vals) > 0 {
			carrier.Set(key, vals[0])
		}
	}
}

func (p grpcTracePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	for _, key := range propgateKeys {
		val := carrier.Get(key)
		if len(val) > 0 {
			md.Set(key, val)
		}
	}
	return metadata.NewIncomingContext(ctx, md)
}

func (p grpcTracePropagator) Fields() []string {
	return propgateKeys
}

func NewTraceVariablePropagator() propagation.TextMapPropagator {
	return grpcTracePropagator{}
}
