package trace

import (
	"context"
	"io"
	"runtime"
	"strings"

	"errors"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

func InitTracing(serviceName string) (opentracing.Tracer, io.Closer, error) {
	if true {
		cfg, err := config.FromEnv()
		if err != nil {
			return nil, nil, errors.New("trace from env error")
		}
		cfg.ServiceName = serviceName
		return cfg.NewTracer()
	}
	cfg := &config.Configuration{
		ServiceName: serviceName,
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	return cfg.NewTracer()
}

func StartSpanFromContext(ctx context.Context, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		return noopSpan(), ctx
	}

	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, "unknown", opts...)
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return span, ctx
	}
	fn := runtime.FuncForPC(pcs[0])
	name := fn.Name()
	if lastSlash := strings.LastIndexByte(name, '/'); lastSlash > 0 {
		name = name[lastSlash+1:]
	}

	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		opts = append(opts, opentracing.ChildOf(parent.Context()))
	}
	span := opentracing.StartSpan(name, opts...)

	file, line := fn.FileLine(pcs[0])
	span.LogFields(log.String("filename", file), log.Int("line", line))

	return span, opentracing.ContextWithSpan(ctx, span)
}

func StartSpanFromContextWithOperationName(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		return noopSpan(), ctx
	}

	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return span, ctx
	}
	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])

	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span := opentracing.StartSpan(operationName, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)

	span.LogFields(log.String("filename", file), log.Int("line", line))

	return span, ctx
}

func LogError(span opentracing.Span, err error) error {
	if err == nil {
		return nil
	}

	// Get caller frame.
	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span.LogFields(log.Error(err))
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return err
	}

	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])
	span.LogFields(log.String("filename", file), log.Int("line", line), log.Error(err))

	return err
}

func InfoFromSpan(span opentracing.Span) (traceID string, sampled bool, found bool) {
	if spanContext, ok := span.Context().(jaeger.SpanContext); ok {
		traceID = spanContext.TraceID().String()
		sampled = spanContext.IsSampled()
		return traceID, sampled, true
	}
	return "", false, false
}

func InfoFromContext(ctx context.Context) (traceID string, sampled bool, found bool) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		return InfoFromSpan(span)
	}
	return "", false, false
}

func InjectContextToPulsarMsgProperties(sc opentracing.SpanContext, properties map[string]string) {
	tracer := opentracing.GlobalTracer()
	tracer.Inject(sc, opentracing.TextMap, propertiesReaderWriter{properties})
}

func ExtractFromPulsarMsgProperties(msg msgstream.TsMsg, properties map[string]string) (opentracing.Span, bool) {
	if !allowTrace(msg) {
		return noopSpan(), false
	}
	tracer := opentracing.GlobalTracer()
	sc, _ := tracer.Extract(opentracing.TextMap, propertiesReaderWriter{properties})
	name := "receive pulsar msg"
	opts := []opentracing.StartSpanOption{
		ext.RPCServerOption(sc),
		opentracing.Tags{
			"ID":       msg.ID(),
			"Type":     msg.Type(),
			"HashKeys": msg.HashKeys(),
			"Position": msg.Position(),
		}}
	return opentracing.StartSpan(name, opts...), true
}

func MsgSpanFromCtx(ctx context.Context, msg msgstream.TsMsg, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		return noopSpan(), ctx
	}
	if !allowTrace(msg) {
		return noopSpan(), ctx
	}
	operationName := "send pulsar msg"
	opts = append(opts, opentracing.Tags{
		"ID":       msg.ID(),
		"Type":     msg.Type(),
		"HashKeys": msg.HashKeys(),
		"Position": msg.Position(),
	})

	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return span, ctx
	}
	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])

	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span := opentracing.StartSpan(operationName, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)

	span.LogFields(log.String("filename", file), log.Int("line", line))

	return span, ctx
}

type propertiesReaderWriter struct {
	ppMap map[string]string
}

func (ppRW propertiesReaderWriter) Set(key, val string) {
	key = strings.ToLower(key)
	ppRW.ppMap[key] = val
}

func (ppRW propertiesReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, val := range ppRW.ppMap {
		if err := handler(k, val); err != nil {
			return err
		}
	}
	return nil
}

func allowTrace(in interface{}) bool {
	if in == nil {
		return false
	}
	switch res := in.(type) {
	case msgstream.TsMsg:
		return !(res.Type() == commonpb.MsgType_TimeTick ||
			res.Type() == commonpb.MsgType_QueryNodeStats ||
			res.Type() == commonpb.MsgType_LoadIndex)
	default:
		return false
	}
}

func noopSpan() opentracing.Span {
	return opentracing.NoopTracer{}.StartSpan("Default-span")
}
