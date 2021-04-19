package trace

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func InitTracing(serviceName string) io.Closer {
	if opentracing.IsGlobalTracerRegistered() {
		return nil
	}
	var cfg *config.Configuration
	var err error
	if true {
		cfg, err = config.FromEnv()
		if err != nil {
			log.Error(err)
			return nil
		}
		cfg.ServiceName = serviceName
	} else {
		cfg = &config.Configuration{
			ServiceName: serviceName,
			Sampler: &config.SamplerConfig{
				Type:  "const",
				Param: 1,
			},
		}
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		log.Error(err)
		return nil
	}
	opentracing.SetGlobalTracer(tracer)
	return closer
}

func StartSpanFromContext(ctx context.Context, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		return NoopSpan(), ctx
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
		return NoopSpan(), ctx
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
	tracer.Inject(sc, opentracing.TextMap, PropertiesReaderWriter{properties})
}

type PropertiesReaderWriter struct {
	PpMap map[string]string
}

func (ppRW PropertiesReaderWriter) Set(key, val string) {
	key = strings.ToLower(key)
	ppRW.PpMap[key] = val
}

func (ppRW PropertiesReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, val := range ppRW.PpMap {
		if err := handler(k, val); err != nil {
			return err
		}
	}
	return nil
}

func NoopSpan() opentracing.Span {
	return opentracing.NoopTracer{}.StartSpan("Default-span")
}
