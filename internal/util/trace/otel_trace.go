package trace

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/milvus-io/milvus/internal/util/logutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	oteltrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	globalTracer     = "milvus.io/global-tracer"
	globalNoopTracer = "milvus.io/noop-tracer"
)

type traceConfig struct {
	// resource configs
	serviceName string

	// exporter configs
	filename       string
	jaegerEndpoint string
	sampleFrac     float64

	sampler     oteltrace.Sampler
	exporter    oteltrace.SpanExporter
	development bool
	host        string
}

type Tracer struct {
	fh *os.File
	tp *oteltrace.TracerProvider
}

type Option interface {
	apply(*traceConfig) *traceConfig
}

type optionApplyFunc func(*traceConfig) *traceConfig

func (o optionApplyFunc) apply(cfg *traceConfig) *traceConfig {
	return o(cfg)
}

func WithServiceName(svc string) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.serviceName = svc
		return cfg
	})
}

func WithExportFile(filename string) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.filename = filename
		return cfg
	})
}

func WithJaegerEndpoint(url string) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.jaegerEndpoint = url
		return cfg
	})
}

func WithHostName(host string) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.host = host
		return cfg
	})
}

func WithSampleFraction(fraction float64) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		if fraction < 0.0 {
			fraction = 0.0
		} else if fraction > 1.0 {
			fraction = 1.0
		}
		cfg.sampleFrac = fraction
		return cfg
	})
}

func WithExporter(exp oteltrace.SpanExporter) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.exporter = exp
		return cfg
	})
}

func WithSampler(sampler oteltrace.Sampler) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.sampler = sampler
		return cfg
	})
}

func WithDevelopmentFlag(dev bool) Option {
	return optionApplyFunc(func(cfg *traceConfig) *traceConfig {
		cfg.development = dev
		return cfg
	})
}

func NewGlobalTracerProvider(opts ...Option) (*Tracer, error) {
	cfg := &traceConfig{}
	for _, opt := range opts {
		cfg = opt.apply(cfg)
	}
	logutil.Logger(context.TODO()).Debug("global trace configs", zap.String("serviceName", cfg.serviceName),
		zap.String("output", cfg.filename), zap.String("jaegerEndpoint", cfg.jaegerEndpoint),
		zap.Float64("sampleFraction", cfg.sampleFrac), zap.Bool("development", cfg.development))
	resource, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.HostNameKey.String(cfg.host),
			semconv.ServiceNameKey.String(cfg.serviceName)))
	if err != nil {
		return nil, err
	}

	var exp oteltrace.SpanExporter
	var fh *os.File
	if cfg.exporter != nil {
		exp = cfg.exporter
	} else if cfg.jaegerEndpoint != "" {
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.jaegerEndpoint)))
		if err != nil {
			return nil, err
		}
	} else if cfg.filename != "" {
		if cfg.filename == "stdout" {
			exp, err = stdouttrace.New(
				stdouttrace.WithPrettyPrint(),
				stdouttrace.WithWriter(os.Stdout),
			)
			if err != nil {
				return nil, err
			}
		} else {
			fh, err = os.Create(cfg.filename)
			if err != nil {
				return nil, err
			}
			exp, err = stdouttrace.New(
				stdouttrace.WithPrettyPrint(),
				stdouttrace.WithWriter(fh),
			)
			if err != nil {
				fh.Close()
				return nil, err
			}
		}
	}
	if exp == nil {
		return nil, fmt.Errorf("init exporter failed")
	}

	var sampler oteltrace.Sampler
	if cfg.sampler != nil {
		sampler = cfg.sampler
	} else if cfg.development {
		sampler = oteltrace.AlwaysSample()
	} else if cfg.sampleFrac >= 0 && cfg.sampleFrac <= 1 {
		sampler = oteltrace.TraceIDRatioBased(cfg.sampleFrac)
	}

	tp := oteltrace.NewTracerProvider(
		oteltrace.WithBatcher(exp),
		oteltrace.WithResource(resource),
		oteltrace.WithSampler(oteltrace.ParentBased(sampler)),
	)
	// set trace provider
	otel.SetTracerProvider(tp)
	// set propagator to propagate trace between wires
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
			logutil.NewTraceVariablePropagator(),
		),
	)
	return &Tracer{
		fh: fh,
		tp: tp,
	}, nil
}

func (t *Tracer) Close() error {
	var closeErr, shutDownErr error
	if t.tp != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()
		shutDownErr = t.tp.Shutdown(ctx)
	}
	if t.fh != nil {
		closeErr = t.fh.Close()
	}
	if closeErr != nil {
		return closeErr
	}
	if shutDownErr != nil {
		return shutDownErr
	}
	return nil
}

func DefaultTracer() trace.Tracer {
	return otel.Tracer(globalTracer)
}
