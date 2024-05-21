// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Init() error {
	params := paramtable.Get()

	exp, err := CreateTracerExporter(params)
	if err != nil {
		log.Warn("Init tracer faield", zap.Error(err))
		return err
	}

	SetTracerProvider(exp, params.TraceCfg.SampleFraction.GetAsFloat())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	log.Info("Init tracer finished", zap.String("Exporter", params.TraceCfg.Exporter.GetValue()))
	return nil
}

func CloseTracerProvider(ctx context.Context) error {
	provider, ok := otel.GetTracerProvider().(*sdk.TracerProvider)
	if ok {
		err := provider.Shutdown(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetTracerProvider(exp sdk.SpanExporter, traceIDRatio float64) {
	tp := sdk.NewTracerProvider(
		sdk.WithBatcher(exp),
		sdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(paramtable.GetRole()),
			attribute.Int64("NodeID", paramtable.GetNodeID()),
		)),
		sdk.WithSampler(sdk.ParentBased(
			sdk.TraceIDRatioBased(traceIDRatio),
		)),
	)
	otel.SetTracerProvider(tp)
}

func CreateTracerExporter(params *paramtable.ComponentParam) (sdk.SpanExporter, error) {
	var exp sdk.SpanExporter
	var err error

	switch params.TraceCfg.Exporter.GetValue() {
	case "jaeger":
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(
			jaeger.WithEndpoint(params.TraceCfg.JaegerURL.GetValue())))
	case "otlp":
		secure := params.TraceCfg.OtlpSecure.GetAsBool()
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(params.TraceCfg.OtlpEndpoint.GetValue()),
		}
		if !secure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err = otlptracegrpc.New(context.Background(), opts...)
	case "stdout":
		exp, err = stdout.New()
	case "noop":
		return nil, nil
	default:
		err = errors.New("Empty Trace")
	}

	return exp, err
}
