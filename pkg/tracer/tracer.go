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
	"encoding/base64"
	"encoding/json"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

// parseHeaders parses base64-encoded JSON headers string into map[string]string
func parseHeaders(headers string) map[string]string {
	if headers == "" {
		return nil
	}

	// Try to decode as base64 first
	decodeheaders, err := base64.StdEncoding.DecodeString(headers)
	if err != nil {
		log.Warn("Failed to decode base64 headers, trying to parse as JSON directly", zap.Error(err))
		// Try to parse headers as JSON directly
		var headersMap map[string]string
		if jsonErr := json.Unmarshal([]byte(headers), &headersMap); jsonErr == nil {
			return headersMap
		}
		log.Warn("Failed to parse headers as JSON", zap.Error(err))
		return nil
	}

	// Parse decoded JSON into map[string]string
	var headersMap map[string]string
	if jsonErr := json.Unmarshal(decodeheaders, &headersMap); jsonErr == nil {
		return headersMap
	}
	log.Warn("Failed to parse decoded headers as JSON", zap.Error(err))
	return nil
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
		headers := params.TraceCfg.OtlpHeaders.GetValue()
		switch params.TraceCfg.OtlpMethod.GetValue() {
		case "", "grpc":
			opts := []otlptracegrpc.Option{
				otlptracegrpc.WithEndpoint(params.TraceCfg.OtlpEndpoint.GetValue()),
			}
			if !secure {
				opts = append(opts, otlptracegrpc.WithInsecure())
			}
			if headersMap := parseHeaders(headers); headersMap != nil {
				opts = append(opts, otlptracegrpc.WithHeaders(headersMap))
			}
			exp, err = otlptracegrpc.New(context.Background(), opts...)
		case "http":
			opts := []otlptracehttp.Option{
				otlptracehttp.WithEndpoint(params.TraceCfg.OtlpEndpoint.GetValue()),
			}
			if !secure {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
			if headersMap := parseHeaders(headers); headersMap != nil {
				opts = append(opts, otlptracehttp.WithHeaders(headersMap))
			}
			exp, err = otlptracehttp.New(context.Background(), opts...)
		default:
			return nil, errors.Newf("otlp method not supported: %s", params.TraceCfg.OtlpMethod.GetValue())
		}
	case "stdout":
		exp, err = stdout.New()
	case "noop":
		return nil, nil
	default:
		err = errors.New("Empty Trace")
	}

	return exp, err
}
