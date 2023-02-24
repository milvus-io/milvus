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
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

func Init() {
	params := paramtable.Get()

	var exp sdk.SpanExporter
	var err error
	switch params.TraceCfg.Exporter.GetValue() {
	case "jaeger":
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(
			jaeger.WithEndpoint(params.TraceCfg.JaegerURL.GetValue())))
	case "stdout":
		exp, err = stdout.New()
	default:
		err = errors.New("Empty Trace")
	}
	if err != nil {
		log.Warn("Init tracer faield", zap.Error(err))
		return
	}
	tp := sdk.NewTracerProvider(
		sdk.WithBatcher(exp),
		sdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(paramtable.GetRole()),
			attribute.Int64("NodeID", paramtable.GetNodeID()),
		)),
		sdk.WithSampler(sdk.ParentBased(
			sdk.TraceIDRatioBased(params.TraceCfg.SampleFraction.GetAsFloat()),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	log.Info("Init tracer finished", zap.String("Exporter", params.TraceCfg.Exporter.GetValue()))
}
