package trace

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestTracer(t *testing.T) {
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
	parent(context.TODO())

	assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
	assert.Equal(t, 4, len(exp.spans))
	assert.NotNil(t, exp.spans["Parent"])
	assert.NotNil(t, exp.spans["Child0"])
	assert.NotNil(t, exp.spans["Child1"])

	assert.Equal(t, 2, exp.spans["Parent"].ChildSpanCount())
	traceID := exp.spans["Parent"].SpanContext().TraceID()
	assert.Equal(t, traceID, exp.spans["Child0"].SpanContext().TraceID())
	assert.Equal(t, traceID, exp.spans["Child1"].SpanContext().TraceID())
	testAttr := func(span trace.ReadOnlySpan, attr2test attribute.KeyValue) bool {
		for _, attr := range span.Attributes() {
			if reflect.DeepEqual(attr, attr2test) {
				return true
			}
		}
		return false
	}
	// test attributes
	assert.True(t, testAttr(exp.spans["Parent"], attribute.Int("ChildNum", 2)), "Attributes: %#v", exp.spans["Parent"].Attributes())
	assert.True(t, testAttr(
		exp.spans["Parent"], attribute.String("function", "github.com/milvus-io/milvus/internal/util/trace.parent")),
		exp.spans["Parent"].Attributes())
	assert.True(t, testAttr(
		exp.spans["Parent"], attribute.String("location", "otel_trace_test.go:154")),
		exp.spans["Parent"].Attributes())

	assert.True(t, testAttr(
		exp.spans["Child0"],
		attribute.String("function", "github.com/milvus-io/milvus/internal/util/trace.child0")),
		"Attributes: %#v", exp.spans["Child0"].Attributes())
	assert.False(t, testAttr(exp.spans["Child0"],
		attribute.String("function", "github.com/milvus-io/milvus/internal/util/trace.parent")),
		"Attibutes: %#v", exp.spans["Child0"].Attributes())
	assert.True(t, testAttr(
		exp.spans["Child0"],
		attribute.String("location", "otel_trace_test.go:164")),
		"Attributes: %#v", exp.spans["Child0"].Attributes())

	_, err = NewGlobalTracerProvider(
		WithServiceName("empty_exporter_tracer"),
	)
	assert.Error(t, err)
}

func TestTraceSampler(t *testing.T) {
	t.Run("fraction-sampler", func(t *testing.T) {
		exp := &cntExporter{cnt: make(map[string]int)}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("tracer-test-service"),
			WithExporter(exp),
			WithSampleFraction(0.5),
		)
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()
		for i := 0; i < 1000; i++ {
			parent(context.TODO())
		}
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		parentCnt, child0Cnt, child1Cnt := exp.cnt["Parent"], exp.cnt["Child0"], exp.cnt["Child1"]
		assert.True(t, parentCnt > 400 && parentCnt < 600, "parentCnt: %d", parentCnt)
		assert.True(t, child0Cnt > 400 && child0Cnt < 600, "child0Cnt: %d", child0Cnt)
		assert.True(t, child1Cnt > 400 && child1Cnt < 600, "child1Cnt: %d", child1Cnt)
		assert.True(t, parentCnt == child0Cnt && child0Cnt == child1Cnt, "Parent: %d, Child0: %d, Child1: %d", parentCnt, child0Cnt, child1Cnt)
	})

	t.Run("custom-sampler drop child1", func(t *testing.T) {
		// if parent span is sampled, so does the child.
		exp := &cntExporter{cnt: make(map[string]int)}
		dropChild1Sampler := &mockSampler{dropNames: []string{"Child1"}}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("drop-child1"),
			WithExporter(exp),
			WithSampler(dropChild1Sampler),
		)
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()

		for i := 0; i < 1000; i++ {
			parent(context.TODO())
		}
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		parentCnt, child0Cnt, child1Cnt, recursiveCnt := exp.cnt["Parent"], exp.cnt["Child0"], exp.cnt["Child1"], exp.cnt["RecursiveChild"]
		assert.True(t, parentCnt == 1000, "parentCnt: %d", parentCnt)
		assert.True(t, child0Cnt == 1000, "child0Cnt: %d", child0Cnt)
		assert.True(t, child1Cnt == 1000, "child1Cnt: %d", child1Cnt)
		assert.True(t, recursiveCnt == 1000, "RecursiveChild: %d", recursiveCnt)
	})

	t.Run("custom-sampler drop parent", func(t *testing.T) {
		exp := &cntExporter{cnt: make(map[string]int)}
		dropChild0Sampler := &mockSampler{dropNames: []string{"Parent"}}
		tracer, err := NewGlobalTracerProvider(
			WithServiceName("drop-child0"),
			WithExporter(exp),
			WithSampler(dropChild0Sampler),
		)
		assert.Nil(t, err)
		defer func() {
			if tracer != nil {
				assert.Nil(t, tracer.Close())
			}
		}()

		for i := 0; i < 1000; i++ {
			parent(context.TODO())
		}
		assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
		parentCnt, child0Cnt, child1Cnt := exp.cnt["Parent"], exp.cnt["Child0"], exp.cnt["Child1"]
		assert.True(t, parentCnt == 0, "parentCnt: %d", parentCnt)
		assert.True(t, child0Cnt == 0, "child0Cnt: %d", child0Cnt)
		assert.True(t, child1Cnt == 0, "child1Cnt: %d", child1Cnt)
	})
}

func parent(ctx context.Context) {
	pctx, span := StartSpanFromContextWithOperationName(ctx, "Parent")
	defer span.End()

	span.AddEvent("Parent Execution")
	span.SetAttributes(attribute.Int("ChildNum", 2))
	child0(pctx)
	child1(pctx)
}

func child0(ctx context.Context) {
	_, span := StartSpanFromContextWithOperationName(ctx, "Child0")
	defer span.End()

	span.AddEvent("Child0 Execution")
	span.SetAttributes(attribute.Int("ChildNum", 0))
}

func child1(ctx context.Context) {
	newctx, span := StartSpanFromContextWithOperationName(ctx, "Child1")
	defer span.End()

	span.AddEvent("Child1 Execution")
	span.SetAttributes(attribute.Int("ChildNum", 0))
	recurChild(newctx)
}

func recurChild(ctx context.Context) {
	_, span := StartSpanFromContextWithOperationName(ctx, "RecursiveChild")
	defer span.End()

	span.AddEvent("Child1 Execution")
}

type mockExporter struct {
	spans map[string]trace.ReadOnlySpan
}

func (exp *mockExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	for i, span := range spans {
		exp.spans[span.Name()] = spans[i]
	}
	return nil
}

func (exp *mockExporter) Shutdown(ctx context.Context) error {
	return nil
}

type cntExporter struct {
	cnt map[string]int
}

func (exp *cntExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	for _, span := range spans {
		exp.cnt[span.Name()] = exp.cnt[span.Name()] + 1
	}
	return nil
}

func (exp *cntExporter) Shutdown(context.Context) error {
	return nil
}

type mockSampler struct {
	dropNames []string
}

func (s *mockSampler) ShouldSample(parameters trace.SamplingParameters) trace.SamplingResult {
	drop := false
	for _, name := range s.dropNames {
		if name == parameters.Name {
			drop = true
		}
	}
	decision := trace.RecordAndSample
	if drop {
		decision = trace.Drop
	}
	spanCtx := oteltrace.SpanContextFromContext(parameters.ParentContext)
	return trace.SamplingResult{
		Decision:   decision,
		Tracestate: spanCtx.TraceState(),
	}
}

func (s *mockSampler) Description() string {
	return "mock-sampler"
}

func preEnd(ctx context.Context) {
	newctx, span := StartSpanFromContextWithOperationName(ctx, "PreEndParent")
	span.End()

	child0(newctx)
}

func TestPreEnd(t *testing.T) {
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
	preEnd(context.TODO())
	assert.Nil(t, tracer.tp.ForceFlush(context.TODO()))
	for key, v := range exp.spans {
		fmt.Printf("%s: %#v", key, v)
	}
}
