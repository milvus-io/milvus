package trace

import (
	"context"
	"encoding/json"

	"github.com/milvus-io/milvus/internal/util/logutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type Span struct {
	trace.Span
}

func (s *Span) RecordInt64Pairs(keys []string, values []int64) {
	if len(keys) != len(values) {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for i := range keys {
		attrs = append(attrs, attribute.Int64(keys[i], values[i]))
	}
	s.SetAttributes(attrs...)
}

func (s *Span) RecordInt(key string, val int) {
	s.SetAttributes(attribute.Int(key, val))
}

func (s *Span) RecordInt64(key string, val int64) {
	s.SetAttributes(attribute.Int64(key, val))
}

func (s *Span) RecordInts(key string, val []int) {
	s.SetAttributes(attribute.IntSlice(key, val))
}

func (s *Span) RecordInt64s(key string, val []int64) {
	s.SetAttributes(attribute.Int64Slice(key, val))
}

func (s *Span) RecordString(key, val string) {
	s.SetAttributes(attribute.String(key, val))
}

func (s *Span) RecordStrings(key string, val []string) {
	s.SetAttributes(attribute.StringSlice(key, val))
}

func (s *Span) RecordStringPairs(keys []string, vals []string) {
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for i := range keys {
		attrs = append(attrs, attribute.String(keys[i], vals[i]))
	}
	s.SetAttributes(attrs...)
}

func (s *Span) RecordBool(key string, val bool) {
	s.SetAttributes(attribute.Bool(key, val))
}

func (s *Span) RecordAnyIgnoreErr(key string, val any) {
	payload, err := json.Marshal(val)
	if err != nil {
		logutil.Logger(context.TODO()).Error("json marshal failed", zap.Error(err))
		s.SetAttributes(attribute.String(key, "{}"))
	} else {
		s.SetAttributes(attribute.String(key, string(payload)))
	}
}

func (s *Span) RecordAny(key string, val any) error {
	payload, err := json.Marshal(val)
	if err != nil {
		return err
	}
	s.SetAttributes(attribute.String(key, string(payload)))
	return nil
}
