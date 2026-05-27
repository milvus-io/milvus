package mlog

import (
	"context"

	"go.uber.org/zap"
)

// contextKey is unexported to prevent collisions
type contextKey struct{}

var fieldsKey = contextKey{}

// logContext holds all mlog-related data in context
type logContext struct {
	fields []Field     // ordered fields attached to the context
	logger *zap.Logger // cached logger with fields applied
}

// WithFields attaches fields to context. Fields accumulate across calls.
// Duplicate keys are preserved in insertion order.
// Fields created with OptPropagated() will be propagated via RPC.
func WithFields(ctx context.Context, fields ...Field) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(fields) == 0 {
		return ctx
	}

	lc := getLogContext(ctx)
	return lc.withFields(ctx, fields)
}

// withFields adds fields to logContext, preserving insertion order and logger caching.
func (lc *logContext) withFields(ctx context.Context, fields []Field) context.Context {
	newFields := make([]Field, len(lc.fields)+len(fields))
	copy(newFields, lc.fields)
	copy(newFields[len(lc.fields):], fields)

	var newLogger *zap.Logger
	if lc.logger != nil {
		newLogger = lc.logger.WithLazy(fields...)
	} else {
		newLogger = getLogger().WithLazy(newFields...)
	}

	return context.WithValue(ctx, fieldsKey, &logContext{
		fields: newFields,
		logger: newLogger,
	})
}

// GetPropagated returns propagated fields for gRPC transmission.
// Only fields created with OptPropagated() are included.
// Values are serialized as strings.
func GetPropagated(ctx context.Context) map[string]string {
	lc := getLogContext(ctx)
	if len(lc.fields) == 0 {
		return nil
	}

	var result map[string]string
	for i := range lc.fields {
		f := &lc.fields[i]
		if isPropagatedField(f) {
			if result == nil {
				result = make(map[string]string)
			}
			result[f.Key] = getPropagatedValue(f)
		}
	}
	return result
}

// emptyLogContext is a shared sentinel to avoid allocating an empty logContext on every call.
var emptyLogContext = &logContext{}

func getLogContext(ctx context.Context) *logContext {
	if ctx == nil {
		return emptyLogContext
	}
	if lc, ok := ctx.Value(fieldsKey).(*logContext); ok && lc != nil {
		return lc
	}
	return emptyLogContext
}

// FieldsFromContext extracts fields from context as a slice.
func FieldsFromContext(ctx context.Context) []Field {
	return getLogContext(ctx).getFields()
}

// fieldCount returns the number of fields in the logContext.
func (lc *logContext) fieldCount() int {
	return len(lc.fields)
}

// getFields returns all fields as a slice.
func (lc *logContext) getFields() []Field {
	if len(lc.fields) == 0 {
		return nil
	}
	fields := make([]Field, len(lc.fields))
	copy(fields, lc.fields)
	return fields
}
