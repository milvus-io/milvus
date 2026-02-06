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
	fieldKeys map[string]*Field // key -> Field pointer for deduplication
	logger    *zap.Logger       // cached logger with fields applied
}

// WithFields attaches fields to context. Fields accumulate across calls.
// Duplicate keys are deduplicated, with later values overriding earlier ones.
// Fields created with PropagatedString/PropagatedInt64 will be propagated via RPC.
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

// withFields adds fields to logContext, handling deduplication and logger caching.
func (lc *logContext) withFields(ctx context.Context, fields []Field) context.Context {
	// Build new fieldKeys map and detect duplicates in one pass
	newFieldKeys := make(map[string]*Field, len(lc.fieldKeys)+len(fields))
	for k, v := range lc.fieldKeys {
		newFieldKeys[k] = v
	}

	hasDuplicate := false
	for i := range fields {
		if _, exists := newFieldKeys[fields[i].Key]; exists {
			hasDuplicate = true
		}
		newFieldKeys[fields[i].Key] = &fields[i]
	}

	var newLogger *zap.Logger
	if hasDuplicate {
		// Rebuild logger from global with all deduplicated fields
		allFields := make([]Field, 0, len(newFieldKeys))
		for _, f := range newFieldKeys {
			allFields = append(allFields, *f)
		}
		newLogger = getLogger().WithLazy(allFields...)
	} else {
		// Fast path: use cached logger with lazy field evaluation
		if lc.logger != nil {
			newLogger = lc.logger.WithLazy(fields...)
		} else {
			newLogger = getLogger().WithLazy(fields...)
		}
	}

	return context.WithValue(ctx, fieldsKey, &logContext{
		fieldKeys: newFieldKeys,
		logger:    newLogger,
	})
}

// GetPropagated returns propagated fields for gRPC transmission.
// Only fields created with PropagatedString/PropagatedInt64 are included.
// Values are serialized as strings.
func GetPropagated(ctx context.Context) map[string]string {
	lc := getLogContext(ctx)
	if len(lc.fieldKeys) == 0 {
		return nil
	}

	var result map[string]string
	for key, f := range lc.fieldKeys {
		if isPropagatedField(f) {
			if result == nil {
				result = make(map[string]string)
			}
			result[key] = getPropagatedValue(f)
		}
	}
	return result
}

func getLogContext(ctx context.Context) *logContext {
	if ctx == nil {
		return &logContext{}
	}
	if lc, ok := ctx.Value(fieldsKey).(*logContext); ok && lc != nil {
		return lc
	}
	return &logContext{}
}

// FieldsFromContext extracts fields from context as a slice.
func FieldsFromContext(ctx context.Context) []Field {
	lc := getLogContext(ctx)
	if len(lc.fieldKeys) == 0 {
		return nil
	}
	fields := make([]Field, 0, len(lc.fieldKeys))
	for _, f := range lc.fieldKeys {
		fields = append(fields, *f)
	}
	return fields
}

// loggerFromContext returns the cached logger from context.
// If no cached logger exists, returns nil.
func loggerFromContext(ctx context.Context) *zap.Logger {
	return getLogContext(ctx).logger
}

// fieldCount returns the number of fields in the logContext.
func (lc *logContext) fieldCount() int {
	return len(lc.fieldKeys)
}

// getFields returns all fields as a slice.
func (lc *logContext) getFields() []Field {
	if len(lc.fieldKeys) == 0 {
		return nil
	}
	fields := make([]Field, 0, len(lc.fieldKeys))
	for _, f := range lc.fieldKeys {
		fields = append(fields, *f)
	}
	return fields
}
