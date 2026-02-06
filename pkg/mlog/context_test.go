//go:build test

package mlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// fieldsToMap converts a slice of Fields to a map for order-independent comparison
func fieldsToMap(fields []Field) map[string]Field {
	m := make(map[string]Field, len(fields))
	for _, f := range fields {
		m[f.Key] = f
	}
	return m
}

func TestWithFieldsBasic(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("key1", "value1"))

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1)
	fm := fieldsToMap(fields)
	assert.Equal(t, zap.String("key1", "value1"), fm["key1"])
}

func TestWithFieldsAccumulates(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("a", "1"))
	ctx = WithFields(ctx, String("b", "2"))

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 2)
	fm := fieldsToMap(fields)
	assert.Equal(t, zap.String("a", "1"), fm["a"])
	assert.Equal(t, zap.String("b", "2"), fm["b"])
}

func TestWithFieldsMultipleFieldsAtOnce(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx,
		String("a", "1"),
		Int64("b", 2),
		Bool("c", true),
	)

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 3)
	fm := fieldsToMap(fields)
	assert.Equal(t, zap.String("a", "1"), fm["a"])
	assert.Equal(t, zap.Int64("b", 2), fm["b"])
	assert.Equal(t, zap.Bool("c", true), fm["c"])
}

func TestWithFieldsChildInheritsParent(t *testing.T) {
	ctx := context.Background()
	parentCtx := WithFields(ctx, String("parent", "value"))
	childCtx := WithFields(parentCtx, String("child", "value"))

	parentFields := FieldsFromContext(parentCtx)
	assert.Len(t, parentFields, 1)

	childFields := FieldsFromContext(childCtx)
	assert.Len(t, childFields, 2)
	fm := fieldsToMap(childFields)
	assert.Equal(t, zap.String("parent", "value"), fm["parent"])
	assert.Equal(t, zap.String("child", "value"), fm["child"])
}

func TestWithFieldsNilContext(t *testing.T) {
	// Should not panic, use background context
	ctx := WithFields(nil, String("key", "value"))
	assert.NotNil(t, ctx)

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1)
	fm := fieldsToMap(fields)
	assert.Equal(t, zap.String("key", "value"), fm["key"])
}

func TestFieldsFromContextNilContext(t *testing.T) {
	fields := FieldsFromContext(nil)
	assert.Nil(t, fields)
}

func TestFieldsFromContextNoFields(t *testing.T) {
	ctx := context.Background()
	fields := FieldsFromContext(ctx)
	assert.Nil(t, fields)
}

func TestWithFieldsDoesNotMutateParent(t *testing.T) {
	ctx := context.Background()
	parentCtx := WithFields(ctx, String("a", "1"))

	// Create child and add more fields
	childCtx := WithFields(parentCtx, String("b", "2"))
	_ = WithFields(childCtx, String("c", "3"))

	// Parent should still only have one field
	parentFields := FieldsFromContext(parentCtx)
	assert.Len(t, parentFields, 1)
}

// Tests for PropagatedString/PropagatedInt64

func TestPropagatedStringField(t *testing.T) {
	f := PropagatedString("key", "value")
	assert.Equal(t, "key", f.Key)
	assert.Equal(t, zapcore.ObjectMarshalerType, f.Type)
	assert.True(t, isPropagatedField(&f))
	assert.Equal(t, "value", getPropagatedValue(&f))
}

func TestPropagatedInt64Field(t *testing.T) {
	f := PropagatedInt64("key", 12345)
	assert.Equal(t, "key", f.Key)
	assert.Equal(t, zapcore.ObjectMarshalerType, f.Type)
	assert.True(t, isPropagatedField(&f))
	assert.Equal(t, "12345", getPropagatedValue(&f))
}

func TestPropagatedInt64NegativeValue(t *testing.T) {
	f := PropagatedInt64("offset", -100)
	assert.Equal(t, "-100", getPropagatedValue(&f))
}

func TestRegularFieldIsNotPropagated(t *testing.T) {
	f := String("key", "value")
	assert.False(t, isPropagatedField(&f))
	assert.Equal(t, "", getPropagatedValue(&f))
}

// Tests for propagated fields via WithFields

func TestWithFieldsPropagatedAddsFieldsToContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx,
		PropagatedString(KeyCollectionName, "my_collection"),
		PropagatedInt64(KeyCollectionID, 12345),
	)

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 2)
}

func TestWithFieldsPropagatedFieldsAreAccessibleViaGetPropagated(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx,
		PropagatedString(KeyCollectionName, "my_collection"),
		PropagatedInt64(KeyCollectionID, 12345),
	)

	props := GetPropagated(ctx)
	assert.Len(t, props, 2)
	assert.Equal(t, "my_collection", props[KeyCollectionName])
	assert.Equal(t, "12345", props[KeyCollectionID])
}

func TestWithFieldsPropagatedAccumulates(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, PropagatedString("a", "1"))
	ctx = WithFields(ctx, PropagatedString("b", "2"))

	props := GetPropagated(ctx)
	assert.Len(t, props, 2)
	assert.Equal(t, "1", props["a"])
	assert.Equal(t, "2", props["b"])

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 2)
}

func TestWithFieldsPropagatedDoesNotMutateParent(t *testing.T) {
	ctx := context.Background()
	parentCtx := WithFields(ctx, PropagatedString("a", "1"))
	_ = WithFields(parentCtx, PropagatedString("b", "2"))

	props := GetPropagated(parentCtx)
	assert.Len(t, props, 1)
	assert.Equal(t, "1", props["a"])
}

func TestWithFieldsCombinesRegularAndPropagated(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("local", "value"))
	ctx = WithFields(ctx, PropagatedString("propagated", "pvalue"))

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 2)

	props := GetPropagated(ctx)
	assert.Len(t, props, 1)
	assert.Equal(t, "pvalue", props["propagated"])
}

func TestGetPropagatedNilContext(t *testing.T) {
	props := GetPropagated(nil)
	assert.Nil(t, props)
}

func TestGetPropagatedNoFields(t *testing.T) {
	ctx := context.Background()
	props := GetPropagated(ctx)
	assert.Nil(t, props)
}

func TestGetPropagatedOnlyRegularFields(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("key", "value"))

	props := GetPropagated(ctx)
	assert.Nil(t, props)
}

func TestWithFieldsNilContextWithPropagated(t *testing.T) {
	ctx := WithFields(nil, PropagatedString("key", "value"))
	assert.NotNil(t, ctx)

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1)

	props := GetPropagated(ctx)
	assert.Len(t, props, 1)
}

func TestWithFieldsEmptyFields(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx)

	// Should return original context unchanged
	props := GetPropagated(ctx)
	assert.Nil(t, props)
}

// Tests for field deduplication

func TestWithFieldsDeduplicatesByKey(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("key", "value1"))
	ctx = WithFields(ctx, String("key", "value2")) // same key, should override

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1, "duplicate key should be deduplicated")
	fm := fieldsToMap(fields)
	assert.Equal(t, "value2", fm["key"].String, "later value should win")
}

func TestWithFieldsDeduplicatesMultipleKeys(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("a", "1"), String("b", "2"))
	ctx = WithFields(ctx, String("a", "3"), String("c", "4")) // a is duplicate

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 3, "should have 3 unique keys")

	// Convert to map for easier assertion
	fieldMap := make(map[string]string)
	for _, f := range fields {
		fieldMap[f.Key] = f.String
	}
	assert.Equal(t, "3", fieldMap["a"], "a should have updated value")
	assert.Equal(t, "2", fieldMap["b"], "b should remain")
	assert.Equal(t, "4", fieldMap["c"], "c should be added")
}

func TestWithFieldsPropagatedDeduplicatesByKey(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, PropagatedString("key", "value1"))
	ctx = WithFields(ctx, PropagatedString("key", "value2")) // same key

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1, "duplicate key should be deduplicated")

	props := GetPropagated(ctx)
	assert.Equal(t, "value2", props["key"], "later value should win")
}

func TestMixedFieldsAndPropagatedDeduplication(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("shared", "from_fields"))
	ctx = WithFields(ctx, PropagatedString("shared", "from_propagated"))

	fields := FieldsFromContext(ctx)
	assert.Len(t, fields, 1, "same key from different sources should deduplicate")

	// Propagated should override and be accessible
	props := GetPropagated(ctx)
	assert.Equal(t, "from_propagated", props["shared"], "propagated should override")
}

// Tests for cached logger

func TestLogContextHasCachedLogger(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("key", "value"))

	lc := getLogContext(ctx)
	assert.NotNil(t, lc.logger, "logContext should have cached logger")
}

func TestCachedLoggerIncludesFields(t *testing.T) {
	// This test verifies that the cached logger has the fields applied
	// We can't easily inspect the logger's fields, but we can verify
	// the logger is not nil and is different from global logger
	ctx := context.Background()
	ctx = WithFields(ctx, String("key", "value"))

	lc := getLogContext(ctx)
	assert.NotNil(t, lc.logger)
	assert.NotSame(t, getLogger(), lc.logger, "cached logger should be different from global")
}

// Tests for fieldKeys type

func TestFieldKeysStoresFieldPointers(t *testing.T) {
	ctx := context.Background()
	ctx = WithFields(ctx, String("key", "value"))

	lc := getLogContext(ctx)
	assert.NotNil(t, lc.fieldKeys)
	assert.Len(t, lc.fieldKeys, 1)

	field, exists := lc.fieldKeys["key"]
	assert.True(t, exists)
	assert.NotNil(t, field)
	assert.Equal(t, "key", field.Key)
	assert.Equal(t, zapcore.StringType, field.Type)
	assert.Equal(t, "value", field.String)
}
