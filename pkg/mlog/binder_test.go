//go:build test

package mlog

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinderReturnsDefaultLoggerWhenUnset(t *testing.T) {
	var buf bytes.Buffer
	initForTest(createTestLogger(&buf))
	defer resetLogger()

	var binder Binder
	binder.Logger().Info(context.Background(), "default logger", String("key", "value"))

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "default logger", entry["msg"])
	assert.Equal(t, "value", entry["key"])
}

func TestBinderUsesExplicitLogger(t *testing.T) {
	var buf bytes.Buffer
	initForTest(createTestLogger(&buf))
	defer resetLogger()

	var binder Binder
	binder.SetLogger(With(FieldComponent("wal")))
	binder.Logger().Info(context.Background(), "bound logger")

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "bound logger", entry["msg"])
	assert.Equal(t, "wal", entry[keyComponent])
}

func TestBinderAcceptsNilLogger(t *testing.T) {
	var buf bytes.Buffer
	initForTest(createTestLogger(&buf))
	defer resetLogger()

	var binder Binder
	binder.SetLogger(With(FieldComponent("wal")))
	binder.SetLogger(nil)
	binder.Logger().Info(context.Background(), "fallback logger", String("key", "value"))

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "fallback logger", entry["msg"])
	assert.Equal(t, "value", entry["key"])
	assert.Nil(t, entry[keyComponent])
}
