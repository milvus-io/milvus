package hookutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestLoadPlugin_EmptyPath(t *testing.T) {
	type Dummy interface{}
	_, err := LoadPlugin[Dummy]("", "Symbol")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty plugin path")
}

func TestLoadPlugin_NonExistentFile(t *testing.T) {
	type Dummy interface{}
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	_, err := LoadPlugin[Dummy]("/nonexistent/plugin-path-canary.so", "Symbol")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail to open plugin")
	assert.Contains(t, err.Error(), "plugin-path-canary")
	assert.NotContains(t, sink.String(), "plugin-path-canary")
}
