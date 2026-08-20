package hookutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadPlugin_EmptyPath(t *testing.T) {
	type Dummy interface{}
	_, err := LoadPlugin[Dummy]("", "Symbol")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty plugin path")
}

func TestLoadPlugin_NonExistentFile(t *testing.T) {
	type Dummy interface{}
	_, err := LoadPlugin[Dummy]("/nonexistent/plugin.so", "Symbol")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail to open plugin")
}

func TestFinishPluginLoading(t *testing.T) {
	// FinishPluginLoading must not panic with unconfigured plugin paths, and
	// must be safe to call more than once (InitOnceCipher is a sync.Once and
	// json.DisableGate is idempotent).
	FinishPluginLoading()
	FinishPluginLoading()
}
