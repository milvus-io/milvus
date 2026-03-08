package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyYaml(t *testing.T) {
	file, err := os.CreateTemp(os.TempDir(), "milvus_ut_config_fs_*.yaml")
	require.NoError(t, err)

	filepath := file.Name()

	file.WriteString("#")
	file.Close()

	defer os.Remove(filepath)

	fs := NewFileSource(&FileInfo{
		Files:           []string{filepath},
		RefreshInterval: time.Hour,
	})

	_, err = fs.GetConfigurations()
	assert.NoError(t, err)

	fs.Close()
}
