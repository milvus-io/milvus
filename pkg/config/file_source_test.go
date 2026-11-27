package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestConfigRefreshKeepsLastValidValues(t *testing.T) {
	dir := t.TempDir()
	overlay := filepath.Join(dir, TestConfigFile)
	user := filepath.Join(dir, "user.yaml")
	valid := []byte("dataCoord: {targetVecIndexVersion: 11}\nlog: {level: debug}\nknowhere: {INDEX: 1, index: 2}\n...\n# end\n")
	require.NoError(t, os.WriteFile(overlay, valid, 0o600))
	require.NoError(t, os.WriteFile(user, []byte("log.level: warn\n"), 0o600))
	fs := NewFileSource(&FileInfo{Files: []string{overlay, user}})
	t.Cleanup(fs.Close)
	values, err := fs.GetConfigurations()
	require.NoError(t, err)
	assert.Equal(t, "warn", values["log.level"])
	assert.Equal(t, "1", values["knowhere.INDEX"])
	assert.Equal(t, "2", values["knowhere.index"])

	for _, invalid := range []string{
		"dataCoord.targetVecIndexVersion: 9\ndataCoord: {targetVecIndexVersion: 7}\n",
		"dataCoord: {targetVecIndexVersion: 9}\netcd: {endpoints: [a, [b]]}\n",
		"dataCoord: {targetVecIndexVersion: 9}\nproxy.accessLog.formatters: {<<: {1: {format: inherited}}, '1': {format: explicit}}\n",
	} {
		require.NoError(t, os.WriteFile(overlay, []byte(invalid), 0o600))
		_, err = fs.GetConfigurations()
		require.Error(t, err)
		version, err := fs.GetConfigurationByKey(formatKey("dataCoord.targetVecIndexVersion"))
		require.NoError(t, err)
		assert.Equal(t, "11", version)
	}
}

func TestTestConfigMergedKeyCollisions(t *testing.T) {
	for _, tc := range []struct{ name, data, key string }{
		{"formatter", "proxy.accessLog.formatters: {<<: {1: {format: inherited}}, '1': {format: explicit}}", "1"},
		{"merge sources", "first: &a {true: null}\nsecond: &b {'true': enabled}\nsettings: {<<: [*a, *b]}", "true"},
		{"list item", "settings: [{<<: {1: first}, '1': second}]", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.data)
			_, err := DecodeTestConfig(data)
			require.ErrorContains(t, err, `multiple YAML keys resolve to "`+tc.key+`"`)
			require.NoError(t, mergeYAML("user.yaml", data, make(map[string]string)))
		})
	}

	const valid = "proxy.accessLog.formatters: {<<: {'1': {format: inherited}}, '1': {format: explicit}}"
	for _, file := range []string{TestConfigFile, "user.yaml"} {
		result := make(map[string]string)
		require.NoError(t, mergeYAML(file, []byte(valid), result))
		require.Equal(t, "explicit", result["proxy.accesslog.formatters.1.format"])
	}
}

func TestFileSourceExplicitBooleanCompatibility(t *testing.T) {
	const data = `enabled: &flag !!bool yes
copy: *flag
disabled: !!bool 'OFF'
label: !!str yes
*flag : named
`
	for _, name := range []string{"user.yaml", TestConfigFile} {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), name)
			require.NoError(t, os.WriteFile(file, []byte(data), 0o600))
			source := NewFileSource(&FileInfo{Files: []string{file}})
			t.Cleanup(source.Close)
			got, err := source.GetConfigurations()
			require.NoError(t, err)
			assert.Equal(t, map[string]string{
				"enabled": "true", "copy": "true", "disabled": "false", "label": "yes", "yes": "named",
			}, got)
		})
	}
}

func TestTestConfigNestedKeys(t *testing.T) {
	const prefix = "proxy: {accessLog: {formatters: "
	for _, mapping := range []string{
		"{on: {format: ci}}", "{01: {format: ci}}",
		"{<<: {on: {format: ci}}}", "{name: &key 01, *key : {format: ci}}",
	} {
		_, err := DecodeTestConfig([]byte(prefix + mapping + "}}"))
		require.ErrorContains(t, err, "quote", mapping)
	}

	// The two files must address the same formatter, including when a quoted
	// literal coexists with the canonical spelling of a boolean or an integer.
	result := make(map[string]string)
	for _, file := range []string{TestConfigFile, "user.yaml"} {
		data := []byte(prefix + "{'on': " + file + ", true: " + file + ", '01': " + file + ", 1: " + file + "}}}")
		require.NoError(t, mergeYAML(file, data, result))
		for _, key := range []string{"on", "true", "01", "1"} {
			require.Equal(t, file, result["proxy.accesslog.formatters."+key])
		}
	}
}

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
