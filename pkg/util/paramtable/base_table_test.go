// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/config"
	etcdkv "github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
)

var baseParams = NewBaseTable(SkipRemote(true))

func TestMain(m *testing.M) {
	baseParams.init()
	code := m.Run()
	// Stop the shared embedded etcd server (started lazily by the version-gate
	// tests) after all tests.
	if etcdkv.HasServer() {
		etcdkv.StopEtcdServer()
	}
	os.Exit(code)
}

func TestRepositoryTestConfig(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "configs")
	t.Setenv("MILVUSCONF", dir)
	t.Setenv("MILVUS_ENABLE_TEST_CONFIG", "true")
	data, err := os.ReadFile(filepath.Join(dir, config.TestConfigFile))
	require.NoError(t, err)
	overrides, err := config.DecodeTestConfig(data)
	require.NoError(t, err)

	base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
	t.Cleanup(base.Manager().Close)
	params := NewComponentParamForConfigGeneration(base)
	checkRepositoryOverrides(t, params, "", overrides)
}

func checkRepositoryOverrides(t *testing.T, params *ComponentParam, prefix string, overrides map[string]any) {
	t.Helper()
	for name, value := range overrides {
		key := prefix + name
		if children, ok := value.(map[string]any); ok {
			require.NotEmpty(t, children, key)
			checkRepositoryOverrides(t, params, key+".", children)
			continue
		}
		require.True(t, isConfigRegistered(params.baseTable.Manager(), key), "unknown setting %q", key)
		raw := fmt.Sprint(value)
		coord := &params.DataCoordCfg
		switch config.FormatKey(key) {
		case config.FormatKey(params.LogCfg.Level.Key):
			require.Contains(t, []string{"trace", "debug", "info", "warn", "error", "panic", "fatal"}, strings.ToLower(raw), key)
		case config.FormatKey(params.DataNodeCfg.StorageFormat.Key):
			require.Contains(t, []string{specutil.FormatParquet, specutil.FormatVortex}, raw, key)
		case config.FormatKey(coord.ForceRebuildScalarSegmentIndex.Key),
			config.FormatKey(coord.BumpSchemaVersionCompactionEnabled.Key),
			config.FormatKey(coord.EnableAutoCompaction.Key):
			_, err := strconv.ParseBool(raw)
			require.NoError(t, err, key)
		case config.FormatKey(coord.TargetVecIndexVersion.Key), config.FormatKey(coord.TargetScalarIndexVersion.Key):
			version, err := strconv.ParseInt(raw, 10, 32)
			require.NoError(t, err, key)
			if config.FormatKey(key) == config.FormatKey(coord.TargetScalarIndexVersion.Key) && version != -1 {
				require.GreaterOrEqual(t, version, int64(common.MinimalScalarIndexEngineVersion), key)
				require.LessOrEqual(t, version, int64(common.MaximumScalarIndexEngineVersion), key)
			}
		default:
			t.Fatalf("add a value check for shared test setting %q in checkRepositoryOverrides", key)
		}
	}
}

func TestBaseTable_TestConfigOverlay(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MILVUSCONF", dir)
	for name, contents := range map[string]string{
		"milvus.yaml":  "feature: base\nchart: base\nuser: base\n",
		"_test.yaml":   "feature: test\nchart: test\nuser: test\n",
		"default.yaml": "chart: old\nchart: chart-default\nuser: chart-default\n", // Deployment files retain last-key-wins behavior.
		"user.yaml":    "user: user-override\n",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600))
	}

	const toggle = "MILVUS_ENABLE_TEST_CONFIG"
	t.Setenv(toggle, "")
	require.NoError(t, os.Unsetenv(toggle))
	for _, tc := range []struct {
		name, env, want string
	}{
		{"unset", "", "base"},
		{"disabled", "false", "base"},
		{"enabled", "true", "test"},
		{"unrecognized", "tru", "base"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv(toggle, tc.env)
			}
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
			t.Cleanup(base.mgr.Close)
			assert.Equal(t, tc.want, base.Get("feature"))
			assert.Equal(t, "chart-default", base.Get("chart"))
			assert.Equal(t, "user-override", base.Get("user"))
		})
	}

	t.Setenv(toggle, "true")
	testFile := filepath.Join(dir, "_test.yaml")
	const defaults = "defaults: &defaults {level: info, active: yes, label: 'yes', date: 2025-02-01}\n" +
		"fallback: &fallback {level: warn, active: no}\nfield: &field level\n"
	for _, tc := range []struct{ name, log, level string }{
		{"merge before override", "<<: *defaults, level: debug", "debug"},
		{"merge after override", "level: debug, <<: *defaults", "info"},
		{"alias before merge", "*field : debug, <<: *defaults", "info"},
		{"ordered merge sources", "<<: [*defaults, *fallback]", "info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(defaults + "log: {" + tc.log + "}\n")
			require.NoError(t, os.WriteFile(testFile, data, 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), data, 0o600))
			deployment := NewBaseTableFromYamlOnly("deployment.yaml")
			t.Cleanup(deployment.mgr.Close)
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
			t.Cleanup(base.mgr.Close)
			assert.Equal(t, tc.level, base.Get("log.level"))
			assert.Equal(t, deployment.Get("log.level"), base.Get("log.level"))
			assert.Equal(t, "true", base.Get("log.active"))
			assert.Equal(t, "yes", base.Get("log.label"))
			assert.Equal(t, "2025-02-01", base.Get("log.date"))
		})
	}
	t.Run("anchored scalar used as both key and value", func(t *testing.T) {
		require.NoError(t, os.WriteFile(testFile, []byte("flag: &flag yes\n*flag : literal-key\ncopy: *flag\n"), 0o600))
		base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
		t.Cleanup(base.mgr.Close)
		assert.Equal(t, "literal-key", base.Get("yes"))
		assert.Equal(t, "true", base.Get("copy"))
	})
	for _, empty := range []string{"", "# no overrides\n", "---\n", "--- # empty\n...\n", "{}\n"} {
		require.NoError(t, os.WriteFile(testFile, []byte(empty), 0o600))
		base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
		assert.Equal(t, "base", base.Get("feature"))
		base.mgr.Close()
	}
	invalid := []byte("feature: [\n")
	require.NoError(t, os.WriteFile(testFile, invalid, 0o600))
	assertStartupFails := func(t *testing.T) {
		t.Helper()
		require.PanicsWithValue(t, "failed to load local configuration including _test.yaml", func() {
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
			base.mgr.Close()
		})
	}
	t.Run("malformed test file", assertStartupFails)

	t.Run("disabled with malformed test file", func(t *testing.T) {
		t.Setenv(toggle, "false")
		base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
		t.Cleanup(base.mgr.Close)
		assert.Equal(t, "base", base.Get("feature"))
		assert.Equal(t, "user-override", base.Get("user"))
	})

	explicit := NewBaseTableFromYamlOnly("milvus.yaml")
	t.Cleanup(explicit.mgr.Close)
	assert.Equal(t, "base", explicit.Get("feature"))

	for _, tc := range []struct{ name, yaml string }{
		{"explicit null", "null\n"},
		{"null shorthand", "~\n"},
		{"tagged null", "!!null ''\n"},
		{"null-tagged mapping", "!!null {}\n"},
		{"sequence tag on mapping", "log: !!seq {level: debug}\n"},
		{"mapping tag on sequence", "etcd: {endpoints: !!map [localhost:2379]}\n"},
		{"scalar tag on sequence", "items: !!str [debug]\n"},
		{"collection tag on scalar", "log: {level: !!map debug}\n"},
		{"mismatched tag on merge source", "log: {<<: !!seq {level: warn}, level: debug}\n"},
		{"scalar root", "debug\n"},
		{"sequence root", "[debug]\n"},
		{"unconvertible endpoint", "etcd: {endpoints: ['a:2379', ['b:2379']]}\n"},
		{"unconvertible object list", "settings: [{ratio: .nan}]\n"},
		{"masked scalar merge source", "log: {level: debug, <<: {level: {<<: 10}}}\n"},
		{"masked invalid merge sequence", "log: {level: debug, <<: {level: {<<: [{other: 1}, 10]}}}\n"},
		{"masked recursive merge", "log: {level: debug, <<: {level: &loop {<<: *loop}}}\n"},
		{"masked alias to scalar merge source", "scalar: &s 10\nlog: {level: debug, <<: {level: {<<: *s}}}\n"},
		{"invalid explicit boolean", "feature: !!bool sometimes\n"},
		{"malformed inherited value", "log: {level: debug, <<: {level: !!int invalid}}\n"},
		{"duplicate section", "dataCoord:\n  targetVecIndexVersion: 11\ndataCoord:\n  targetScalarIndexVersion: 5\n"},
		{"duplicate alias key after literal", "field: &key level\nlog: {level: debug, *key : info}\n"},
		{"duplicate alias key before literal", "field: &key level\nlog: {*key : info, level: debug}\n"},
		{"duplicate keys from different anchors", "first: &a level\nsecond: &b level\nlog: {*a : info, *b : debug}\n"},
		{"duplicate parameter", "dataCoord:\n  targetVecIndexVersion: 11\n  targetVecIndexVersion: 10\n"},
		{"nested and dotted parameter", "dataCoord:\n  targetVecIndexVersion: 11\ndataCoord.targetVecIndexVersion: 9\n"},
		{"normalized parameter", "DATA_COORD/targetVecIndexVersion: 9\ndataCoord.targetVecIndexVersion: 11\n"},
		{"list and scalar parameter", "items: [a, b]\nITEMS: c\n"},
		{"second document", "log: {level: debug}\n---\ndataNode: {storage: {format: vortex}}\n"},
		{"malformed second document", "log: {level: debug}\n---\ndataNode: [\n"},
		{"empty second document", "log: {level: debug}\n---\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(testFile, []byte(tc.yaml), 0o600))
			assertStartupFails(t)
		})
	}

	require.NoError(t, os.Remove(testFile))
	base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
	t.Cleanup(base.mgr.Close)
	assert.Equal(t, "base", base.Get("feature"))

	t.Run("malformed user file prevents applying test file", func(t *testing.T) {
		require.NoError(t, os.WriteFile(testFile, []byte("feature: test\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "user.yaml"), invalid, 0o600))
		assertStartupFails(t)
	})
}

func TestBaseTable_DuplicateValues(t *testing.T) {
	baseParams.Save("rootcoord.dmlchannelnum", "10")
	baseParams.Save("rootcoorddmlchannelnum", "11")

	prefix := "rootcoord."
	configs := baseParams.mgr.GetConfigs()

	configsWithPrefix := make(map[string]string)
	for k, v := range configs {
		if strings.HasPrefix(k, prefix) {
			configsWithPrefix[k] = v
		}
	}

	rootconfigs := baseParams.mgr.GetBy(config.WithPrefix(prefix))

	assert.Equal(t, len(rootconfigs), len(configsWithPrefix))
	assert.Equal(t, "11", rootconfigs["rootcoord.dmlchannelnum"])
}

func TestBaseTable_RemoveThenSaveGroupMember(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MILVUSCONF", dir)
	require.NoError(t, os.WriteFile(dir+"/milvus.yaml", []byte("public:\n  group:\n    member: source-value\n"), 0o600))
	base := NewBaseTable(Files([]string{"milvus.yaml"}), SkipRemote(true), SkipEnv(true), Interval(0))
	t.Cleanup(base.mgr.Close)
	group := ParamGroup{KeyPrefix: "public.group."}
	group.Init(base.mgr)

	const key = "public.group.member"
	require.NoError(t, base.Remove(key))
	assert.Empty(t, group.GetValue())
	require.NoError(t, base.Save(key, "restored-value"))
	source, value, err := base.mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, config.RuntimeSource, source)
	assert.Equal(t, "restored-value", value)
	assert.Equal(t, value, base.Get(key))
	assert.Equal(t, value, group.GetValue()["member"])
}

func TestBaseTable_SaveAndLoad(t *testing.T) {
	err1 := baseParams.Save("int", "10")
	assert.Nil(t, err1)

	err2 := baseParams.Save("string", "testSaveAndLoad")
	assert.Nil(t, err2)

	err3 := baseParams.Save("float", "1.234")
	assert.Nil(t, err3)

	r1, _ := baseParams.Load("int")
	assert.Equal(t, "10", r1)

	r2, _ := baseParams.Load("string")
	assert.Equal(t, "testSaveAndLoad", r2)

	r3, _ := baseParams.Load("float")
	assert.Equal(t, "1.234", r3)

	err4 := baseParams.Remove("int")
	assert.Nil(t, err4)

	err5 := baseParams.Remove("string")
	assert.Nil(t, err5)

	err6 := baseParams.Remove("float")
	assert.Nil(t, err6)
}

func TestBaseTable_Remove(t *testing.T) {
	err1 := baseParams.Save("RemoveInt", "10")
	assert.Nil(t, err1)

	err2 := baseParams.Save("RemoveString", "testRemove")
	assert.Nil(t, err2)

	err3 := baseParams.Save("RemoveFloat", "1.234")
	assert.Nil(t, err3)

	err4 := baseParams.Remove("RemoveInt")
	assert.Nil(t, err4)

	err5 := baseParams.Remove("RemoveString")
	assert.Nil(t, err5)

	err6 := baseParams.Remove("RemoveFloat")
	assert.Nil(t, err6)
}

func TestBaseTable_Get(t *testing.T) {
	err := baseParams.Save("key", "10")
	assert.NoError(t, err)

	v := baseParams.Get("key")
	assert.Equal(t, "10", v)

	v2 := baseParams.Get("none")
	assert.Equal(t, "", v2)
}

func TestBaseTable_Pulsar(t *testing.T) {
	// test PULSAR ADDRESS
	t.Setenv("PULSAR_ADDRESS", "pulsar://localhost:6650")
	baseParams.init()

	address := baseParams.Get("pulsar.address")
	assert.Equal(t, "pulsar://localhost:6650", address)

	port := baseParams.Get("pulsar.port")
	assert.NotEqual(t, "", port)
}

func TestBaseTable_Env(t *testing.T) {
	t.Setenv("milvus.test", "test")
	t.Setenv("milvus.test.test2", "test2")

	baseParams.init()
	result, _ := baseParams.Load("test")
	assert.Equal(t, result, "test")

	result, _ = baseParams.Load("test.test2")
	assert.Equal(t, result, "test2")

	t.Setenv("milvus.invalid", "xxx=test")

	baseParams.init()
	result, _ = baseParams.Load("invalid")
	assert.Equal(t, result, "xxx=test")
}

func TestNewBaseTableFromYamlOnly(t *testing.T) {
	var yaml string
	var gp *BaseTable
	yaml = "not_exist.yaml"
	gp = NewBaseTableFromYamlOnly(yaml)
	assert.Empty(t, gp.Get("key"))
}
