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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

var baseParams = NewBaseTable(SkipRemote(true))

func TestMain(m *testing.M) {
	baseParams.init()
	code := m.Run()
	os.Exit(code)
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

// setPrimaryConfigNameVar stands in for the link-time -X assignment and
// restores the default after the test.
func setPrimaryConfigNameVar(t *testing.T, name string) {
	t.Helper()
	old := primaryConfigName
	primaryConfigName = name
	t.Cleanup(func() { primaryConfigName = old })
}

func TestPrimaryConfigNameDefaultsToMilvusYaml(t *testing.T) {
	assert.Equal(t, "milvus.yaml", PrimaryConfigName())
	assert.Equal(t, []string{"milvus.yaml", "_test.yaml", "default.yaml", "user.yaml"}, defaultYamlFiles())
}

// The primary configuration file is replaceable per deployment form at link
// time; the rest of the list keeps its meaning, and a table built after that
// reads the new name in milvus.yaml's position.
func TestPrimaryConfigNameFromLinkTime(t *testing.T) {
	setPrimaryConfigNameVar(t, "kite.yaml")
	assert.Equal(t, "kite.yaml", PrimaryConfigName())
	assert.Equal(t, []string{"kite.yaml", "_test.yaml", "default.yaml", "user.yaml"}, defaultYamlFiles(),
		"only the primary entry may change; the layering of the others is load-bearing")

	setPrimaryConfigNameVar(t, "kite.yml")
	assert.Equal(t, "kite.yml", PrimaryConfigName())
}

// The environment overrides the link-time name, so an operator can point a
// built binary at another file without rebuilding it.
func TestPrimaryConfigNameEnvOverridesLinkTime(t *testing.T) {
	setPrimaryConfigNameVar(t, "kite.yaml")
	t.Setenv(MilvusPrimaryConfigEnvKey, "kite-dev.yaml")
	assert.Equal(t, "kite-dev.yaml", PrimaryConfigName())
	assert.Equal(t, "kite-dev.yaml", defaultYamlFiles()[0])
}

// A bad primary name does not fail locally: the file source rejects it and
// drops every local yaml source with a warning, so the process would run on
// compiled-in defaults. The name is therefore refused at the first paramtable,
// on both the link-time and the environment path.
func TestPrimaryConfigNameRefusesABadName(t *testing.T) {
	for _, bad := range []string{"", "kite", "kite.conf", "conf/kite.yaml", "../kite.yaml"} {
		t.Run("linktime:"+bad, func(t *testing.T) {
			setPrimaryConfigNameVar(t, bad)
			assert.Panics(t, func() { PrimaryConfigName() })
			assert.Panics(t, func() { NewBaseTable(SkipRemote(true), SkipEnv(true)) },
				"a table must not come up on a name the file source would reject")
		})
		if bad == "" {
			continue // an empty environment variable is "unset", not a name
		}
		t.Run("env:"+bad, func(t *testing.T) {
			t.Setenv(MilvusPrimaryConfigEnvKey, bad)
			assert.Panics(t, func() { PrimaryConfigName() })
		})
	}
}

// A table built with the primary name replaced must actually READ that file
// in milvus.yaml's position - and must not read milvus.yaml, which the
// directory may still carry. This is the end-to-end check the name plumbing
// exists for.
func TestPrimaryConfigIsReadEndToEnd(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kite.yaml"), []byte("primaryprobe: fromkite\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"), []byte("primaryprobe: frommilvus\n"), 0o600))
	t.Setenv("MILVUSCONF", dir)

	native := NewBaseTable(SkipRemote(true), SkipEnv(true))
	assert.Equal(t, "frommilvus", native.Get("primaryprobe"), "a stock table reads milvus.yaml")

	t.Setenv(MilvusPrimaryConfigEnvKey, "kite.yaml")
	replaced := NewBaseTable(SkipRemote(true), SkipEnv(true))
	assert.Equal(t, "fromkite", replaced.Get("primaryprobe"),
		"a table built under the replaced name must read that file, not milvus.yaml")

	// A missing primary is skipped, as a missing milvus.yaml is: the table
	// comes up on defaults rather than failing.
	t.Setenv(MilvusPrimaryConfigEnvKey, "absent.yaml")
	assert.NotPanics(t, func() {
		missing := NewBaseTable(SkipRemote(true), SkipEnv(true))
		assert.Empty(t, missing.Get("primaryprobe"), "neither file is read when the primary is absent")
	})
}
