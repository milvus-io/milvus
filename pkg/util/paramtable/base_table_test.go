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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
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
