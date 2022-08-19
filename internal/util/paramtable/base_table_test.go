// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/grpclog"
)

var baseParams = BaseTable{}

func TestMain(m *testing.M) {
	baseParams.Init()
	code := m.Run()
	os.Exit(code)
}

func TestBaseTable_GetConfigSubSet(t *testing.T) {
	err1 := baseParams.Save("a.b", "1")
	assert.Nil(t, err1)

	err2 := baseParams.Save("a.c", "2")
	assert.Nil(t, err2)

	subSet := baseParams.GetConfigSubSet("a")

	assert.Equal(t, len(subSet), 3)
	assert.Equal(t, subSet["b"], "1")
	assert.Equal(t, subSet["c"], "2")
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
	assert.Nil(t, err)

	v := baseParams.Get("key")
	assert.Equal(t, "10", v)

	v2 := baseParams.Get("none")
	assert.Equal(t, "", v2)
}

func TestBaseTable_Pulsar(t *testing.T) {
	//test PULSAR ADDRESS
	os.Setenv("PULSAR_ADDRESS", "pulsar://localhost:6650")
	baseParams.Init()

	address := baseParams.Get("pulsar.address")
	assert.Equal(t, "pulsar://localhost:6650", address)

	port := baseParams.Get("pulsar.port")
	assert.NotEqual(t, "", port)
}

// func TestBaseTable_ConfDir(t *testing.T) {
// 	rightConfig := baseParams.configDir
// 	// fake dir
// 	baseParams.configDir = "./"

// 	assert.Panics(t, func() { baseParams.loadFromYaml(defaultYaml) })

// 	baseParams.configDir = rightConfig
// 	baseParams.loadFromYaml(defaultYaml)
// 	baseParams.GlobalInitWithYaml(defaultYaml)
// }

// func TestBateTable_ConfPath(t *testing.T) {
// 	os.Setenv("MILVUSCONF", "test")
// 	config := baseParams.initConfPath()
// 	assert.Equal(t, config, "test")

// 	os.Unsetenv("MILVUSCONF")
// 	dir, _ := os.Getwd()
// 	config = baseParams.initConfPath()
// 	assert.Equal(t, filepath.Clean(config), filepath.Clean(dir+"/../../../configs/"))

// 	// test use get dir
// 	os.Chdir(dir + "/../../../")
// 	defer os.Chdir(dir)
// 	config = baseParams.initConfPath()
// 	assert.Equal(t, filepath.Clean(config), filepath.Clean(dir+"/../../../configs/"))
// }

func TestBaseTable_Env(t *testing.T) {
	os.Setenv("milvus.test", "test")
	os.Setenv("milvus.test.test2", "test2")

	baseParams.Init()
	result, _ := baseParams.Load("test")
	assert.Equal(t, result, "test")

	result, _ = baseParams.Load("test.test2")
	assert.Equal(t, result, "test2")

	err := os.Setenv("milvus.invalid=xxx", "test")
	assert.Error(t, err)

	err = os.Setenv("milvus.invalid", "xxx=test")
	assert.NoError(t, err)

	baseParams.Init()
	result, _ = baseParams.Load("invalid")
	assert.Equal(t, result, "xxx=test")
}

func TestBaseTable_Parse(t *testing.T) {
	t.Run("ParseBool", func(t *testing.T) {
		assert.Nil(t, baseParams.Save("key", "true"))
		assert.True(t, baseParams.ParseBool("key", false))
		assert.False(t, baseParams.ParseBool("not_exist_key", false))

		assert.Nil(t, baseParams.Save("key", "rand"))
		assert.Panics(t, func() { baseParams.ParseBool("key", false) })
	})

	t.Run("ParseFloat", func(t *testing.T) {
		assert.Nil(t, baseParams.Save("key", "0"))
		assert.Equal(t, float64(0), baseParams.ParseFloat("key"))

		assert.Nil(t, baseParams.Save("key", "3.14"))
		assert.Equal(t, float64(3.14), baseParams.ParseFloat("key"))

		assert.Panics(t, func() { baseParams.ParseFloat("not_exist_key") })
		assert.Nil(t, baseParams.Save("key", "abc"))
		assert.Panics(t, func() { baseParams.ParseFloat("key") })
	})

	t.Run("ParseFloatWithDefault", func(t *testing.T) {
		baseParams.Remove("key")
		assert.Equal(t, float64(0.0), baseParams.ParseFloatWithDefault("key", 0.0))
		assert.Equal(t, float64(3.14), baseParams.ParseFloatWithDefault("key", 3.14))

		assert.Nil(t, baseParams.Save("key", "2"))
		assert.Equal(t, float64(2.0), baseParams.ParseFloatWithDefault("key", 3.14))
	})

	t.Run("ParseInt32", func(t *testing.T) {
		assert.Nil(t, baseParams.Save("key", "0"))
		assert.Equal(t, int32(0), baseParams.ParseInt32("key"))

		assert.Nil(t, baseParams.Save("key", "314"))
		assert.Equal(t, int32(314), baseParams.ParseInt32("key"))

		assert.Panics(t, func() { baseParams.ParseInt32("not_exist_key") })
		assert.Nil(t, baseParams.Save("key", "abc"))
		assert.Panics(t, func() { baseParams.ParseInt32("key") })
	})

	t.Run("ParseInt32WithDefault", func(t *testing.T) {
		baseParams.Remove("key")
		assert.Equal(t, int32(1), baseParams.ParseInt32WithDefault("key", 1))
		assert.Nil(t, baseParams.Save("key", "2"))
		assert.Equal(t, int32(2), baseParams.ParseInt32WithDefault("key", 1))
	})

	t.Run("ParseInt64", func(t *testing.T) {
		assert.Nil(t, baseParams.Save("key", "0"))
		assert.Equal(t, int64(0), baseParams.ParseInt64("key"))

		assert.Nil(t, baseParams.Save("key", "314"))
		assert.Equal(t, int64(314), baseParams.ParseInt64("key"))

		assert.Panics(t, func() { baseParams.ParseInt64("not_exist_key") })
		assert.Nil(t, baseParams.Save("key", "abc"))
		assert.Panics(t, func() { baseParams.ParseInt64("key") })
	})

	t.Run("ParseInt64WithDefault", func(t *testing.T) {
		baseParams.Remove("key")
		assert.Equal(t, int64(1), baseParams.ParseInt64WithDefault("key", 1))
		assert.Nil(t, baseParams.Save("key", "2"))
		assert.Equal(t, int64(2), baseParams.ParseInt64WithDefault("key", 1))
	})

	t.Run("ParseIntWithDefault", func(t *testing.T) {
		baseParams.Remove("key")
		assert.Equal(t, int(1), baseParams.ParseIntWithDefault("key", 1))
		assert.Nil(t, baseParams.Save("key", "2"))
		assert.Equal(t, int(2), baseParams.ParseIntWithDefault("key", 1))
	})
}

func Test_ConvertRangeToIntSlice(t *testing.T) {
	t.Run("ConvertRangeToIntSlice", func(t *testing.T) {
		slice := ConvertRangeToIntSlice("0,10", ",")
		assert.Equal(t, 10, len(slice))

		assert.Panics(t, func() { ConvertRangeToIntSlice("0", ",") })
		assert.Panics(t, func() { ConvertRangeToIntSlice("0, 10", ",") })
		assert.Panics(t, func() { ConvertRangeToIntSlice("abc,10", ",") })
		assert.Panics(t, func() { ConvertRangeToIntSlice("0,abc", ",") })
		assert.Panics(t, func() { ConvertRangeToIntSlice("-1,9", ",") })
		assert.Panics(t, func() { ConvertRangeToIntSlice("9,0", ",") })
	})
}

func Test_SetLogger(t *testing.T) {
	t.Run("TestSetLooger", func(t *testing.T) {
		baseParams.RoleName = "rootcoord"
		baseParams.Save("log.file.rootPath", ".")
		baseParams.SetLogger(UniqueID(-1))
		assert.Equal(t, "rootcoord.log", baseParams.Log.File.Filename)

		baseParams.RoleName = "datanode"
		baseParams.SetLogger(UniqueID(1))
		assert.Equal(t, "datanode-1.log", baseParams.Log.File.Filename)

		baseParams.RoleName = "datanode"
		baseParams.SetLogger(UniqueID(0))
		assert.Equal(t, "datanode-0.log", baseParams.Log.File.Filename)
	})

	t.Run("TestGrpclog", func(t *testing.T) {
		baseParams.Save("grpc.log.level", "Warning")
		baseParams.SetLogConfig()

		baseParams.SetLogger(UniqueID(1))
		assert.Equal(t, false, grpclog.V(0))
		assert.Equal(t, true, grpclog.V(1))
		assert.Equal(t, true, grpclog.V(2))
	})
}
