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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
)

var baseParams = BaseTable{}

func TestMain(m *testing.M) {
	baseParams.Init()
	code := m.Run()
	os.Exit(code)
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

func TestBaseTable_LoadFromKVPair(t *testing.T) {
	var kvPairs []*commonpb.KeyValuePair
	kvPairs = append(kvPairs, &commonpb.KeyValuePair{
		Key:   "k1",
		Value: "v1",
	})
	kvPairs = append(kvPairs, &commonpb.KeyValuePair{
		Key:   "k2",
		Value: "v2",
	})

	err := baseParams.LoadFromKVPair(kvPairs)
	assert.Nil(t, err)

	v, err := baseParams.Load("k1")
	assert.Nil(t, err)
	assert.Equal(t, "v1", v)

	v, err = baseParams.Load("k2")
	assert.Nil(t, err)
	assert.Equal(t, "v2", v)
}

func TestBaseTable_LoadRange(t *testing.T) {
	_ = baseParams.Save("xxxaab", "10")
	_ = baseParams.Save("xxxfghz", "20")
	_ = baseParams.Save("xxxbcde", "1.1")
	_ = baseParams.Save("xxxabcd", "testSaveAndLoad")
	_ = baseParams.Save("xxxzhi", "12")

	keys, values, err := baseParams.LoadRange("xxxa", "xxxg", 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(keys))
	assert.Equal(t, "10", values[0])
	assert.Equal(t, "testSaveAndLoad", values[1])
	assert.Equal(t, "1.1", values[2])
	assert.Equal(t, "20", values[3])

	_ = baseParams.Remove("abc")
	_ = baseParams.Remove("fghz")
	_ = baseParams.Remove("bcde")
	_ = baseParams.Remove("abcd")
	_ = baseParams.Remove("zhi")
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

func TestBaseTable_LoadYaml(t *testing.T) {
	err := baseParams.LoadYaml("milvus.yaml")
	assert.Nil(t, err)
	err = baseParams.LoadYaml("advanced/channel.yaml")
	assert.Nil(t, err)
	assert.Panics(t, func() { baseParams.LoadYaml("advanced/not_exist.yaml") })

	_, err = baseParams.Load("etcd.address")
	assert.Nil(t, err)
	_, err = baseParams.Load("pulsar.port")
	assert.Nil(t, err)
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

	t.Run("ParseInt32", func(t *testing.T) {
		assert.Nil(t, baseParams.Save("key", "0"))
		assert.Equal(t, int32(0), baseParams.ParseInt32("key"))

		assert.Nil(t, baseParams.Save("key", "314"))
		assert.Equal(t, int32(314), baseParams.ParseInt32("key"))

		assert.Panics(t, func() { baseParams.ParseInt32("not_exist_key") })
		assert.Nil(t, baseParams.Save("key", "abc"))
		assert.Panics(t, func() { baseParams.ParseInt32("key") })
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
