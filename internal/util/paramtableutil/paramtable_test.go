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

package paramtableutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var param = NewGlobalParamsTable()

func TestGlobalParamsTable_SaveAndLoad(t *testing.T) {
	err1 := param.Save("int", "10")
	assert.Nil(t, err1)

	err2 := param.Save("string", "testSaveAndLoad")
	assert.Nil(t, err2)

	err3 := param.Save("float", "1.234")
	assert.Nil(t, err3)

	r1, _ := param.Load("int")
	assert.Equal(t, "10", r1)

	r2, _ := param.Load("string")
	assert.Equal(t, "testSaveAndLoad", r2)

	r3, _ := param.Load("float")
	assert.Equal(t, "1.234", r3)

	err4 := param.Remove("int")
	assert.Nil(t, err4)

	err5 := param.Remove("string")
	assert.Nil(t, err5)

	err6 := param.Remove("float")
	assert.Nil(t, err6)
}

func TestGlobalParamsTable_LoadRange(t *testing.T) {
	_ = param.Save("abc", "10")
	_ = param.Save("fghz", "20")
	_ = param.Save("bcde", "1.1")
	_ = param.Save("abcd", "testSaveAndLoad")
	_ = param.Save("zhi", "12")

	keys, values, err := param.LoadRange("a", "g", 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(keys))
	assert.Equal(t, "10", values[0])
	assert.Equal(t, "testSaveAndLoad", values[1])
	assert.Equal(t, "1.1", values[2])
	assert.Equal(t, "20", values[3])

	_ = param.Remove("abc")
	_ = param.Remove("fghz")
	_ = param.Remove("bcde")
	_ = param.Remove("abcd")
	_ = param.Remove("zhi")
}

func TestGlobalParamsTable_Remove(t *testing.T) {
	err1 := param.Save("RemoveInt", "10")
	assert.Nil(t, err1)

	err2 := param.Save("RemoveString", "testRemove")
	assert.Nil(t, err2)

	err3 := param.Save("RemoveFloat", "1.234")
	assert.Nil(t, err3)

	err4 := param.Remove("RemoveInt")
	assert.Nil(t, err4)

	err5 := param.Remove("RemoveString")
	assert.Nil(t, err5)

	err6 := param.Remove("RemoveFloat")
	assert.Nil(t, err6)
}

func TestGlobalParamsTable_LoadYaml(t *testing.T) {
	err := param.LoadYaml("config.yaml")
	assert.Nil(t, err)

	value1, err1 := param.Load("etcd.address")
	value2, err2 := param.Load("pulsar.port")
	value3, err3 := param.Load("reader.topicend")
	value4, err4 := param.Load("proxy.pulsarTopics.readerTopicPrefix")
	value5, err5 := param.Load("proxy.network.address")

	assert.Equal(t, value1, "localhost")
	assert.Equal(t, value2, "6650")
	assert.Equal(t, value3, "128")
	assert.Equal(t, value4, "milvusReader")
	assert.Equal(t, value5, "0.0.0.0")

	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Nil(t, err3)
	assert.Nil(t, err4)
	assert.Nil(t, err5)
}
