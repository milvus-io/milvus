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
)

var Params = BaseTable{}

func TestMain(m *testing.M) {
	Params.Init()
	code := m.Run()
	os.Exit(code)
}

//func TestMain

func TestGlobalParamsTable_SaveAndLoad(t *testing.T) {
	err1 := Params.Save("int", "10")
	assert.Nil(t, err1)

	err2 := Params.Save("string", "testSaveAndLoad")
	assert.Nil(t, err2)

	err3 := Params.Save("float", "1.234")
	assert.Nil(t, err3)

	r1, _ := Params.Load("int")
	assert.Equal(t, "10", r1)

	r2, _ := Params.Load("string")
	assert.Equal(t, "testSaveAndLoad", r2)

	r3, _ := Params.Load("float")
	assert.Equal(t, "1.234", r3)

	err4 := Params.Remove("int")
	assert.Nil(t, err4)

	err5 := Params.Remove("string")
	assert.Nil(t, err5)

	err6 := Params.Remove("float")
	assert.Nil(t, err6)
}

func TestGlobalParamsTable_LoadRange(t *testing.T) {
	_ = Params.Save("xxxaab", "10")
	_ = Params.Save("xxxfghz", "20")
	_ = Params.Save("xxxbcde", "1.1")
	_ = Params.Save("xxxabcd", "testSaveAndLoad")
	_ = Params.Save("xxxzhi", "12")

	keys, values, err := Params.LoadRange("xxxa", "xxxg", 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(keys))
	assert.Equal(t, "10", values[0])
	assert.Equal(t, "testSaveAndLoad", values[1])
	assert.Equal(t, "1.1", values[2])
	assert.Equal(t, "20", values[3])

	_ = Params.Remove("abc")
	_ = Params.Remove("fghz")
	_ = Params.Remove("bcde")
	_ = Params.Remove("abcd")
	_ = Params.Remove("zhi")
}

func TestGlobalParamsTable_Remove(t *testing.T) {
	err1 := Params.Save("RemoveInt", "10")
	assert.Nil(t, err1)

	err2 := Params.Save("RemoveString", "testRemove")
	assert.Nil(t, err2)

	err3 := Params.Save("RemoveFloat", "1.234")
	assert.Nil(t, err3)

	err4 := Params.Remove("RemoveInt")
	assert.Nil(t, err4)

	err5 := Params.Remove("RemoveString")
	assert.Nil(t, err5)

	err6 := Params.Remove("RemoveFloat")
	assert.Nil(t, err6)
}

func TestGlobalParamsTable_LoadYaml(t *testing.T) {
	err := Params.LoadYaml("milvus.yaml")
	assert.Nil(t, err)

	err = Params.LoadYaml("advanced/channel.yaml")
	assert.Nil(t, err)

	_, err = Params.Load("etcd.address")
	assert.Nil(t, err)
	_, err = Params.Load("pulsar.port")
	assert.Nil(t, err)
	_, err = Params.Load("msgChannel.channelRange.insert")
	assert.Nil(t, err)

}
