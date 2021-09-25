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

package proxy

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/stretchr/testify/assert"

	"github.com/jarcoal/httpmock"
)

func TestGetPulsarConfig(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	runtimeConfig := make(map[string]interface{})
	runtimeConfig[PulsarMaxMessageSizeKey] = strconv.FormatInt(5*1024*1024, 10)

	protocol := "http"
	ip := "pulsar"
	port := "18080"
	url := "/admin/v2/brokers/configuration/runtime"
	httpmock.RegisterResponder("GET", protocol+"://"+ip+":"+port+url,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, runtimeConfig)
		},
	)

	ret, err := GetPulsarConfig(protocol, ip, port, url)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(ret), len(runtimeConfig))
	assert.Equal(t, len(ret), 1)
	for key, value := range ret {
		assert.Equal(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", runtimeConfig[key]))
	}
}

func TestGetAttrByKeyFromRepeatedKV(t *testing.T) {
	kvs := []*commonpb.KeyValuePair{
		{Key: "Key1", Value: "Value1"},
		{Key: "Key2", Value: "Value2"},
		{Key: "Key3", Value: "Value3"},
	}

	cases := []struct {
		key      string
		kvs      []*commonpb.KeyValuePair
		value    string
		errIsNil bool
	}{
		{"Key1", kvs, "Value1", true},
		{"Key2", kvs, "Value2", true},
		{"Key3", kvs, "Value3", true},
		{"other", kvs, "", false},
	}

	for _, test := range cases {
		value, err := GetAttrByKeyFromRepeatedKV(test.key, test.kvs)
		assert.Equal(t, test.value, value)
		assert.Equal(t, test.errIsNil, err == nil)
	}
}

func TestUtil(t *testing.T) {
	a, b := 1, 2
	t.Run("getMax", func(t *testing.T) {
		ans := getMax(a, b)
		assert.Equal(t, b, ans)

		ans = getMax(b, a)
		assert.Equal(t, b, ans)
	})

	t.Run("getMin", func(t *testing.T) {
		ans := getMin(a, b)
		assert.Equal(t, a, ans)

		ans = getMin(b, a)
		assert.Equal(t, a, ans)
	})
}

func TestGetPulsarConfig_Error(t *testing.T) {
	protocol := "http"
	ip := "pulsar"
	port := "17777"
	url := "/admin/v2/brokers/configuration/runtime"

	ret, err := GetPulsarConfig(protocol, ip, port, url, 1, 1)
	assert.NotNil(t, err)
	assert.Nil(t, ret)
}
