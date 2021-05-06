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

package proxynode

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

func TestCheckIntByRange(t *testing.T) {
	params := map[string]string{
		"1":  strconv.Itoa(1),
		"2":  strconv.Itoa(2),
		"3":  strconv.Itoa(3),
		"s1": "s1",
		"s2": "s2",
		"s3": "s3",
	}

	cases := []struct {
		params map[string]string
		key    string
		min    int
		max    int
		want   bool
	}{
		{params, "1", 0, 4, true},
		{params, "2", 0, 4, true},
		{params, "3", 0, 4, true},
		{params, "1", 4, 5, false},
		{params, "2", 4, 5, false},
		{params, "3", 4, 5, false},
		{params, "4", 0, 4, false},
		{params, "5", 0, 4, false},
		{params, "6", 0, 4, false},
		{params, "s1", 0, 4, false},
		{params, "s2", 0, 4, false},
		{params, "s3", 0, 4, false},
		{params, "s4", 0, 4, false},
		{params, "s5", 0, 4, false},
		{params, "s6", 0, 4, false},
	}

	for _, test := range cases {
		if got := CheckIntByRange(test.params, test.key, test.min, test.max); got != test.want {
			t.Errorf("CheckIntByRange(%v, %v, %v, %v) = %v", test.params, test.key, test.min, test.max, test.want)
		}
	}
}

func TestCheckStrByValues(t *testing.T) {
	params := map[string]string{
		"1": strconv.Itoa(1),
		"2": strconv.Itoa(2),
		"3": strconv.Itoa(3),
	}

	cases := []struct {
		params    map[string]string
		key       string
		container []string
		want      bool
	}{
		{params, "1", []string{"1", "2", "3"}, true},
		{params, "2", []string{"1", "2", "3"}, true},
		{params, "3", []string{"1", "2", "3"}, true},
		{params, "1", []string{"4", "5", "6"}, false},
		{params, "2", []string{"4", "5", "6"}, false},
		{params, "3", []string{"4", "5", "6"}, false},
		{params, "1", []string{}, false},
		{params, "2", []string{}, false},
		{params, "3", []string{}, false},
		{params, "4", []string{"1", "2", "3"}, false},
		{params, "5", []string{"1", "2", "3"}, false},
		{params, "6", []string{"1", "2", "3"}, false},
		{params, "4", []string{"4", "5", "6"}, false},
		{params, "5", []string{"4", "5", "6"}, false},
		{params, "6", []string{"4", "5", "6"}, false},
		{params, "4", []string{}, false},
		{params, "5", []string{}, false},
		{params, "6", []string{}, false},
	}

	for _, test := range cases {
		if got := CheckStrByValues(test.params, test.key, test.container); got != test.want {
			t.Errorf("CheckStrByValues(%v, %v, %v) = %v", test.params, test.key, test.container, test.want)
		}
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
