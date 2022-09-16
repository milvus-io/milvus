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

package typeutil

import (
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestCompareIndexParams(t *testing.T) {
	cases := []*struct {
		param1 []*commonpb.KeyValuePair
		param2 []*commonpb.KeyValuePair
		result bool
	}{
		{param1: nil, param2: nil, result: true},
		{param1: nil, param2: []*commonpb.KeyValuePair{}, result: false},
		{param1: []*commonpb.KeyValuePair{}, param2: []*commonpb.KeyValuePair{}, result: true},
		{param1: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, param2: []*commonpb.KeyValuePair{}, result: false},
		{param1: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, param2: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, result: true},
	}

	for _, testcase := range cases {
		assert.EqualValues(t, testcase.result, CompareIndexParams(testcase.param1, testcase.param2))
	}
}
