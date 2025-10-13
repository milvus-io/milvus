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

package info

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestGetSdkTypeByUserAgent(t *testing.T) {
	_, ok := getSdkTypeByUserAgent([]string{})
	assert.False(t, ok)

	sdk, ok := getSdkTypeByUserAgent([]string{"grpc-node-js.test"})
	assert.True(t, ok)
	assert.Equal(t, "nodejs", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-python.test"})
	assert.True(t, ok)
	assert.Equal(t, "Python", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-go.test"})
	assert.True(t, ok)
	assert.Equal(t, "Golang", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-java.test"})
	assert.True(t, ok)
	assert.Equal(t, "Java", sdk)

	_, ok = getSdkTypeByUserAgent([]string{"invalid_type"})
	assert.False(t, ok)
}

func TestGetLengthFromTemplateValue(t *testing.T) {
	t.Run("bool_array", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false},
						},
					},
				},
			},
		}

		assert.Equal(t, 2, getLengthFromTemplateValue(tv))
	})

	t.Run("string_array", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"foo", "bar"},
						},
					},
				},
			},
		}

		assert.Equal(t, 2, getLengthFromTemplateValue(tv))
	})

	t.Run("long_array", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{0, 1},
						},
					},
				},
			},
		}

		assert.Equal(t, 2, getLengthFromTemplateValue(tv))
	})

	t.Run("double_array", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{0, 1},
						},
					},
				},
			},
		}

		assert.Equal(t, 2, getLengthFromTemplateValue(tv))
	})

	t.Run("json_array", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: [][]byte{[]byte("{}"), []byte("{}")},
						},
					},
				},
			},
		}

		assert.Equal(t, 2, getLengthFromTemplateValue(tv))
	})

	t.Run("nil", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{},
			},
		}

		assert.Equal(t, -1, getLengthFromTemplateValue(tv))
	})

	t.Run("primitive", func(t *testing.T) {
		tv := &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_StringVal{
				StringVal: "foo",
			},
		}

		assert.Equal(t, 1, getLengthFromTemplateValue(tv))
	})
}
