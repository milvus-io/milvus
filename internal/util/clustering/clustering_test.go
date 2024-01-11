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

package clustering

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestClusteringSearchOptionsParse(t *testing.T) {
	kv := []*commonpb.KeyValuePair{
		{
			Key:   SearchClusteringFilterRatio,
			Value: "0.3",
		},
	}

	options, err := SearchClusteringOptions(kv)
	assert.NoError(t, err)
	assert.Equal(t, float32(0.3), options.GetFilterRatio())

	kv2 := []*commonpb.KeyValuePair{
		{
			Key:   SearchClusteringFilterRatio,
			Value: "1",
		},
	}
	options2, err := SearchClusteringOptions(kv2)
	assert.NoError(t, err)
	assert.Equal(t, float32(1), options2.GetFilterRatio())

	kv3 := []*commonpb.KeyValuePair{
		{
			Key:   SearchClusteringFilterRatio,
			Value: "3",
		},
	}
	_, err = SearchClusteringOptions(kv3)
	assert.Error(t, err)

	kv4 := []*commonpb.KeyValuePair{
		{
			Key:   SearchClusteringFilterRatio,
			Value: "1.2",
		},
	}
	_, err = SearchClusteringOptions(kv4)
	assert.Error(t, err)

	kv5 := []*commonpb.KeyValuePair{
		{
			Key:   SearchClusteringFilterRatio,
			Value: "abc",
		},
	}
	_, err = SearchClusteringOptions(kv5)
	assert.Error(t, err)
}
