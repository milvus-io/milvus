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
//

package clustering

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestParseClusteringInfo(t *testing.T) {
	kv := []*commonpb.KeyValuePair{
		{
			Key:   CLUSTERING_CENTROID,
			Value: "[1.0,2.0,3.0,4.0,5.0]",
		},
		{
			Key:   CLUSTERING_SIZE,
			Value: "10000",
		},
		{
			Key:   CLUSTERING_ID,
			Value: "30000",
		},
		{
			Key:   CLUSTERING_GROUPID,
			Value: "60000",
		},
	}

	cluster, err := ClusteringInfoFromKV(kv)
	assert.NoError(t, err)
	assert.Equal(t, int64(10000), cluster.Size)
	assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0, 5.0}, cluster.Centroid)
	assert.Equal(t, int64(30000), cluster.Id)
	assert.Equal(t, int64(60000), cluster.GroupID)
}

func TestParseInvalidClusteringInfo(t *testing.T) {
	kv2 := []*commonpb.KeyValuePair{
		{
			Key:   "other key",
			Value: "[1.0,2.0,3.0,4.0,5.0]",
		},
		{
			Key:   CLUSTERING_SIZE,
			Value: "10000",
		},
	}
	cluster2, err := ClusteringInfoFromKV(kv2)
	assert.NoError(t, err)
	assert.Nil(t, cluster2)

	kv3 := []*commonpb.KeyValuePair{
		{
			Key:   CLUSTERING_CENTROID,
			Value: "abcdefg",
		},
		{
			Key:   CLUSTERING_SIZE,
			Value: "10000",
		},
	}
	_, err = ClusteringInfoFromKV(kv3)
	assert.Error(t, err)

	kv4 := []*commonpb.KeyValuePair{
		{
			Key:   CLUSTERING_CENTROID,
			Value: "[1.0,2.0,3.0,4.0,5.0]",
		},
		{
			Key:   CLUSTERING_SIZE,
			Value: "10000.45",
		},
	}
	_, err = ClusteringInfoFromKV(kv4)
	assert.Error(t, err)
}

func TestClusteringOptionsParse(t *testing.T) {
	kv := []*commonpb.KeyValuePair{
		{
			Key:   SEARCH_ENABLE_CLUSTERING,
			Value: "true",
		},
		{
			Key:   SEARCH_CLUSTERING_FILTER_RATIO,
			Value: "0.3",
		},
	}

	options, err := SearchClusteringOptions(kv)
	assert.NoError(t, err)
	assert.Equal(t, true, options.Enable)
	assert.Equal(t, float32(0.3), options.FilterRate)

	kv2 := []*commonpb.KeyValuePair{
		{
			Key:   SEARCH_ENABLE_CLUSTERING,
			Value: "true2",
		},
		{
			Key:   SEARCH_CLUSTERING_FILTER_RATIO,
			Value: "0.3",
		},
	}
	options2, err := SearchClusteringOptions(kv2)
	assert.Error(t, err)
	assert.Nil(t, options2)

	kv3 := []*commonpb.KeyValuePair{
		{
			Key:   SEARCH_ENABLE_CLUSTERING,
			Value: "true",
		},
		{
			Key:   SEARCH_CLUSTERING_FILTER_RATIO,
			Value: "3",
		},
	}
	_, err = SearchClusteringOptions(kv3)
	assert.Error(t, err)

	kv4 := []*commonpb.KeyValuePair{
		{
			Key:   SEARCH_ENABLE_CLUSTERING,
			Value: "true",
		},
		{
			Key:   SEARCH_CLUSTERING_FILTER_RATIO,
			Value: "1.2",
		},
	}
	_, err = SearchClusteringOptions(kv4)
	assert.Error(t, err)
}
