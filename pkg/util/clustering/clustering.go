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
	"encoding/json"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

const (
	CLUSTERING_CENTROID = "clustering.centroid"
	CLUSTERING_SIZE     = "clustering.size"
	CLUSTERING_ID       = "clustering.id"
	CLUSTERING_GROUPID  = "clustering.groupID"

	SEARCH_ENABLE_CLUSTERING       = "clustering.enable"
	SEARCH_CLUSTERING_FILTER_RATIO = "clustering.filter_ratio"
)

func ClusteringInfoFromKV(kv []*commonpb.KeyValuePair) (*internalpb.ClusteringInfo, error) {
	kvMap := funcutil.KeyValuePair2Map(kv)
	if v, ok := kvMap[CLUSTERING_CENTROID]; ok {
		var floatSlice []float32
		err := json.Unmarshal([]byte(v), &floatSlice)
		if err != nil {
			log.Error("Failed to parse cluster center value:", zap.String("value", v), zap.Error(err))
			return nil, err
		}
		clusterInfo := &internalpb.ClusteringInfo{
			Centroid: floatSlice,
		}
		if sizeStr, ok := kvMap[CLUSTERING_SIZE]; ok {
			size, err := strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				log.Error("Failed to parse cluster size value:", zap.String("value", sizeStr), zap.Error(err))
				return nil, err
			}
			clusterInfo.Size = size
		}
		if clusterIDStr, ok := kvMap[CLUSTERING_ID]; ok {
			clusterID, err := strconv.ParseInt(clusterIDStr, 10, 64)
			if err != nil {
				log.Error("Failed to parse cluster id value:", zap.String("value", clusterIDStr), zap.Error(err))
				return nil, err
			}
			clusterInfo.Id = clusterID
		}
		if groupIDStr, ok := kvMap[CLUSTERING_GROUPID]; ok {
			groupID, err := strconv.ParseInt(groupIDStr, 10, 64)
			if err != nil {
				log.Error("Failed to parse cluster group id value:", zap.String("value", groupIDStr), zap.Error(err))
				return nil, err
			}
			clusterInfo.GroupID = groupID
		}
		return clusterInfo, nil
	}
	return nil, nil
}

func SearchClusteringOptions(kv []*commonpb.KeyValuePair) (*internalpb.SearchClusteringOptions, error) {
	kvMap := funcutil.KeyValuePair2Map(kv)

	clusteringOptions := &internalpb.SearchClusteringOptions{
		Enable:     false,
		FilterRate: 0.5, // default
	}

	if enable, ok := kvMap[SEARCH_ENABLE_CLUSTERING]; ok {
		b, err := strconv.ParseBool(enable)
		if err != nil {
			return nil, errors.New("illegal search params clustering.enable value, should be true or false")
		}
		clusteringOptions.Enable = b
	}

	if clusterBasedFilterRatio, ok := kvMap[SEARCH_CLUSTERING_FILTER_RATIO]; ok {
		b, err := strconv.ParseFloat(clusterBasedFilterRatio, 32)
		if err != nil {
			return nil, errors.New("illegal search params clustering.filter_ratio value, should be a float in range (0.0, 1.0]")
		}
		if b <= 0.0 || b > 1.0 {
			return nil, errors.New("invalid clustering.filter_ratio value, should be a float in range (0.0, 1.0]")
		}
		clusteringOptions.FilterRate = float32(b)
	}

	return clusteringOptions, nil
}
