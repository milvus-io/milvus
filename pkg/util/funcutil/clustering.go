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

package funcutil

import (
	"encoding/json"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"

	"go.uber.org/zap"
)

const (
	CLUSTERING_CENTER = "clustering.center"
	CLUSTERING_SIZE   = "clustering.size"
)

func ClusteringInfoFromKV(kv []*commonpb.KeyValuePair) (*internalpb.ClusteringInfo, error) {
	kvMap := KeyValuePair2Map(kv)
	if v, ok := kvMap[CLUSTERING_CENTER]; ok {
		var floatSlice []float32
		err := json.Unmarshal([]byte(v), &floatSlice)
		if err != nil {
			log.Error("Failed to parse cluster center value:", zap.String("value", v), zap.Error(err))
			return nil, err
		}
		clusterInfo := &internalpb.ClusteringInfo{
			Center: floatSlice,
		}
		if sizeStr, ok := kvMap[CLUSTERING_SIZE]; ok {
			size, err := strconv.ParseInt(sizeStr, 10, 0)
			if err != nil {
				log.Error("Failed to parse cluster size value:", zap.String("value", sizeStr), zap.Error(err))
				return nil, err
			}
			clusterInfo.Size = size
		}
		return clusterInfo, nil
	}
	return nil, nil
}
