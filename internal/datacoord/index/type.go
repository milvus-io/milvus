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

package index

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func GetIndexParam(indexParams []*commonpb.KeyValuePair, key string) (string, error) {
	for _, p := range indexParams {
		if p.Key == key {
			return p.Value, nil
		}
	}
	return "", merr.WrapErrParameterInvalidMsg("%s not found", key)
}

func isNoTrainIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsNoTrainIndex(indexType)
}

func isMvSupported(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsMvSupported(indexType)
}

func isDiskANNIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType)
}

func DeleteParams(from []*commonpb.KeyValuePair, deletes []string) []*commonpb.KeyValuePair {
	params := make(map[string]string)
	for _, param := range from {
		params[param.GetKey()] = param.GetValue()
	}

	// delete the params
	for _, key := range deletes {
		delete(params, key)
	}

	return lo.MapToSlice(params, func(k string, v string) *commonpb.KeyValuePair {
		return &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		}
	})
}
