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
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func isSameKVPair(pair1, pair2 *commonpb.KeyValuePair) bool {
	return pair1.Key == pair2.Key && pair1.Value == pair2.Value
}

func isSameKVPairWrapper(pair1, pair2 interface{}) bool {
	return isSameKVPair(pair1.(*commonpb.KeyValuePair), pair2.(*commonpb.KeyValuePair))
}

func canBeMerged(ignoreTravelTs, ignoreGuaranteeTs bool, reqs ...*milvuspb.SearchRequest) bool {
	if len(reqs) <= 0 {
		return true
	}

	req := reqs[0]
	for idx := range reqs {
		if idx == 0 {
			continue
		}

		if reqs[idx].DbName != req.DbName {
			return false
		}

		if reqs[idx].CollectionName != req.CollectionName {
			return false
		}

		// why not SliceSetEqual? just serve for experiment, the order is certainly same.
		if !funcutil.SortedSliceEqual(reqs[idx].PartitionNames, req.PartitionNames) {
			return false
		}

		if reqs[idx].Dsl != req.Dsl {
			return false
		}

		if reqs[idx].DslType != req.DslType {
			return false
		}

		// need to check PlaceholderGroup?
		// seriously yes.
		// merging multiple search requests means we need to decode multiple placeholders,
		// and then assemble these placeholders and then encode it.

		// why not SliceSetEqual? just serve for experiment, the order is certainly same.
		if !funcutil.SortedSliceEqual(reqs[idx].SearchParams, req.SearchParams, isSameKVPairWrapper) {
			return false
		}

		if !ignoreTravelTs && reqs[idx].TravelTimestamp != req.TravelTimestamp {
			return false
		}

		if !ignoreGuaranteeTs && reqs[idx].GuaranteeTimestamp != req.GuaranteeTimestamp {
			return false
		}
	}

	return true
}

func mergeMultipleSearchRequests(reqs ...*milvuspb.SearchRequest) *milvuspb.SearchRequest {
	if len(reqs) <= 0 {
		return nil
	}

	if len(reqs) == 1 {
		reqs[0].Merged = false
		return reqs[0]
	}

	req := reqs[0]
	req.Merged = true

	var err error
	var placeholderGroup milvuspb.PlaceholderGroup
	if err = proto.Unmarshal(req.PlaceholderGroup, &placeholderGroup); err != nil {
		return nil
	}
	req.Nqs = []int64{int64(len(placeholderGroup.Placeholders[0].Values))}
	req.MsgIds = []int64{req.Base.MsgID}

	for i := 1; i < len(reqs); i++ {
		var anotherPlg milvuspb.PlaceholderGroup
		if err := proto.Unmarshal(reqs[i].PlaceholderGroup, &anotherPlg); err != nil {
			return nil
		}
		// just serving for experiment, no need to check other fields.
		// directly append.
		if len(placeholderGroup.Placeholders) != len(anotherPlg.Placeholders) {
			return nil
		}
		for j := range placeholderGroup.Placeholders {
			placeholderGroup.Placeholders[j].Values = append(placeholderGroup.Placeholders[j].Values, anotherPlg.Placeholders[j].Values...)
		}
		req.Nqs = append(req.Nqs, int64(len(anotherPlg.Placeholders[0].Values)))
		req.MsgIds = append(req.MsgIds, reqs[i].Base.MsgID)
	}

	req.PlaceholderGroup, err = proto.Marshal(&placeholderGroup)
	if err != nil {
		return nil
	}

	return req
}
