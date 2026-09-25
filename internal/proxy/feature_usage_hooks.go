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

package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/featureusage"
)

// recordUpsertFeatures counts, once per upsert request, how it updates rows:
// override (the whole row is replaced) or merge (partial_update, including a
// request that a non-REPLACE field op promoted to it), each partial-update
// operator it applies, and a bucket of how many fields it carries. Called
// after PreExecute has normalized partial_update, so the mode is the one the
// request runs with.
func recordUpsertFeatures(req *milvuspb.UpsertRequest) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	if req.GetPartialUpdate() {
		set.Set(featureusage.FeatureUpsertMerge)
	} else {
		set.Set(featureusage.FeatureUpsertOverride)
	}
	for _, op := range req.GetFieldOps() {
		set.Set(featureusage.FieldOpFeature(op.GetOp()))
	}
	set.Set(featureusage.UpsertFieldsFeature(len(req.GetFieldsData())))
	set.HitAll()
}

// recordDeleteMode counts whether a delete named its rows by primary key or
// by a filter the Proxy has to query first. A filter that is only a primary
// key match (pk in [...] or pk == x) is how the SDKs send delete(ids=...), and
// runs the same path, so it counts as ids.
func recordDeleteMode(byIDs bool) {
	if !featureusage.Enabled() {
		return
	}
	if byIDs {
		featureusage.Hit(featureusage.FeatureDeleteByIDs)
	} else {
		featureusage.Hit(featureusage.FeatureDeleteByFilter)
	}
}
