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

package featureusage

import (
	"encoding/json"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// ComputeIndexEntries computes the static groups that derive from index
// metadata: index_types, metric_types, index_params, and the is_auto_index
// predicate of declared. Deleted indexes must be filtered by the caller.
//
// index_type and metric_type values are emitted verbatim: they are validated
// by indexparamcheck when the index is created and cannot otherwise reach the
// metadata, so they are a code-defined set even though this package does not
// enumerate them.
func ComputeIndexEntries(indexes []*model.Index) []*internalpb.FeatureEntry {
	c := newCollector()
	c.ensure(GroupDeclared, DeclaredIsAutoIndex, "")

	perCollection := make(map[int64]seen)
	seenFor := func(collID int64) seen {
		s, ok := perCollection[collID]
		if !ok {
			s = newSeen()
			perCollection[collID] = s
		}
		return s
	}

	for _, idx := range indexes {
		if idx == nil || idx.IsDeleted {
			continue
		}
		s := seenFor(idx.CollectionID)

		if t, ok := firstParam(idx, common.IndexTypeKey); ok && t != "" {
			c.addOnce(s, GroupIndexTypes, t, "")
		}
		if m, ok := firstParam(idx, common.MetricTypeKey); ok && m != "" {
			c.addOnce(s, GroupMetricTypes, m, "")
		}
		if idx.IsAutoIndex {
			c.addOnce(s, GroupDeclared, DeclaredIsAutoIndex, "")
		}
		for key, value := range flattenIndexParams(idx.UserIndexParams) {
			switch key {
			case common.IndexTypeKey, common.MetricTypeKey, common.ParamsKey:
				// Structural keys present on every index; they carry no signal.
				continue
			}
			c.addOnce(s, GroupIndexParams, keyValueEntryName(key, value), "")
		}
	}
	return c.entries()
}

// firstParam looks a key up in IndexParams first (the flattened, effective
// parameters) and falls back to UserIndexParams.
func firstParam(idx *model.Index, key string) (string, bool) {
	if v, ok := stringParam(idx.IndexParams, key); ok {
		return v, true
	}
	return stringParam(idx.UserIndexParams, key)
}

// flattenIndexParams returns the keys a user set on an index. SDKs send the
// index-specific parameters as a JSON object under the "params" key, so that
// object is opened one level and its keys are reported alongside the top-level
// ones. Values are kept only to decide the boolean split; they are never
// emitted.
func flattenIndexParams(kvs []*commonpb.KeyValuePair) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		if kv.GetKey() != common.ParamsKey {
			out[kv.GetKey()] = kv.GetValue()
			continue
		}
		var nested map[string]any
		if err := json.Unmarshal([]byte(kv.GetValue()), &nested); err != nil {
			continue
		}
		for k, v := range nested {
			switch t := v.(type) {
			case string:
				out[k] = t
			case bool:
				out[k] = fmt.Sprintf("%t", t)
			default:
				out[k] = ""
			}
		}
	}
	return out
}
