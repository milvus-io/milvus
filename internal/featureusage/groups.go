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

// Package featureusage computes the feature usage report described in
// docs/design-docs/design_docs/20260902-feature-usage-reporting.md.
//
// Static statistics are recomputed from coordinator metadata on every call
// and hold no state. Request counters are a fixed-size array of atomic
// values whose index set is a compile-time constant; nothing in this package
// is keyed by collection, user, or time, so nothing needs cleanup.
//
// Every string that reaches an output entry is drawn from a code-defined set:
// an enum name, an official key from pkg/common, a bucket label, or a counter
// name. User-controlled strings are folded into the "_custom" / "_other"
// entries and are never emitted.
package featureusage

// Groups of the report. The meaning of FeatureEntry.value depends on the group.
const (
	// GroupFieldTypes: collections with at least one field of this DataType.
	GroupFieldTypes = "field_types"
	// GroupIndexTypes: collections with at least one index of this index_type.
	GroupIndexTypes = "index_types"
	// GroupMetricTypes: collections with at least one index of this metric_type.
	GroupMetricTypes = "metric_types"
	// GroupFunctions: collections with at least one function of this FunctionType.
	GroupFunctions = "functions"
	// GroupProviders: collections with at least one embedding or rerank function of this provider.
	GroupProviders = "providers"
	// GroupDeclared: collections for which a hand-written predicate holds.
	GroupDeclared = "declared"
	// GroupProperties: collections whose collection-level properties contain this key.
	GroupProperties = "properties"
	// GroupDBProperties: databases whose properties contain this key.
	GroupDBProperties = "db_properties"
	// GroupFieldParams: collections with at least one field whose type_params contain this key.
	GroupFieldParams = "field_params"
	// GroupIndexParams: collections with at least one index whose user index params contain this key.
	GroupIndexParams = "index_params"
	// GroupObjects: count of objects of this kind in the instance.
	GroupObjects = "objects"
	// GroupDist: collections falling into bucket for this quantity.
	GroupDist = "dist"
	// GroupSegment: collections with at least one live segment carrying this materialized trait.
	GroupSegment = "segment"
	// GroupConfig: node-level configuration items in effect on the reporting node, as key=true/false.
	GroupConfig = "config"
	// GroupLoaded: what QueryCoord currently has loaded (collections, partial load, resource groups).
	GroupLoaded = "loaded"
	// GroupRequest: monotonic count of requests using this feature since node start.
	GroupRequest = "request"
)

// AllGroups is every group the report can carry, in report order. The
// consumer switches on this set, so it is enumerated here rather than left
// implicit in the constants above; TestReportSurfaceIsStable fails when it
// drifts from the golden surface file.
func AllGroups() []string {
	return []string{
		GroupFieldTypes,
		GroupIndexTypes,
		GroupMetricTypes,
		GroupFunctions,
		GroupProviders,
		GroupDeclared,
		GroupProperties,
		GroupDBProperties,
		GroupFieldParams,
		GroupIndexParams,
		GroupObjects,
		GroupDist,
		GroupSegment,
		GroupLoaded,
		GroupConfig,
		GroupRequest,
	}
}

// Names shared by several groups.
const (
	// CustomKey is the single entry that every non-official key folds into.
	CustomKey = "_custom"
	// OtherValue is the single entry that every unrecognized value folds into.
	OtherValue = "_other"
	// RerankProviderPrefix keeps rerank providers apart from embedding providers
	// inside GroupProviders.
	RerankProviderPrefix = "rerank:"
)
