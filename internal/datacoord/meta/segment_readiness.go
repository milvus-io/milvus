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

package meta

import (
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentReadiness is the shared write-time readiness predicate for segment
// change staging (design §5.3). It is prepared once per collection to cache the
// schema-derived index / sort / stats / schema requirements, then evaluates a
// candidate member against the same sources of truth the async pipeline uses:
//
//   - data: the segment is Flushed/Flushing with a data footprint (binlogs or a
//     V3 manifest);
//   - index: every target index on the collection is Finished for the segment,
//     unless the collection has no index requirement or the small sorted-leaf
//     exemption applies;
//   - sort: the segment carries the sorted flag when sort compaction is enabled
//     and the collection is not external;
//   - stats: text / JSON-key stats have been written back (the same needDo*
//     predicates the statsInspector uses to schedule tasks);
//   - schema: the segment's SchemaVersion is not behind the collection's.
//
// A segment is "ready" when every term passes. Ready never reads IsInvisible:
// staged members stay IsInvisible=true through STAGED→READY (the flip happens
// at publish), so visibility must not gate readiness. The predicate is safe for
// concurrent evaluation: indexMeta guards itself, and the prepared schema
// fields are immutable.
type segmentReadiness struct {
	collectionID int64
	collection   *CollectionInfo

	// Index term.
	HasIndex       bool
	TargetFieldIDs []int64

	// Sort term: requires the sorted flag when enableSortCompaction() and the
	// collection is not external.
	RequireSorted bool

	// Stats term.
	RequireTextStats     bool
	textStatsFieldIDs    []int64
	RequireJSONKeyStats  bool
	jsonKeyStatsFieldIDs []int64

	// Schema term.
	SchemaVersion int32

	// Shared source of truth for the index term.
	indexMeta            SegmentIndexCatalog
	minRowsToEnableIndex int64
}

// prepareReadiness builds the per-collection readiness predicate. It returns
// nil when the collection is unknown or schema-less; callers (the
// groupInspector and the legacy read-time compatibility path) must treat a nil
// predicate as "no group staged for this collection" and keep the group-less
// visibility rules.
func (m *Meta) PrepareReadiness(collection *CollectionInfo) *segmentReadiness {
	if collection == nil || collection.Schema == nil {
		return nil
	}
	r := &segmentReadiness{
		collectionID:         collection.ID,
		collection:           collection,
		indexMeta:            m.IndexCatalog,
		SchemaVersion:        collection.Schema.GetVersion(),
		RequireSorted:        EnableSortCompaction() && !collection.IsExternal(),
		minRowsToEnableIndex: Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64(),
	}

	vectorFieldIDs := make([]UniqueID, 0, len(collection.Schema.GetFields()))
	for _, field := range collection.Schema.GetFields() {
		if typeutil.IsVectorType(field.GetDataType()) {
			vectorFieldIDs = append(vectorFieldIDs, field.GetFieldID())
		}
		h := typeutil.CreateFieldSchemaHelper(field)
		if h.EnableMatch() {
			r.textStatsFieldIDs = append(r.textStatsFieldIDs, field.GetFieldID())
		}
		if h.EnableJSONKeyStatsIndex() && Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() {
			r.jsonKeyStatsFieldIDs = append(r.jsonKeyStatsFieldIDs, field.GetFieldID())
		}
	}
	r.RequireTextStats = len(r.textStatsFieldIDs) > 0
	r.RequireJSONKeyStats = len(r.jsonKeyStatsFieldIDs) > 0

	// Index term: all vector fields must be indexed; when DVForceAllIndexReady
	// every scalar-indexed field must be too (the same target set as
	// FilterInIndexedSegments). A pure-scalar collection leaves the vector set
	// empty: the term would otherwise be vacuously satisfied, so degrade to
	// "every scalar index on the collection is Finished" (design §5.3, O3).
	if m.IndexCatalog != nil && m.IndexCatalog.HasIndex(collection.ID) {
		r.HasIndex = true
		r.TargetFieldIDs = append(r.TargetFieldIDs, vectorFieldIDs...)
		if len(vectorFieldIDs) == 0 || Params.DataCoordCfg.DVForceAllIndexReady.GetAsBool() {
			for _, index := range m.IndexCatalog.GetIndexesForCollection(collection.ID, "") {
				r.TargetFieldIDs = append(r.TargetFieldIDs, index.FieldID)
			}
		}
	}
	return r
}

// Ready reports whether the segment satisfies every readiness term. A nil
// segment is never ready.
func (r *segmentReadiness) Ready(segment *SegmentInfo) bool {
	if segment == nil {
		return false
	}
	return r.DataReady(segment) &&
		r.IndexReady(segment) &&
		r.SortReady(segment) &&
		r.StatsReady(segment) &&
		r.SchemaReady(segment)
}

// Predicate returns the readiness evaluation as a function, the form the
// legacy read-time compatibility path consumes (design §Implementation Notes:
// segmentReadiness.Prepare(collection) -> func(segment) bool).
func (r *segmentReadiness) Predicate() func(*SegmentInfo) bool {
	return r.Ready
}

// dataReady requires the segment to be in a flush state with a data footprint.
// It deliberately does NOT require !IsInvisible: staged members are still
// invisible through STAGED→READY and the flip happens at publish.
func (r *segmentReadiness) DataReady(segment *SegmentInfo) bool {
	if !IsFlushState(segment.GetState()) {
		return false
	}
	return len(segment.GetBinlogs()) > 0 || segment.GetManifestPath() != ""
}

// indexReady requires every target index Finished unless the collection has no
// index requirement or the small sorted leaf is exempt. The no-index exemption
// uses the "skipNoIndexCollection=true" semantics (on a no-index collection a
// child may be published as soon as its data is complete), a behavior fix over
// the read-time FilterInIndexedSegments fallback (design §5.3).
func (r *segmentReadiness) IndexReady(segment *SegmentInfo) bool {
	if !r.HasIndex {
		return true
	}
	if r.segmentIndexed(segment) {
		return true
	}
	return r.smallExempt(segment)
}

// segmentIndexed reports whether every target index on the collection is in
// Finished state for this segment.
func (r *segmentReadiness) segmentIndexed(segment *SegmentInfo) bool {
	indexed := r.indexMeta.GetIndexedSegments(r.collectionID, []UniqueID{segment.GetID()}, r.TargetFieldIDs)
	return len(indexed) > 0
}

// smallExempt is the small sorted-leaf exemption: a sorted segment below the
// MinSegmentNumRowsToEnableIndex threshold may serve queries without an index
// (mirrors retrieveQuerySegments, handler.go).
func (r *segmentReadiness) smallExempt(segment *SegmentInfo) bool {
	return (segment.GetIsSorted() || segment.GetIsSortedByNamespace()) &&
		segment.GetNumOfRows() < r.minRowsToEnableIndex
}

// sortReady requires the sorted flag when sort compaction is enabled for the
// collection and the collection is not external.
func (r *segmentReadiness) SortReady(segment *SegmentInfo) bool {
	if !r.RequireSorted {
		return true
	}
	return segment.GetIsSorted() || segment.GetIsSortedByNamespace()
}

// statsReady requires text / JSON-key stats to have converged when the
// collection declares the corresponding requirement. Convergence uses the same
// needDo* predicates the statsInspector uses to schedule tasks, so write-time
// readiness can never drift from what the pipeline actually produces.
func (r *segmentReadiness) StatsReady(segment *SegmentInfo) bool {
	if !r.RequireTextStats && !r.RequireJSONKeyStats {
		return true
	}
	allowUnsorted := r.collection.IsExternal()
	if r.RequireTextStats && NeedDoTextIndex(segment, r.textStatsFieldIDs, allowUnsorted) {
		return false
	}
	if r.RequireJSONKeyStats && !r.jsonKeyStatsConverged(segment, allowUnsorted) {
		return false
	}
	return true
}

// jsonKeyStatsConverged reports whether the JSON-key stats requirement is
// satisfied for the segment. External collections only build JSON-key stats on
// V3 manifest segments; a non-buildable segment never gets a task (mirroring
// statsInspector's capability gate) and is therefore exempt.
func (r *segmentReadiness) jsonKeyStatsConverged(segment *SegmentInfo, allowUnsorted bool) bool {
	if r.collection.IsExternal() && !CanBuildExternalJSONKeyIndex(segment) {
		return true
	}
	return !NeedDoJSONKeyIndex(segment, r.jsonKeyStatsFieldIDs, allowUnsorted)
}

// schemaReady requires the segment to carry the collection's current schema
// version. A staged member behind the collection version can never catch up
// (schema-bump materialization only targets visible segments, design C23), so
// the groupInspector must treat a persistent schema miss as FAILED/ABORTED
// rather than wait; this predicate only reports the miss.
func (r *segmentReadiness) SchemaReady(segment *SegmentInfo) bool {
	return segment.GetSchemaVersion() >= r.SchemaVersion
}
