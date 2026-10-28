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

package datacoord

import (
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentReadiness is the shared write-time readiness predicate for segment
// change staging (design §5.3). It is prepared once per collection to cache the
// schema-derived index / sort / stats / schema requirements, then evaluates a
// candidate member against the same sources of truth the async pipeline uses:
//
//   - data: the segment has rows and is Flushed/Flushing with a footprint (binlogs or a
//     V3 manifest);
//   - index: every target index on the collection is Finished for the segment,
//     unless the collection has no index requirement or the small sorted-leaf
//     exemption applies;
//   - sort: the segment carries the sorted flag when sort compaction is enabled
//     and the collection is not external;
//   - stats: text / JSON-key stats have been written back (the same needDo*
//     artifact checks also used by statsInspector, without scheduling gates);
//   - schema: the segment's SchemaVersion is not behind the collection's.
//
// A segment is "ready" when every term passes. Ready never reads IsInvisible:
// staged members stay IsInvisible=true through STAGED→READY (the flip happens
// at publish), so visibility must not gate readiness. The predicate is safe for
// concurrent evaluation: indexMeta guards itself, and the prepared schema
// fields are immutable.
type segmentReadiness struct {
	collectionID int64
	collection   *collectionInfo

	// Index term.
	hasIndex       bool
	targetFieldIDs []int64

	// Sort term: requires the sorted flag when enableSortCompaction() and the
	// collection is not external.
	requireSorted bool

	// Stats term.
	requireTextStats     bool
	textStatsFieldIDs    []int64
	requireJSONKeyStats  bool
	jsonKeyStatsFieldIDs []int64

	// Schema term.
	schemaVersion int32

	// Shared source of truth for the index term.
	indexMeta            *indexMeta
	minRowsToEnableIndex int64
}

// prepareReadiness builds the per-collection readiness predicate. It returns
// nil when the collection is unknown or schema-less; callers (the
// groupInspector and the legacy read-time compatibility path) must treat a nil
// predicate as "no group staged for this collection" and keep the group-less
// visibility rules.
func (m *meta) prepareReadiness(collection *collectionInfo) *segmentReadiness {
	if collection == nil || collection.Schema == nil {
		return nil
	}
	r := &segmentReadiness{
		collectionID:         collection.ID,
		collection:           collection,
		indexMeta:            m.indexMeta,
		schemaVersion:        collection.Schema.GetVersion(),
		requireSorted:        enableSortCompaction() && !collection.IsExternal(),
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
	r.requireTextStats = len(r.textStatsFieldIDs) > 0
	r.requireJSONKeyStats = len(r.jsonKeyStatsFieldIDs) > 0

	// Index term: all vector fields must be indexed; when DVForceAllIndexReady
	// every scalar-indexed field must be too (the same target set as
	// FilterInIndexedSegments). A pure-scalar collection leaves the vector set
	// empty: the term would otherwise be vacuously satisfied, so degrade to
	// "every scalar index on the collection is Finished" (design §5.3, O3).
	if m.indexMeta != nil && m.indexMeta.HasIndex(collection.ID) {
		r.hasIndex = true
		r.targetFieldIDs = append(r.targetFieldIDs, vectorFieldIDs...)
		if len(vectorFieldIDs) == 0 || Params.DataCoordCfg.DVForceAllIndexReady.GetAsBool() {
			for _, index := range m.indexMeta.GetIndexesForCollection(collection.ID, "") {
				r.targetFieldIDs = append(r.targetFieldIDs, index.FieldID)
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
	return r.dataReady(segment) &&
		r.indexReady(segment) &&
		r.sortReady(segment) &&
		r.statsReady(segment) &&
		r.schemaReady(segment)
}

// Predicate returns the readiness evaluation as a function, the form the
// legacy read-time compatibility path consumes (design §Implementation Notes:
// segmentReadiness.Prepare(collection) -> func(segment) bool).
func (r *segmentReadiness) Predicate() func(*SegmentInfo) bool {
	return r.Ready
}

// dataReady requires a nonempty segment in a flush state with a data footprint.
// It deliberately does NOT require !IsInvisible: staged members are still
// invisible through STAGED→READY and the flip happens at publish.
func (r *segmentReadiness) dataReady(segment *SegmentInfo) bool {
	if !isFlushState(segment.GetState()) || segment.GetNumOfRows() <= 0 {
		return false
	}
	return len(segment.GetBinlogs()) > 0 || segment.GetManifestPath() != ""
}

// indexReady requires every target index Finished unless the collection has no
// index requirement or the small sorted leaf is exempt. The no-index exemption
// uses the "skipNoIndexCollection=true" semantics (on a no-index collection a
// child may be published as soon as its data is complete), a behavior fix over
// the read-time FilterInIndexedSegments fallback (design §5.3).
func (r *segmentReadiness) indexReady(segment *SegmentInfo) bool {
	if !r.hasIndex {
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
	indexed := r.indexMeta.GetIndexedSegments(r.collectionID, []UniqueID{segment.GetID()}, r.targetFieldIDs)
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
func (r *segmentReadiness) sortReady(segment *SegmentInfo) bool {
	if !r.requireSorted {
		return true
	}
	return segment.GetIsSorted() || segment.GetIsSortedByNamespace()
}

// statsReady checks produced artifacts independently of scheduling eligibility.
// An unsorted segment that the stats scheduler skips is still missing stats.
func (r *segmentReadiness) statsReady(segment *SegmentInfo) bool {
	if !r.requireTextStats && !r.requireJSONKeyStats {
		return true
	}
	if r.requireTextStats {
		if !hasRequiredTextStats(segment, r.textStatsFieldIDs) {
			return false
		}
	}
	if r.requireJSONKeyStats && !r.jsonKeyStatsConverged(segment) {
		return false
	}
	return true
}

// jsonKeyStatsConverged reports whether the JSON-key stats requirement is
// satisfied for the segment. External collections only build JSON-key stats on
// V3 manifest segments; a non-buildable segment never gets a task (mirroring
// statsInspector's capability gate) and is therefore exempt.
func (r *segmentReadiness) jsonKeyStatsConverged(segment *SegmentInfo) bool {
	if r.collection.IsExternal() && !canBuildExternalJSONKeyIndex(segment) {
		return true
	}
	return hasRequiredJSONKeyStats(segment, r.jsonKeyStatsFieldIDs)
}

// schemaReady requires the segment to carry the collection's current schema
// version. A staged member behind the collection version can never catch up
// (schema-bump materialization only targets visible segments, design C23), so
// the groupInspector must treat a persistent schema miss as FAILED/ABORTED
// rather than wait; this predicate only reports the miss.
func (r *segmentReadiness) schemaReady(segment *SegmentInfo) bool {
	return segment.GetSchemaVersion() >= r.schemaVersion
}
