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

package coordinator

import (
	"context"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

// Readiness witness is version-based (no IsLoaded propagation, no index consulted).
// With global index-meta sync a backfilled field is correctly searchable once its DATA is
// loaded (segcore FieldAccessible reduces to HasFieldData; the index is a pure performance
// follow-up), so readiness is a per-segment check that the segment carries/loaded the
// version holding the round's columns.
//
// Watermark scope (internal add_function_field / external-collection refresh) is a
// two-layer witness -- both must be clear before the round reveals:
//  1. DataView (datacoord meta, live): no backfill-eligible segment may still be below the
//     watermark. Catches segments not yet loaded/reported at all (loading, pending
//     handoff), which are invisible to the read-side view.
//  2. SegmentDist (querynode dist report): no loaded segment below the watermark. Catches
//     loaded-but-not-yet-reopened old content -- target CONTENT leads dist (the segment
//     checker reopens a segment exactly when dist lags the target), so only dist proves
//     the reopen actually completed. dist is deliberately a SUPERSET of the searchable
//     set (it includes not-yet-promoted and pending-release copies): revoke is a one-way
//     release, so it must cover what MAY become searchable, not just what is.
//
// LOAD-BEARING ASSUMPTION (in place of a currentTarget layer): load/reopen requests
// resolve their segment info LIVE from the datacoord broker at dispatch time
// (querycoordv2 task executor getLoadInfo -> broker.GetSegmentInfo), so any load
// dispatched after the backfill apply lands with the post-apply content -- a segment
// absent from dist (crash / release-reload in flight) reloads fresh and needs no gating.
// If load info ever starts being served from target snapshots or a cache, that
// assumption breaks and a write-side-snapshot layer must be reintroduced. The residual
// (a load dispatched BEFORE the apply landing AFTER revoke) is accepted: the segment
// checker reopens it within a cycle and segcore FieldAccessible fails loudly meanwhile.
//
// Segment-list scope (external Spark) uses presence-required dist: each committed
// segment must be PRESENT in dist at >= its V_commit; a segment datacoord has since
// dropped (compacted away under the workflow's compaction contract, GC) is pruned -- it
// exited every view and can never report again. The DataView condition holds by
// construction (the round is registered in the manifest-apply ack callback, after
// datacoord meta reflects every per-segment V_commit).
//
// A not-yet-backfilled old segment still reports the pre-backfill version and holds the
// round; a new segment is born at/after V, so it never blocks (no liveness bug).

// SegmentDistInfo is a per-segment version row of the querynode dist report, holding
// only what the readiness witness needs.
type SegmentDistInfo struct {
	SegmentID     int64
	Version       int64  // numeric data version (storage v2 segment-list witness)
	ManifestPath  string // manifest path (storage v3 segment-list witness; encodes the version)
	SchemaVersion int32  // schema_version (watermark witness)
}

// DistReader exposes the querynode dist report for a collection. The concrete adapter
// over querycoord SegmentDistManager is wired in mixCoord.
type DistReader interface {
	SegmentDist(collectionID int64) []SegmentDistInfo
}

// DataViewReader exposes the datacoord segment meta (the write-side truth) to the
// witness. datacoord's Server satisfies it structurally; mixCoord wires it. It is
// REQUIRED -- a missing DataView would silently skip layer 1 and the segment-list
// pruning (fail-open); tests pass an empty fake instead of nil.
type DataViewReader interface {
	// StaleBackfillSegments returns the IDs of the collection's backfill-eligible
	// segments (healthy, flushed-side, non-L0) whose schema_version is strictly below
	// the watermark, i.e. the write side has not finished backfilling them.
	StaleBackfillSegments(ctx context.Context, collectionID int64, watermark int32) []int64
	// BackfillSegmentDropped reports whether the segment is no longer a healthy
	// datacoord segment (dropped by compaction/GC), so a segment-list round must stop
	// waiting for it.
	BackfillSegmentDropped(ctx context.Context, collectionID, segmentID int64) bool
}

// ReadinessProvider answers "can ALL in-scope segments correctly serve this round?",
// encapsulating the per-scope layered witness so the observer never sees
// watermark-vs-Spark distinctions.
type ReadinessProvider struct {
	dist     DistReader
	dataView DataViewReader
}

// NewReadinessProvider constructs a provider. Both readers are required.
func NewReadinessProvider(dist DistReader, dataView DataViewReader) *ReadinessProvider {
	if dist == nil || dataView == nil {
		panic("bump_defence readiness provider requires both a dist reader and a data-view reader")
	}
	return &ReadinessProvider{dist: dist, dataView: dataView}
}

// IsRoundReady reports whether every in-scope segment has loaded the version that carries
// the round's backfilled columns. A round reveals atomically — it returns false until
// every in-scope segment has reached that version.
func (p *ReadinessProvider) IsRoundReady(ctx context.Context, round *BackfillRound) bool {
	if round == nil || len(round.Fields) == 0 {
		return true
	}
	// dist entries are per (segment, node): a segment loaded on several nodes/replicas
	// contributes one entry per copy, and every copy must individually pass the witness.
	dist := p.dist.SegmentDist(round.CollectionID)
	byID := make(map[int64][]SegmentDistInfo, len(dist))
	for _, s := range dist {
		byID[s.SegmentID] = append(byID[s.SegmentID], s)
	}

	switch round.Scope.Kind {
	case ScopeWatermark:
		threshold := int64(round.Scope.Watermark)
		// Layer 1 -- DataView: the write side must have finished the backfill. A stale
		// segment here may be loading / pending handoff, invisible to the read-side
		// view below.
		if len(p.dataView.StaleBackfillSegments(ctx, round.CollectionID, round.Scope.Watermark)) > 0 {
			return false
		}
		// Layer 2 -- SegmentDist: every loaded segment must have reopened to
		// schema_version >= V (target content leads dist; only dist proves the reopen
		// completed). A new segment is born at/after V, so it never holds the round back.
		// A segment absent from dist (crash / release-reload in flight) needs no check:
		// its reload resolves fresh post-apply content from the datacoord broker at
		// dispatch time (see the load-bearing assumption in the header comment).
		for _, s := range dist {
			if !segmentReady(s, threshold, true) {
				return false
			}
		}
		return true
	case ScopeSegmentList:
		// Presence-required: each committed segment must be IN dist with EVERY copy
		// (across nodes/replicas) at >= its V_commit -- a fresh copy on one replica must
		// not mask a stale copy still serving on another. An absent segment (crash /
		// release-reload in flight) must keep blocking -- a pure intersection would pass
		// vacuously and revoke before a stale reload lands. The only exception is a
		// segment datacoord has dropped (compacted away under the workflow's compaction
		// contract, GC): it exited every view and can never report again, so waiting for
		// it would hang the round forever.
		for segID, vCommit := range round.Scope.Segments {
			if entries := byID[segID]; len(entries) > 0 && allCopiesReady(entries, vCommit) {
				continue
			}
			if p.dataView.BackfillSegmentDropped(ctx, round.CollectionID, segID) {
				continue
			}
			return false
		}
		return true
	default:
		return false
	}
}

// allCopiesReady reports whether every dist copy of one committed segment has reached its
// V_commit (segment-list scope).
func allCopiesReady(entries []SegmentDistInfo, vCommit int64) bool {
	for _, s := range entries {
		if !segmentReady(s, vCommit, false) {
			return false
		}
	}
	return true
}

// segmentReady reports whether one in-scope segment has loaded the version that carries
// the round's backfilled columns. With global index-meta sync this is the same for vector
// and scalar fields -- correctness only needs the data loaded, so the index is never
// consulted. useSchemaVersion selects the segment's schema_version (watermark scope) vs its
// manifest/data version (segment-list V_commit scope).
func segmentReady(s SegmentDistInfo, threshold int64, useSchemaVersion bool) bool {
	switch {
	case useSchemaVersion:
		// watermark scope (internal add_function_field / external-collection): the round's
		// columns are present once the segment reopened to schema_version >= V.
		return int64(s.SchemaVersion) >= threshold
	case s.ManifestPath != "":
		// segment-list scope, storage v3: the loaded manifest path encodes the loaded
		// version; the columns are present once it reaches V_commit. A malformed path is
		// treated as not-ready (conservative).
		_, loadedVer, err := packed.UnmarshalManifestPath(s.ManifestPath)
		return err == nil && loadedVer >= threshold
	default:
		// segment-list scope, storage v2: numeric data version. V_commit is sourced from
		// datacoord meta in the manifest-apply ack callback (after DataVersion++ ran), so
		// it is always faithful; the negative guard is pure defense against a missing
		// version ever slipping in (conservative not-ready, never premature).
		return threshold >= 0 && s.Version >= threshold
	}
}
