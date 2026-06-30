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

// Readiness witness is uniformly DIST and version-based (no IsLoaded propagation, no
// datacoord read). With global index-meta sync a backfilled field is correctly searchable
// once its DATA is loaded (segcore FieldAccessible reduces to HasFieldData; the index is a
// pure performance follow-up), so readiness is a per-segment check that the segment has
// loaded the version carrying the round's columns:
//   - watermark scope (internal / external-collection): schema_version >= V;
//   - segment-list scope (external Spark): the loaded manifest (v3) / data (v2) version
//     has reached the per-segment V_commit.
// A not-yet-backfilled old segment still reports the pre-backfill version and holds the
// round; a new segment is born at/after V, so it never blocks (no liveness bug). The index
// is never consulted here.

// SegmentDistInfo is a per-segment slice of the querynode dist report, holding only
// what the dist-only readiness witness needs.
type SegmentDistInfo struct {
	SegmentID     int64
	Version       int64  // loaded numeric data version (storage v2 segment-list witness)
	ManifestPath  string // loaded manifest path (storage v3 segment-list witness; encodes the version)
	SchemaVersion int32  // loaded schema_version (watermark witness)
}

// DistReader exposes the querynode dist report for a collection. The concrete adapter
// over querycoord SegmentDistManager is wired in mixCoord.
type DistReader interface {
	SegmentDist(collectionID int64) []SegmentDistInfo
}

// SegmentScopeReader resolves the in-scope (still-need-backfill) segment IDs for a
// watermark round from datacoord segment meta. Spark rounds carry their own segment
// list, so they do not need this. The concrete adapter is wired in mixCoord; it may be
// nil, in which case the watermark witness relies on the dist report alone.
type SegmentScopeReader interface {
	// StaleSegments returns the segment IDs of a collection whose schema_version is
	// strictly below the given watermark (i.e. they still need the backfill).
	StaleSegments(collectionID int64, watermark int32) []int64
}

// ReadinessProvider answers "can ALL in-scope segments correctly serve this round?"
// from the dist report alone, encapsulating the per-scope version witness so the observer
// never sees watermark-vs-Spark distinctions.
type ReadinessProvider struct {
	dist  DistReader
	scope SegmentScopeReader // optional
}

// NewReadinessProvider constructs a provider. scope may be nil.
func NewReadinessProvider(dist DistReader, scope SegmentScopeReader) *ReadinessProvider {
	return &ReadinessProvider{dist: dist, scope: scope}
}

// IsRoundReady reports whether every in-scope segment has loaded the version that carries
// the round's backfilled columns. A round reveals atomically — it returns false until
// every in-scope segment has reached that version.
func (p *ReadinessProvider) IsRoundReady(ctx context.Context, round *BackfillRound) bool {
	if round == nil || len(round.Fields) == 0 {
		return true
	}
	dist := p.dist.SegmentDist(round.CollectionID)
	byID := make(map[int64]SegmentDistInfo, len(dist))
	for _, s := range dist {
		byID[s.SegmentID] = s
	}

	switch round.Scope.Kind {
	case ScopeWatermark:
		threshold := int64(round.Scope.Watermark)
		// Every loaded segment must have reopened to schema_version >= V. A not-yet-
		// backfilled old segment still reports schema_version < V; a new segment is born
		// at/after V, so it never holds the round back.
		for _, s := range dist {
			if !segmentReady(s, threshold, true) {
				return false
			}
		}
		// Also ensure no datacoord-known stale segment is missing from dist or unready
		// (covers a segment that is known to lag but has not been reported yet).
		if p.scope != nil {
			for _, segID := range p.scope.StaleSegments(round.CollectionID, round.Scope.Watermark) {
				s, ok := byID[segID]
				if !ok || !segmentReady(s, threshold, true) {
					return false
				}
			}
		}
		return true
	case ScopeSegmentList:
		// Each committed segment must have reached its own V_commit.
		for segID, vCommit := range round.Scope.Segments {
			s, ok := byID[segID]
			if !ok {
				return false
			}
			if !segmentReady(s, vCommit, false) {
				return false
			}
		}
		return true
	default:
		return false
	}
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
		// segment-list scope, storage v2: numeric data version. A negative threshold is
		// the -1 sentinel (BackfillSegment.Version is -1 for v2) meaning no faithful
		// version was sourced upstream yet -> conservative not-ready (never premature).
		// Sourcing a faithful v2 data version is a datacoord-side follow-up (enhance.md E1).
		return threshold >= 0 && s.Version >= threshold
	}
}
