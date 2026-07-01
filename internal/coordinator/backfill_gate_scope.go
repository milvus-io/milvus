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

// Scope construction helpers. The scope encodes "which segments must reach the readiness
// threshold" for a round, derived from the genuine backfill operation (first-source):
//   - internal & external-collection -> schema_version watermark (no segment list);
//     both bump schema_version, so the in-scope set is derivable as schema_version < V.
//   - external Spark -> the committed segment list + per-segment V_commit, because
//     Spark does not bump schema_version.

// NewWatermarkScope builds the scope for internal / external-collection rounds: every
// segment with schema_version < schemaVersion still needs the backfill.
func NewWatermarkScope(schemaVersion int32) BackfillScope {
	return BackfillScope{Kind: ScopeWatermark, Watermark: schemaVersion}
}

// NewSegmentListScope builds the scope for an external Spark round from the committed segments
// and the per-segment V_commit at which the round's columns landed.
func NewSegmentListScope(segmentVCommit map[int64]int64) BackfillScope {
	segs := make(map[int64]int64, len(segmentVCommit))
	for segID, v := range segmentVCommit {
		segs[segID] = v
	}
	return BackfillScope{Kind: ScopeSegmentList, Segments: segs}
}
