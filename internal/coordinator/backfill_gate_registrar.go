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
	"fmt"
)

// Primitive-only registration entry points. The registration sites live in datacoord /
// rootcoord, which cannot import the coordinator package (import cycle: coordinator
// imports them). So they depend on a small primitive-only BackfillAtomicGateRegistrar interface
// defined in their own packages; *BackfillAtomicGate satisfies it structurally via these
// methods (no BackfillRound type crosses the package boundary).

// RegisterWatermark registers a round whose in-scope set is "schema_version <
// watermark" (internal add_function_field; external-collection add_field/add_function_field).
// All fields (vector and scalar) become ready once each in-scope segment's schema_version
// reaches the watermark (global index-meta sync makes data-loaded the readiness bar).
//
// Same roundID logic as RegisterSegmentList: roundID is a unique allocated ID, and the
// round's Source provides idempotency on retry. For a watermark round the Source is
// derived deterministically from the schema version (which uniquely identifies the
// schema-change round), so callers need not supply it.
func (r *BackfillAtomicGate) RegisterWatermark(ctx context.Context, collectionID, roundID int64, vectorFieldIDs, scalarFieldIDs []int64, watermark int32) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       fmt.Sprintf("watermark:%d", watermark),
		Fields:       buildBackfillFields(vectorFieldIDs, scalarFieldIDs),
		Scope:        NewWatermarkScope(watermark),
	})
}

// RegisterSegmentList registers a round whose in-scope set is an explicit committed
// segment list with per-segment V_commit (external backfill, any executor). roundID must
// be unique (allocate it); source is the round's external identity (e.g. the backfill
// result path) used to dedupe retries idempotently.
func (r *BackfillAtomicGate) RegisterSegmentList(ctx context.Context, collectionID, roundID int64, source string, vectorFieldIDs, scalarFieldIDs []int64, segmentVCommit map[int64]int64) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       source,
		Fields:       buildBackfillFields(vectorFieldIDs, scalarFieldIDs),
		Scope:        NewSegmentListScope(segmentVCommit),
	})
}

// buildBackfillFields flattens the caller's vector + scalar field IDs into the round's
// field set. With global index-meta sync the two are treated identically (readiness is a
// per-segment version check, not a per-field-type criterion); the split is kept only
// because the registration sites naturally know each field's type.
func buildBackfillFields(vectorFieldIDs, scalarFieldIDs []int64) []int64 {
	fields := make([]int64, 0, len(vectorFieldIDs)+len(scalarFieldIDs))
	fields = append(fields, vectorFieldIDs...)
	fields = append(fields, scalarFieldIDs...)
	return fields
}
