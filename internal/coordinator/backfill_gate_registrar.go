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
// watermark". Registration sites follow the "register at round start" rule: internal
// add_function_field registers at the DDL (the DDL auto-starts the bump-schema
// compaction); an external-collection round registers at the refresh apply (datacoord
// applyFinishedJobSegments -- the DDL starts nothing, and until a refresh lands the
// field is uniformly absent with no mixed state to protect). All fields (vector and
// scalar) become ready once each in-scope segment's schema_version reaches the
// watermark (global index-meta sync makes data-loaded the readiness bar).
//
// Same roundID logic as RegisterSegmentList: roundID is a unique allocated ID, and the
// round's Source provides idempotency on retry. For a watermark round the Source is
// derived deterministically from the schema version (which uniquely identifies the
// schema-change round), so callers need not supply it.
func (r *BackfillAtomicGate) RegisterWatermark(ctx context.Context, collectionID, roundID int64, fieldIDs []int64, watermark int32) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       fmt.Sprintf("watermark:%d", watermark),
		Fields:       fieldIDs,
		Scope:        NewWatermarkScope(watermark),
	})
}

// RegisterSegmentList registers a round whose in-scope set is an explicit committed
// segment list with per-segment V_commit (external backfill, any executor). roundID must
// be unique (allocate it); source is the round's external identity used to dedupe retries
// idempotently (the caller passes a stable key, e.g. the broadcast ID of the manifest
// commit). With global index-meta sync readiness is a per-segment version check, so the
// round's fields are a flat ID set (no vector/scalar criterion).
func (r *BackfillAtomicGate) RegisterSegmentList(ctx context.Context, collectionID, roundID int64, source string, fieldIDs []int64, segmentVCommit map[int64]int64) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       source,
		Fields:       fieldIDs,
		Scope:        NewSegmentListScope(segmentVCommit),
	})
}
