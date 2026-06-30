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

// RegisterWatermark registers a round whose write side is "no sealed segment below the
// watermark AND every vchannel checkpoint past the DDL timetick" (internal
// add_function_field; the DDL auto-starts the bump-schema compaction). It is called from
// the AlterCollection ack callback (message declares, ack registers), so a failed
// broadcast never arms a gate and a registration failure is retried by the ack loop.
//
// roundID is a unique allocated ID; the round's Source ("watermark:<V>", the schema
// version uniquely identifies the schema-change round) provides idempotency on retry.
func (r *BackfillAtomicGate) RegisterWatermark(ctx context.Context, collectionID, roundID int64, fieldIDs []int64, watermark int32, schemaChangeTimeTick uint64) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       fmt.Sprintf("watermark:%d", watermark),
		Fields:       fieldIDs,
		Scope:        NewWatermarkScope(watermark, schemaChangeTimeTick),
	})
}

// RegisterExternal registers an external (Spark) backfill round. The caller MUST invoke
// it AFTER the manifest apply landed in datacoord meta (the ack callback does), so the
// write side holds by construction — the observer stamps the round's T_c at first sight
// and revokes once a later-built target is promoted. roundID must be unique (allocate
// it); source is the round's external identity used to dedupe retries idempotently (a
// stable key, e.g. the broadcast ID of the manifest commit).
func (r *BackfillAtomicGate) RegisterExternal(ctx context.Context, collectionID, roundID int64, source string, fieldIDs []int64) error {
	return r.Register(ctx, &BackfillRound{
		CollectionID: collectionID,
		RoundID:      roundID,
		Source:       source,
		Fields:       fieldIDs,
		Scope:        NewExternalScope(),
	})
}
