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

package delegator

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readySnapshot is an immutable, fully-ready view of a schema version that the
// read path (Search/Query) serves from. It is published atomically once all of
// its runtime dependencies (function runner metadata, idfOracle sync) are in
// place. Never mutated after publish; a new version means a new instance.
type readySnapshot struct {
	version       int64
	barrierTs     uint64
	schema        *schemapb.CollectionSchema
	functionState *functionRuntimeState
	// fieldIDs contains every field ID resolvable in this schema version
	// (including struct sub-fields), for the O(1) dropped-anns-field guard on
	// the search path.
	fieldIDs map[int64]struct{}
}

// hasField reports whether the field ID exists in this snapshot's schema.
func (s *readySnapshot) hasField(fieldID int64) bool {
	_, ok := s.fieldIDs[fieldID]
	return ok
}

// newReadySnapshot builds an immutable snapshot for a schema version. barrierTs
// is the applied schema barrier timestamp of this payload: the collection
// manager can refresh a schema payload (e.g. collection properties) at the same
// logical schema version but a newer barrier, so the barrier disambiguates
// same-version refreshes for the publish monotonic guard. (version, barrierTs,
// schema) must be read atomically from one collection snapshot; pairing a
// schema payload with a barrier read separately can pin a stale payload under a
// newer barrier and make the monotonic guard drop the genuine refresh. The
// idfOracle stays a shared, live singleton read via sd.getIDFOracle(); it is
// intentionally not captured here.
func newReadySnapshot(version int64, barrierTs uint64, schema *schemapb.CollectionSchema, functionState *functionRuntimeState) *readySnapshot {
	allFields := typeutil.GetAllFieldSchemas(schema)
	fieldIDs := make(map[int64]struct{}, len(allFields))
	for _, field := range allFields {
		fieldIDs[field.GetFieldID()] = struct{}{}
	}
	return &readySnapshot{
		version:       version,
		barrierTs:     barrierTs,
		schema:        schema,
		functionState: functionState,
		fieldIDs:      fieldIDs,
	}
}

// schemaReadyState holds the currently published readySnapshot behind an atomic
// pointer (RCU). Readers load the snapshot lock-free; the writer builds the next
// snapshot off-lock and publishes it under publishMu with a monotonic guard.
// Whether a snapshot is ready to publish is NOT decided here: the delegator's
// tryPublishReadySchema evaluates the state-derived readiness checklist
// (function runners ready, no pending BM25 stats loads, idfOracle activated)
// before calling publish, and every state-change point re-attempts, so no
// control-flow coordination between publishers is needed.
type schemaReadyState struct {
	publishMu sync.Mutex
	snap      atomic.Pointer[readySnapshot]
}

// load returns the currently published snapshot, or nil if none published yet.
func (s *schemaReadyState) load() *readySnapshot {
	return s.snap.Load()
}

// resolve returns the currently published snapshot for the read path. When no
// snapshot has been published yet (cold start / not yet serving), it returns a
// retriable ErrCollectionSchemaVersionNotReady so the caller retries.
func (s *schemaReadyState) resolve() (*readySnapshot, error) {
	snap := s.snap.Load()
	if snap == nil {
		return nil, merr.ErrCollectionSchemaVersionNotReady
	}
	return snap, nil
}

// publish atomically stores next as the current snapshot, unless a
// newer-or-equal snapshot has already been published (monotonic: a stale build
// from a concurrent path never rolls the ready view back). Ordering is by
// (version, barrierTs): a newer logical schema version always wins, and at the
// same version a newer barrier wins so a same-version payload refresh (e.g.
// collection properties, which does not bump schema.Version) still reaches the
// read path instead of being dropped. Returns true if next became the
// published snapshot.
func (s *schemaReadyState) publish(next *readySnapshot) bool {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if cur := s.snap.Load(); cur != nil && !next.isNewerThan(cur) {
		return false
	}
	s.snap.Store(next)
	return true
}

// isNewerThan reports whether s should replace cur under the (version, barrierTs)
// ordering used by publish.
func (s *readySnapshot) isNewerThan(cur *readySnapshot) bool {
	if s.version != cur.version {
		return s.version > cur.version
	}
	return s.barrierTs > cur.barrierTs
}
