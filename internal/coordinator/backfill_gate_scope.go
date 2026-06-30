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

// Scope construction helpers. The scope encodes the round's WRITE-SIDE semantics
// (the read side is uniformly certified by the target promote barrier):
//   - internal (add_function_field) -> schema_version watermark: the backfill bumps
//     schema_version progressively, so "write side done" must be live-checked as
//     "no eligible segment with schema_version < V".
//   - external Spark -> no payload: the round registers after the manifest apply, so
//     the write side holds by construction.

// NewWatermarkScope builds the scope for internal rounds: every sealed segment with
// schema_version < schemaVersion still needs the backfill, and every vchannel checkpoint
// must pass the DDL timetick (proving no pre-V data still hides in growing segments).
func NewWatermarkScope(schemaVersion int32, schemaChangeTimeTick uint64) BackfillScope {
	return BackfillScope{Kind: ScopeWatermark, Watermark: schemaVersion, SchemaChangeTimeTick: schemaChangeTimeTick}
}

// NewExternalScope builds the scope for an external Spark round. It carries no payload:
// the round registers after the manifest apply (write side by construction) and the read
// side is certified by the target promote barrier.
func NewExternalScope() BackfillScope {
	return BackfillScope{Kind: ScopeExternal}
}
