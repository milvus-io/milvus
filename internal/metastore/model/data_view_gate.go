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

package model

// DataViewGateKind records WHY a field is gated so a RootCoord restart can reconcile correctly:
// a Drop gate is released (drop DDL is redriven or abandoned; paused proxies must be resumed),
// an Add gate is re-installed and its backfill-completion releaser restarted.
type DataViewGateKind int32

const (
	DataViewGateDrop DataViewGateKind = 0
	DataViewGateAdd  DataViewGateKind = 1
)

// DataViewGate is RootCoord's persisted gate record for one collection. It is persisted (dedicated
// etcd prefix) because the gate carries push-only state — the per-collection complex-delete pause on
// already-connected proxies is NOT part of the DescribeCollection pull, so a crashed RootCoord must
// re-derive it on restart to resume those proxies. AlterCollectionSchema is serialized per
// collection (DDL lock), so at most one active gate record exists per collection at a time.
type DataViewGate struct {
	CollectionID int64
	// OpVersion is the schema version this operation produced (each schema-change DDL increments
	// coll.SchemaVersion), unique per collection — it is the per-operation key discriminator and, for
	// an Add, the completion target: the gate releases once every alive segment reaches it.
	OpVersion int32
	FieldIDs  []int64
	Kind      DataViewGateKind
}
