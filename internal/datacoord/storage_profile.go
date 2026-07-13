// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func beginDataCoordStorageTask(
	ctx context.Context,
	taskID string,
	collectionID int64,
	kind storageprofile.WorkloadKind,
	subtype storageprofile.WorkloadSubtype,
	phase storageprofile.WorkloadPhase,
	role storageprofile.StorageRole,
) (context.Context, func()) {
	attribution := storageprofile.Attribution{
		ScopeType:       storageprofile.ScopeTypeTask,
		TaskID:          taskID,
		Component:       "datacoord",
		NodeID:          paramtable.GetNodeID(),
		CollectionID:    collectionID,
		WorkloadClass:   storageprofile.WorkloadClassBackground,
		WorkloadKind:    kind,
		WorkloadSubtype: subtype,
		Phase:           phase,
		StorageRole:     role,
	}
	ctx = storageprofile.WithDefaultAttribution(ctx, attribution)
	if storageprofile.HasActiveRecorder(ctx) {
		return ctx, func() {}
	}
	scope := storageprofile.NewTaskScope(attribution)
	return scope.Bind(ctx), scope.Finish
}
