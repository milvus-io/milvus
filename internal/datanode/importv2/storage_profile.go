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

package importv2

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newImportStorageScope(taskID, collectionID int64, subtype storageprofile.WorkloadSubtype, phase storageprofile.WorkloadPhase) *storageprofile.Scope {
	role := storageprofile.StorageRoleSource
	if subtype == storageprofile.WorkloadSubtypeCopySegment {
		role = storageprofile.StorageRolePersistent
	}
	return storageprofile.NewTaskScope(storageprofile.Attribution{
		TaskID:          strconv.FormatInt(taskID, 10),
		Component:       "datanode",
		NodeID:          paramtable.GetNodeID(),
		CollectionID:    collectionID,
		WorkloadClass:   storageprofile.WorkloadClassBackground,
		WorkloadKind:    storageprofile.WorkloadKindImport,
		WorkloadSubtype: subtype,
		Phase:           phase,
		StorageRole:     role,
	})
}

type profiledImportTask interface {
	FinishStorageProfile()
}
