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

package rootcoord

import coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/rootcoord"

// These symbols were defined in meta_table.go / meta_rbac.go, which moved to the
// shared pkg/v3/coordmeta/rootcoord package. They are also referenced by
// internal/rootcoord DDL task / callback code, so they are re-exported here
// under their original (unexported) names — the values are identical, so
// errors.Is checks keep matching across the package boundary.
var (
	errIgnoredAlterAlias       = coordmeta.ErrIgnoredAlterAlias
	errIgnoredAlterCollection  = coordmeta.ErrIgnoredAlterCollection
	errIgnoredAlterDatabase    = coordmeta.ErrIgnoredAlterDatabase
	errIgnoredCreateCollection = coordmeta.ErrIgnoredCreateCollection
	errIgnoerdCreatePartition  = coordmeta.ErrIgnoerdCreatePartition
	errIgnoredDropCollection   = coordmeta.ErrIgnoredDropCollection
	errIgnoredDropPartition    = coordmeta.ErrIgnoredDropPartition
	errAlterCollectionNotFound = coordmeta.ErrAlterCollectionNotFound

	errEmptyRBACMeta           = coordmeta.ErrEmptyRBACMeta
	errNotCustomPrivilegeGroup = coordmeta.ErrNotCustomPrivilegeGroup
	errRoleAlreadyExists       = coordmeta.ErrRoleAlreadyExists
	errRoleNotExists           = coordmeta.ErrRoleNotExists
	errUserAlreadyExists       = coordmeta.ErrUserAlreadyExists
	errUserNotFound            = coordmeta.ErrUserNotFound
)

var (
	updateMaxFieldIDProperty     = coordmeta.UpdateMaxFieldIDProperty
	maxAssignedFieldIDFromSchema = coordmeta.MaxAssignedFieldIDFromSchema
)
