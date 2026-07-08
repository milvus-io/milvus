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

// Package model re-exports the rootcoord domain model from
// github.com/milvus-io/milvus/pkg/v3/metastore/model.
//
// The rootcoord model (collection / partition / database / alias / credential /
// role and their sub-types) was moved into the shared pkg/v3 module so the
// pooled catalog service can reuse the same definitions; this file keeps the
// original internal import path working unchanged via aliases. The datacoord /
// querycoord model (index / segment / segment_index / load_info) still lives in
// this package and will move with each coord's own refactor.
package model

import model "github.com/milvus-io/milvus/pkg/v3/metastore/model"

type (
	Alias            = model.Alias
	Collection       = model.Collection
	Credential       = model.Credential
	Database         = model.Database
	Field            = model.Field
	Function         = model.Function
	Option           = model.Option
	Partition        = model.Partition
	Role             = model.Role
	ShardInfo        = model.ShardInfo
	StructArrayField = model.StructArrayField
)

var (
	CheckFieldsEqual            = model.CheckFieldsEqual
	CheckPartitionsEqual        = model.CheckPartitionsEqual
	CheckStructArrayFieldsEqual = model.CheckStructArrayFieldsEqual
	CloneFields                 = model.CloneFields
	CloneFunctions              = model.CloneFunctions
	ClonePartitions             = model.ClonePartitions
	CloneStructArrayFields      = model.CloneStructArrayFields

	MarshalAliasModel                = model.MarshalAliasModel
	MarshalCollectionModel           = model.MarshalCollectionModel
	MarshalCollectionModelWithOption = model.MarshalCollectionModelWithOption
	MarshalCredentialModel           = model.MarshalCredentialModel
	MarshalDatabaseModel             = model.MarshalDatabaseModel
	MarshalFieldModel                = model.MarshalFieldModel
	MarshalFieldModels               = model.MarshalFieldModels
	MarshalFunctionModel             = model.MarshalFunctionModel
	MarshalFunctionModels            = model.MarshalFunctionModels
	MarshalPartitionModel            = model.MarshalPartitionModel
	MarshalRoleModel                 = model.MarshalRoleModel
	MarshalStructArrayFieldModel     = model.MarshalStructArrayFieldModel
	MarshalStructArrayFieldModels    = model.MarshalStructArrayFieldModels

	NewDatabase        = model.NewDatabase
	NewDefaultDatabase = model.NewDefaultDatabase

	UnmarshalAliasModel             = model.UnmarshalAliasModel
	UnmarshalCollectionModel        = model.UnmarshalCollectionModel
	UnmarshalCredentialModel        = model.UnmarshalCredentialModel
	UnmarshalDatabaseModel          = model.UnmarshalDatabaseModel
	UnmarshalFieldModel             = model.UnmarshalFieldModel
	UnmarshalFieldModels            = model.UnmarshalFieldModels
	UnmarshalFunctionModel          = model.UnmarshalFunctionModel
	UnmarshalFunctionModels         = model.UnmarshalFunctionModels
	UnmarshalPartitionModel         = model.UnmarshalPartitionModel
	UnmarshalRoleModel              = model.UnmarshalRoleModel
	UnmarshalStructArrayFieldModel  = model.UnmarshalStructArrayFieldModel
	UnmarshalStructArrayFieldModels = model.UnmarshalStructArrayFieldModels

	WithFields            = model.WithFields
	WithPartitions        = model.WithPartitions
	WithStructArrayFields = model.WithStructArrayFields
)
