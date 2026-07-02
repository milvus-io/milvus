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

package milvusclient

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

// RefreshExternalCollectionOption is the interface for RefreshExternalCollection options.
type RefreshExternalCollectionOption interface {
	Request() *milvuspb.RefreshExternalCollectionRequest
}

var _ RefreshExternalCollectionOption = (*refreshExternalCollectionOption)(nil)

type refreshExternalCollectionOption struct {
	dbName         string
	collectionName string
	externalSource string
	externalSpec   string
}

func (opt *refreshExternalCollectionOption) Request() *milvuspb.RefreshExternalCollectionRequest {
	req := &milvuspb.RefreshExternalCollectionRequest{
		CollectionName: opt.collectionName,
		ExternalSource: opt.externalSource,
		ExternalSpec:   opt.externalSpec,
	}
	if opt.dbName != "" {
		req.DbName = opt.dbName
	}
	return req
}

// WithExternalSource sets the external source for this refresh job.
// If not set, uses the collection's current external source.
func (opt *refreshExternalCollectionOption) WithExternalSource(externalSource string) *refreshExternalCollectionOption {
	opt.externalSource = externalSource
	return opt
}

// WithExternalSpec sets the external spec for this refresh job.
// If not set, uses the collection's current external spec.
func (opt *refreshExternalCollectionOption) WithExternalSpec(externalSpec string) *refreshExternalCollectionOption {
	opt.externalSpec = externalSpec
	return opt
}

// WithDbName sets the database name for multi-tenancy. If not set, default database is used.
func (opt *refreshExternalCollectionOption) WithDbName(dbName string) *refreshExternalCollectionOption {
	opt.dbName = dbName
	return opt
}

// NewRefreshExternalCollectionOption creates a new RefreshExternalCollectionOption.
func NewRefreshExternalCollectionOption(collectionName string) *refreshExternalCollectionOption {
	return &refreshExternalCollectionOption{
		collectionName: collectionName,
	}
}

// GetRefreshExternalCollectionProgressOption is the interface for GetRefreshExternalCollectionProgress options.
type GetRefreshExternalCollectionProgressOption interface {
	Request() *milvuspb.GetRefreshExternalCollectionProgressRequest
}

var _ GetRefreshExternalCollectionProgressOption = (*getRefreshExternalCollectionProgressOption)(nil)

type getRefreshExternalCollectionProgressOption struct {
	jobID int64
}

func (opt *getRefreshExternalCollectionProgressOption) Request() *milvuspb.GetRefreshExternalCollectionProgressRequest {
	return &milvuspb.GetRefreshExternalCollectionProgressRequest{
		JobId: opt.jobID,
	}
}

// NewGetRefreshExternalCollectionProgressOption creates a new GetRefreshExternalCollectionProgressOption.
func NewGetRefreshExternalCollectionProgressOption(jobID int64) *getRefreshExternalCollectionProgressOption {
	return &getRefreshExternalCollectionProgressOption{
		jobID: jobID,
	}
}

// ListRefreshExternalCollectionJobsOption is the interface for ListRefreshExternalCollectionJobs options.
type ListRefreshExternalCollectionJobsOption interface {
	Request() *milvuspb.ListRefreshExternalCollectionJobsRequest
}

var _ ListRefreshExternalCollectionJobsOption = (*listRefreshExternalCollectionJobsOption)(nil)

type listRefreshExternalCollectionJobsOption struct {
	collectionName string
}

func (opt *listRefreshExternalCollectionJobsOption) Request() *milvuspb.ListRefreshExternalCollectionJobsRequest {
	return &milvuspb.ListRefreshExternalCollectionJobsRequest{
		CollectionName: opt.collectionName,
	}
}

// NewListRefreshExternalCollectionJobsOption creates a new ListRefreshExternalCollectionJobsOption.
func NewListRefreshExternalCollectionJobsOption(collectionName string) *listRefreshExternalCollectionJobsOption {
	return &listRefreshExternalCollectionJobsOption{
		collectionName: collectionName,
	}
}

// AlterCollectionSchemaOption is the interface for AlterCollectionSchema options.
type AlterCollectionSchemaOption interface {
	Request() *milvuspb.AlterCollectionSchemaRequest
}

type alterCollectionSchemaBaseOption struct {
	dbName         string
	collectionName string
}

// fillBase copies the shared collection identity fields into the request.
func (opt *alterCollectionSchemaBaseOption) fillBase(req *milvuspb.AlterCollectionSchemaRequest) {
	req.CollectionName = opt.collectionName
	if opt.dbName != "" {
		req.DbName = opt.dbName
	}
}

type alterCollectionSchemaAddFunctionOption struct {
	alterCollectionSchemaBaseOption
	function           *entity.Function
	outputFields       []*entity.Field
	doPhysicalBackfill bool
}

var _ AlterCollectionSchemaOption = (*alterCollectionSchemaAddFunctionOption)(nil)

// WithDbName sets the target database for an add-function request.
func (opt *alterCollectionSchemaAddFunctionOption) WithDbName(dbName string) *alterCollectionSchemaAddFunctionOption {
	opt.dbName = dbName
	return opt
}

// WithPhysicalBackfill requests server-side backfill for the added function output.
func (opt *alterCollectionSchemaAddFunctionOption) WithPhysicalBackfill(enabled bool) *alterCollectionSchemaAddFunctionOption {
	opt.doPhysicalBackfill = enabled
	return opt
}

// Request builds the RPC request for adding a function and its output fields.
func (opt *alterCollectionSchemaAddFunctionOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	fieldInfos := make([]*milvuspb.AlterCollectionSchemaRequest_FieldInfo, 0, len(opt.outputFields))
	for _, field := range opt.outputFields {
		fieldInfos = append(fieldInfos, &milvuspb.AlterCollectionSchemaRequest_FieldInfo{
			FieldSchema: field.ProtoMessage(),
		})
	}

	req := &milvuspb.AlterCollectionSchemaRequest{
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos:         fieldInfos,
					FuncSchema:         []*schemapb.FunctionSchema{opt.function.ProtoMessage()},
					DoPhysicalBackfill: opt.doPhysicalBackfill,
				},
			},
		},
	}
	opt.fillBase(req)
	return req
}

// NewAlterCollectionSchemaAddFunctionOption creates an AlterCollectionSchema option for adding a function.
func NewAlterCollectionSchemaAddFunctionOption(
	collectionName string,
	function *entity.Function,
	outputFields ...*entity.Field,
) *alterCollectionSchemaAddFunctionOption {
	return &alterCollectionSchemaAddFunctionOption{
		alterCollectionSchemaBaseOption: alterCollectionSchemaBaseOption{collectionName: collectionName},
		function:                        function,
		outputFields:                    outputFields,
	}
}

type alterCollectionSchemaAddFieldOption struct {
	alterCollectionSchemaBaseOption
	field *entity.Field
}

var _ AlterCollectionSchemaOption = (*alterCollectionSchemaAddFieldOption)(nil)

// WithDbName sets the target database for an add-field request.
func (opt *alterCollectionSchemaAddFieldOption) WithDbName(dbName string) *alterCollectionSchemaAddFieldOption {
	opt.dbName = dbName
	return opt
}

// Request builds the RPC request for adding a field.
func (opt *alterCollectionSchemaAddFieldOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	req := &milvuspb.AlterCollectionSchemaRequest{
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: opt.field.ProtoMessage()},
					},
				},
			},
		},
	}
	opt.fillBase(req)
	return req
}

// NewAlterCollectionSchemaAddFieldOption creates an AlterCollectionSchema option for adding a source-backed external field.
func NewAlterCollectionSchemaAddFieldOption(collectionName string, field *entity.Field) *alterCollectionSchemaAddFieldOption {
	return &alterCollectionSchemaAddFieldOption{
		alterCollectionSchemaBaseOption: alterCollectionSchemaBaseOption{collectionName: collectionName},
		field:                           field,
	}
}

type alterCollectionSchemaDropFieldOption struct {
	alterCollectionSchemaBaseOption
	fieldName string
}

var _ AlterCollectionSchemaOption = (*alterCollectionSchemaDropFieldOption)(nil)

// WithDbName sets the target database for a drop-field request.
func (opt *alterCollectionSchemaDropFieldOption) WithDbName(dbName string) *alterCollectionSchemaDropFieldOption {
	opt.dbName = dbName
	return opt
}

// Request builds the RPC request for dropping a field by name.
func (opt *alterCollectionSchemaDropFieldOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	req := &milvuspb.AlterCollectionSchemaRequest{
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: opt.fieldName},
				},
			},
		},
	}
	opt.fillBase(req)
	return req
}

// NewAlterCollectionSchemaDropFieldOption creates an AlterCollectionSchema option for dropping a field.
func NewAlterCollectionSchemaDropFieldOption(collectionName string, fieldName string) *alterCollectionSchemaDropFieldOption {
	return &alterCollectionSchemaDropFieldOption{
		alterCollectionSchemaBaseOption: alterCollectionSchemaBaseOption{collectionName: collectionName},
		fieldName:                       fieldName,
	}
}

type alterCollectionSchemaDropFunctionOption struct {
	alterCollectionSchemaBaseOption
	functionName     string
	dropOutputFields bool
}

var _ AlterCollectionSchemaOption = (*alterCollectionSchemaDropFunctionOption)(nil)

// WithDbName sets the target database for a drop-function request.
func (opt *alterCollectionSchemaDropFunctionOption) WithDbName(dbName string) *alterCollectionSchemaDropFunctionOption {
	opt.dbName = dbName
	return opt
}

// WithDropOutputFields drops the function output fields together with the function.
func (opt *alterCollectionSchemaDropFunctionOption) WithDropOutputFields(enabled bool) *alterCollectionSchemaDropFunctionOption {
	opt.dropOutputFields = enabled
	return opt
}

// Request builds the RPC request for dropping a function by name.
func (opt *alterCollectionSchemaDropFunctionOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	req := &milvuspb.AlterCollectionSchemaRequest{
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier:               &milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName{FunctionName: opt.functionName},
					DropFunctionOutputFields: opt.dropOutputFields,
				},
			},
		},
	}
	opt.fillBase(req)
	return req
}

// NewAlterCollectionSchemaDropFunctionOption creates an AlterCollectionSchema option for dropping a function.
func NewAlterCollectionSchemaDropFunctionOption(collectionName string, functionName string) *alterCollectionSchemaDropFunctionOption {
	return &alterCollectionSchemaDropFunctionOption{
		alterCollectionSchemaBaseOption: alterCollectionSchemaBaseOption{collectionName: collectionName},
		functionName:                    functionName,
	}
}
