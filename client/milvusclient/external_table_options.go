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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
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
