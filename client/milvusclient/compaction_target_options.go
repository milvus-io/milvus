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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// CreateCompactionTargetOption builds a create request for a REWRITE or OPTIMIZE target.
type CreateCompactionTargetOption interface {
	Request() *milvuspb.CreateCompactionTargetRequest
}

type createCompactionTargetOption struct {
	dbName          string
	collectionName  string
	segmentIDs      []int64
	optimize        bool
	targetSizeBytes int64
}

// NewRewriteCompactionTargetOption creates a REWRITE request. An empty list selects the collection scope.
func NewRewriteCompactionTargetOption(collectionName string, segmentIDs ...int64) *createCompactionTargetOption {
	return &createCompactionTargetOption{collectionName: collectionName, segmentIDs: slices.Clone(segmentIDs)}
}

// NewOptimizeCompactionTargetOption creates an OPTIMIZE request with a target size in bytes.
func NewOptimizeCompactionTargetOption(collectionName string, targetSizeBytes int64) *createCompactionTargetOption {
	return &createCompactionTargetOption{collectionName: collectionName, optimize: true, targetSizeBytes: targetSizeBytes}
}

func (opt *createCompactionTargetOption) WithDbName(dbName string) *createCompactionTargetOption {
	opt.dbName = dbName
	return opt
}

func (opt *createCompactionTargetOption) Request() *milvuspb.CreateCompactionTargetRequest {
	req := &milvuspb.CreateCompactionTargetRequest{DbName: opt.dbName, CollectionName: opt.collectionName}
	if opt.optimize {
		req.Target = &milvuspb.CreateCompactionTargetRequest_Optimize{
			Optimize: &milvuspb.OptimizeTargetSpec{InputTargetSizeBytes: opt.targetSizeBytes},
		}
	} else {
		req.Target = &milvuspb.CreateCompactionTargetRequest_Rewrite{
			Rewrite: &milvuspb.RewriteTargetSpec{SegmentIds: slices.Clone(opt.segmentIDs)},
		}
	}
	return req
}

// ListCompactionTargetsOption builds a list request. An empty database filter includes all databases.
type ListCompactionTargetsOption interface {
	Request() *milvuspb.ListCompactionTargetsRequest
}

type listCompactionTargetsOption struct {
	dbName         string
	collectionName *string
	collectionID   *int64
	states         []milvuspb.CompactionTargetState
	includeSystem  bool
}

func NewListCompactionTargetsOption() *listCompactionTargetsOption {
	return &listCompactionTargetsOption{}
}

func (opt *listCompactionTargetsOption) WithDbName(dbName string) *listCompactionTargetsOption {
	opt.dbName = dbName
	return opt
}

func (opt *listCompactionTargetsOption) WithCollectionName(name string) *listCompactionTargetsOption {
	opt.collectionName, opt.collectionID = &name, nil
	return opt
}

func (opt *listCompactionTargetsOption) WithCollectionID(id int64) *listCompactionTargetsOption {
	opt.collectionName, opt.collectionID = nil, &id
	return opt
}

func (opt *listCompactionTargetsOption) WithStates(states ...milvuspb.CompactionTargetState) *listCompactionTargetsOption {
	opt.states = slices.Clone(states)
	return opt
}

func (opt *listCompactionTargetsOption) WithIncludeSystem(include bool) *listCompactionTargetsOption {
	opt.includeSystem = include
	return opt
}

func (opt *listCompactionTargetsOption) Request() *milvuspb.ListCompactionTargetsRequest {
	req := &milvuspb.ListCompactionTargetsRequest{
		DbName: opt.dbName, States: slices.Clone(opt.states), IncludeSystem: opt.includeSystem,
	}
	if opt.collectionName != nil {
		req.CollectionFilter = &milvuspb.ListCompactionTargetsRequest_CollectionName{CollectionName: *opt.collectionName}
	}
	if opt.collectionID != nil {
		req.CollectionFilter = &milvuspb.ListCompactionTargetsRequest_CollectionId{CollectionId: *opt.collectionID}
	}
	return req
}
