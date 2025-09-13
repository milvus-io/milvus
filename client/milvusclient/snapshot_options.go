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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// CreateSnapshotOption interface for creating snapshot options
type CreateSnapshotOption interface {
	Request() *milvuspb.CreateSnapshotRequest
}

type createSnapshotOption struct {
	dbName         string
	collectionName string
	name           string
	description    string
}

func (opt *createSnapshotOption) Request() *milvuspb.CreateSnapshotRequest {
	return &milvuspb.CreateSnapshotRequest{
		Base:           &commonpb.MsgBase{},
		Name:           opt.name,
		Description:    opt.description,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

func (opt *createSnapshotOption) WithDescription(description string) *createSnapshotOption {
	opt.description = description
	return opt
}

func (opt *createSnapshotOption) WithDbName(dbName string) *createSnapshotOption {
	opt.dbName = dbName
	return opt
}

// NewCreateSnapshotOption creates a new CreateSnapshotOption
func NewCreateSnapshotOption(name string, collectionName string) *createSnapshotOption {
	return &createSnapshotOption{
		collectionName: collectionName,
		name:           name,
	}
}

// DropSnapshotOption interface for dropping snapshot options
type DropSnapshotOption interface {
	Request() *milvuspb.DropSnapshotRequest
}

type dropSnapshotOption struct {
	name string
}

func (opt *dropSnapshotOption) Request() *milvuspb.DropSnapshotRequest {
	return &milvuspb.DropSnapshotRequest{
		Base: &commonpb.MsgBase{},
		Name: opt.name,
	}
}

// NewDropSnapshotOption creates a new DropSnapshotOption
func NewDropSnapshotOption(name string) *dropSnapshotOption {
	return &dropSnapshotOption{
		name: name,
	}
}

// ListSnapshotsOption interface for listing snapshots options
type ListSnapshotsOption interface {
	Request() *milvuspb.ListSnapshotsRequest
}

type listSnapshotsOption struct {
	dbName         string
	collectionName string
}

func (opt *listSnapshotsOption) Request() *milvuspb.ListSnapshotsRequest {
	return &milvuspb.ListSnapshotsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

func (opt *listSnapshotsOption) WithCollectionName(collectionName string) *listSnapshotsOption {
	opt.collectionName = collectionName
	return opt
}

func (opt *listSnapshotsOption) WithDbName(dbName string) *listSnapshotsOption {
	opt.dbName = dbName
	return opt
}

// NewListSnapshotsOption creates a new ListSnapshotsOption
func NewListSnapshotsOption() *listSnapshotsOption {
	return &listSnapshotsOption{}
}

// DescribeSnapshotOption interface for describing snapshot options
type DescribeSnapshotOption interface {
	Request() *milvuspb.DescribeSnapshotRequest
}

type describeSnapshotOption struct {
	name string
}

func (opt *describeSnapshotOption) Request() *milvuspb.DescribeSnapshotRequest {
	return &milvuspb.DescribeSnapshotRequest{
		Base: &commonpb.MsgBase{},
		Name: opt.name,
	}
}

// NewDescribeSnapshotOption creates a new DescribeSnapshotOption
func NewDescribeSnapshotOption(name string) *describeSnapshotOption {
	return &describeSnapshotOption{
		name: name,
	}
}

// RestoreSnapshotOption interface for restoring snapshot options
type RestoreSnapshotOption interface {
	Request() *milvuspb.RestoreSnapshotRequest
}

type restoreSnapshotOption struct {
	dbName         string
	name           string
	collectionName string
	rewriteData    bool
}

func (opt *restoreSnapshotOption) Request() *milvuspb.RestoreSnapshotRequest {
	return &milvuspb.RestoreSnapshotRequest{
		Base:           &commonpb.MsgBase{},
		Name:           opt.name,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		RewriteData:    opt.rewriteData,
	}
}

func (opt *restoreSnapshotOption) WithRewriteData(rewrite bool) *restoreSnapshotOption {
	opt.rewriteData = rewrite
	return opt
}

func (opt *restoreSnapshotOption) WithDbName(dbName string) *restoreSnapshotOption {
	opt.dbName = dbName
	return opt
}

// NewRestoreSnapshotOption creates a new RestoreSnapshotOption
func NewRestoreSnapshotOption(name string, collectionName string) *restoreSnapshotOption {
	return &restoreSnapshotOption{
		name:           name,
		collectionName: collectionName,
	}
}
