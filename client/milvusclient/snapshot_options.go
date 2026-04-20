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
	name           string
	dbName         string
	collectionName string
}

func (opt *dropSnapshotOption) Request() *milvuspb.DropSnapshotRequest {
	return &milvuspb.DropSnapshotRequest{
		Base:           &commonpb.MsgBase{},
		Name:           opt.name,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

func (opt *dropSnapshotOption) WithDbName(dbName string) *dropSnapshotOption {
	opt.dbName = dbName
	return opt
}

// NewDropSnapshotOption creates a new DropSnapshotOption
func NewDropSnapshotOption(name string, collectionName string) *dropSnapshotOption {
	return &dropSnapshotOption{
		name:           name,
		collectionName: collectionName,
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

// NewListSnapshotsOption creates a new ListSnapshotsOption.
// collectionName is required — the proxy rejects empty collection_name.
func NewListSnapshotsOption(collectionName string) *listSnapshotsOption {
	return &listSnapshotsOption{collectionName: collectionName}
}

// DescribeSnapshotOption interface for describing snapshot options
type DescribeSnapshotOption interface {
	Request() *milvuspb.DescribeSnapshotRequest
}

type describeSnapshotOption struct {
	name           string
	dbName         string
	collectionName string
}

func (opt *describeSnapshotOption) Request() *milvuspb.DescribeSnapshotRequest {
	return &milvuspb.DescribeSnapshotRequest{
		Base:           &commonpb.MsgBase{},
		Name:           opt.name,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

func (opt *describeSnapshotOption) WithDbName(dbName string) *describeSnapshotOption {
	opt.dbName = dbName
	return opt
}

// NewDescribeSnapshotOption creates a new DescribeSnapshotOption
func NewDescribeSnapshotOption(name string, collectionName string) *describeSnapshotOption {
	return &describeSnapshotOption{
		name:           name,
		collectionName: collectionName,
	}
}

// RestoreSnapshotOption interface for restoring snapshot options
type RestoreSnapshotOption interface {
	Request() *milvuspb.RestoreSnapshotRequest
}

type restoreSnapshotOption struct {
	name                 string
	dbName               string
	collectionName       string
	targetDbName         string
	targetCollectionName string
}

func (opt *restoreSnapshotOption) Request() *milvuspb.RestoreSnapshotRequest {
	return &milvuspb.RestoreSnapshotRequest{
		Base:                 &commonpb.MsgBase{},
		Name:                 opt.name,
		DbName:               opt.dbName,
		CollectionName:       opt.collectionName,
		TargetDbName:         opt.targetDbName,
		TargetCollectionName: opt.targetCollectionName,
	}
}

func (opt *restoreSnapshotOption) WithDbName(dbName string) *restoreSnapshotOption {
	opt.dbName = dbName
	return opt
}

func (opt *restoreSnapshotOption) WithTargetDbName(targetDbName string) *restoreSnapshotOption {
	opt.targetDbName = targetDbName
	return opt
}

// NewRestoreSnapshotOption creates a new RestoreSnapshotOption
// name: snapshot name, collectionName: the source collection where the snapshot lives,
// targetCollectionName: the name for the restored collection (must differ from source)
func NewRestoreSnapshotOption(name string, collectionName string, targetCollectionName string) *restoreSnapshotOption {
	return &restoreSnapshotOption{
		name:                 name,
		collectionName:       collectionName,
		targetCollectionName: targetCollectionName,
	}
}

// GetRestoreSnapshotStateOption interface for getting restore snapshot state options
type GetRestoreSnapshotStateOption interface {
	Request() *milvuspb.GetRestoreSnapshotStateRequest
}

type getRestoreSnapshotStateOption struct {
	jobID int64
}

func (opt *getRestoreSnapshotStateOption) Request() *milvuspb.GetRestoreSnapshotStateRequest {
	return &milvuspb.GetRestoreSnapshotStateRequest{
		Base:  &commonpb.MsgBase{},
		JobId: opt.jobID,
	}
}

// NewGetRestoreSnapshotStateOption creates a new GetRestoreSnapshotStateOption
func NewGetRestoreSnapshotStateOption(jobID int64) *getRestoreSnapshotStateOption {
	return &getRestoreSnapshotStateOption{
		jobID: jobID,
	}
}

// ListRestoreSnapshotJobsOption interface for listing restore snapshot jobs options
type ListRestoreSnapshotJobsOption interface {
	Request() *milvuspb.ListRestoreSnapshotJobsRequest
}

type listRestoreSnapshotJobsOption struct {
	dbName         string
	collectionName string
}

func (opt *listRestoreSnapshotJobsOption) Request() *milvuspb.ListRestoreSnapshotJobsRequest {
	return &milvuspb.ListRestoreSnapshotJobsRequest{
		Base:           &commonpb.MsgBase{},
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

func (opt *listRestoreSnapshotJobsOption) WithDbName(dbName string) *listRestoreSnapshotJobsOption {
	opt.dbName = dbName
	return opt
}

func (opt *listRestoreSnapshotJobsOption) WithCollectionName(collectionName string) *listRestoreSnapshotJobsOption {
	opt.collectionName = collectionName
	return opt
}

// NewListRestoreSnapshotJobsOption creates a new ListRestoreSnapshotJobsOption
func NewListRestoreSnapshotJobsOption() *listRestoreSnapshotJobsOption {
	return &listRestoreSnapshotJobsOption{}
}

// PinSnapshotDataOption interface for pinning snapshot data
type PinSnapshotDataOption interface {
	Request() *milvuspb.PinSnapshotDataRequest
}

type pinSnapshotDataOption struct {
	name           string
	dbName         string
	collectionName string
	ttlSeconds     int64
}

func (opt *pinSnapshotDataOption) Request() *milvuspb.PinSnapshotDataRequest {
	return &milvuspb.PinSnapshotDataRequest{
		Base:           &commonpb.MsgBase{},
		Name:           opt.name,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		TtlSeconds:     opt.ttlSeconds,
	}
}

func (opt *pinSnapshotDataOption) WithDbName(dbName string) *pinSnapshotDataOption {
	opt.dbName = dbName
	return opt
}

func (opt *pinSnapshotDataOption) WithTTL(ttlSeconds int64) *pinSnapshotDataOption {
	opt.ttlSeconds = ttlSeconds
	return opt
}

// NewPinSnapshotDataOption creates a new PinSnapshotDataOption
func NewPinSnapshotDataOption(name string, collectionName string) *pinSnapshotDataOption {
	return &pinSnapshotDataOption{name: name, collectionName: collectionName}
}

// UnpinSnapshotDataOption interface for unpinning snapshot data
type UnpinSnapshotDataOption interface {
	Request() *milvuspb.UnpinSnapshotDataRequest
}

type unpinSnapshotDataOption struct {
	pinID int64
}

func (opt *unpinSnapshotDataOption) Request() *milvuspb.UnpinSnapshotDataRequest {
	return &milvuspb.UnpinSnapshotDataRequest{
		Base:  &commonpb.MsgBase{},
		PinId: opt.pinID,
	}
}

// NewUnpinSnapshotDataOption creates a new UnpinSnapshotDataOption
func NewUnpinSnapshotDataOption(pinID int64) *unpinSnapshotDataOption {
	return &unpinSnapshotDataOption{pinID: pinID}
}
