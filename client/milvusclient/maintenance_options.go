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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type LoadCollectionOption interface {
	Request() *milvuspb.LoadCollectionRequest
	CheckInterval() time.Duration
	IsRefresh() bool
}

type loadCollectionOption struct {
	collectionName       string
	interval             time.Duration
	replicaNum           int
	loadFields           []string
	skipLoadDynamicField bool
	isRefresh            bool
	resourceGroups       []string
}

func (opt *loadCollectionOption) Request() *milvuspb.LoadCollectionRequest {
	return &milvuspb.LoadCollectionRequest{
		CollectionName:       opt.collectionName,
		ReplicaNumber:        int32(opt.replicaNum),
		LoadFields:           opt.loadFields,
		SkipLoadDynamicField: opt.skipLoadDynamicField,
		ResourceGroups:       opt.resourceGroups,
	}
}

func (opt *loadCollectionOption) CheckInterval() time.Duration {
	return opt.interval
}

func (opt *loadCollectionOption) IsRefresh() bool {
	return opt.isRefresh
}

func (opt *loadCollectionOption) WithReplica(num int) *loadCollectionOption {
	opt.replicaNum = num
	return opt
}

func (opt *loadCollectionOption) WithResourceGroup(resourceGroups ...string) *loadCollectionOption {
	opt.resourceGroups = resourceGroups
	return opt
}

func (opt *loadCollectionOption) WithLoadFields(loadFields ...string) *loadCollectionOption {
	opt.loadFields = loadFields
	return opt
}

func (opt *loadCollectionOption) WithSkipLoadDynamicField(skipFlag bool) *loadCollectionOption {
	opt.skipLoadDynamicField = skipFlag
	return opt
}

func (opt *loadCollectionOption) WithRefresh(isRefresh bool) *loadCollectionOption {
	opt.isRefresh = isRefresh
	return opt
}

func NewLoadCollectionOption(collectionName string) *loadCollectionOption {
	return &loadCollectionOption{
		collectionName: collectionName,
		// replicaNum:     1, The default value of the replicaNum should be set on the server side
		interval: time.Millisecond * 200,
	}
}

type LoadPartitionsOption interface {
	Request() *milvuspb.LoadPartitionsRequest
	CheckInterval() time.Duration
	IsRefresh() bool
}

var _ LoadPartitionsOption = (*loadPartitionsOption)(nil)

type loadPartitionsOption struct {
	collectionName       string
	partitionNames       []string
	interval             time.Duration
	replicaNum           int
	resourceGroups       []string
	loadFields           []string
	skipLoadDynamicField bool
	isRefresh            bool
}

func (opt *loadPartitionsOption) Request() *milvuspb.LoadPartitionsRequest {
	return &milvuspb.LoadPartitionsRequest{
		CollectionName:       opt.collectionName,
		PartitionNames:       opt.partitionNames,
		ReplicaNumber:        int32(opt.replicaNum),
		LoadFields:           opt.loadFields,
		SkipLoadDynamicField: opt.skipLoadDynamicField,
		ResourceGroups:       opt.resourceGroups,
	}
}

func (opt *loadPartitionsOption) CheckInterval() time.Duration {
	return opt.interval
}

func (opt *loadPartitionsOption) IsRefresh() bool {
	return opt.isRefresh
}

func (opt *loadPartitionsOption) WithReplica(num int) *loadPartitionsOption {
	opt.replicaNum = num
	return opt
}

func (opt *loadPartitionsOption) WithResourceGroup(resourceGroups ...string) *loadPartitionsOption {
	opt.resourceGroups = resourceGroups
	return opt
}

func (opt *loadPartitionsOption) WithLoadFields(loadFields ...string) *loadPartitionsOption {
	opt.loadFields = loadFields
	return opt
}

func (opt *loadPartitionsOption) WithSkipLoadDynamicField(skipFlag bool) *loadPartitionsOption {
	opt.skipLoadDynamicField = skipFlag
	return opt
}

func (opt *loadPartitionsOption) WithRefresh(isRefresh bool) *loadPartitionsOption {
	opt.isRefresh = isRefresh
	return opt
}

func NewLoadPartitionsOption(collectionName string, partitionsNames ...string) *loadPartitionsOption {
	return &loadPartitionsOption{
		collectionName: collectionName,
		partitionNames: partitionsNames,
		// replicaNum:     1, The default value of the replicaNum should be set on the server side
		interval: time.Millisecond * 200,
	}
}

type GetLoadStateOption interface {
	Request() *milvuspb.GetLoadStateRequest
	ProgressRequest() *milvuspb.GetLoadingProgressRequest
}

type getLoadStateOption struct {
	collectionName string
	partitionNames []string
}

func (opt *getLoadStateOption) Request() *milvuspb.GetLoadStateRequest {
	return &milvuspb.GetLoadStateRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
	}
}

func (opt *getLoadStateOption) ProgressRequest() *milvuspb.GetLoadingProgressRequest {
	return &milvuspb.GetLoadingProgressRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
	}
}

func NewGetLoadStateOption(collectionName string, partitionNames ...string) *getLoadStateOption {
	return &getLoadStateOption{
		collectionName: collectionName,
		partitionNames: partitionNames,
	}
}

type RefreshLoadOption interface {
	Request() *milvuspb.LoadCollectionRequest
	CheckInterval() time.Duration
}

type refreshLoadOption struct {
	collectionName string
	checkInterval  time.Duration
}

func (opt *refreshLoadOption) Request() *milvuspb.LoadCollectionRequest {
	return &milvuspb.LoadCollectionRequest{
		CollectionName: opt.collectionName,
		Refresh:        true,
	}
}

func (opt *refreshLoadOption) CheckInterval() time.Duration {
	return opt.checkInterval
}

func NewRefreshLoadOption(collectionName string) *refreshLoadOption {
	return &refreshLoadOption{
		collectionName: collectionName,
		checkInterval:  time.Millisecond * 200,
	}
}

type ReleaseCollectionOption interface {
	Request() *milvuspb.ReleaseCollectionRequest
}

var _ ReleaseCollectionOption = (*releaseCollectionOption)(nil)

type releaseCollectionOption struct {
	collectionName string
}

func (opt *releaseCollectionOption) Request() *milvuspb.ReleaseCollectionRequest {
	return &milvuspb.ReleaseCollectionRequest{
		CollectionName: opt.collectionName,
	}
}

func NewReleaseCollectionOption(collectionName string) *releaseCollectionOption {
	return &releaseCollectionOption{
		collectionName: collectionName,
	}
}

type ReleasePartitionsOption interface {
	Request() *milvuspb.ReleasePartitionsRequest
}

var _ ReleasePartitionsOption = (*releasePartitionsOption)(nil)

type releasePartitionsOption struct {
	collectionName string
	partitionNames []string
}

func (opt *releasePartitionsOption) Request() *milvuspb.ReleasePartitionsRequest {
	return &milvuspb.ReleasePartitionsRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
	}
}

func NewReleasePartitionsOptions(collectionName string, partitionNames ...string) *releasePartitionsOption {
	return &releasePartitionsOption{
		collectionName: collectionName,
		partitionNames: partitionNames,
	}
}

type FlushOption interface {
	Request() *milvuspb.FlushRequest
	CollectionName() string
	CheckInterval() time.Duration
}

type flushOption struct {
	collectionName string
	interval       time.Duration
}

func (opt *flushOption) Request() *milvuspb.FlushRequest {
	return &milvuspb.FlushRequest{
		CollectionNames: []string{opt.collectionName},
	}
}

func (opt *flushOption) CollectionName() string {
	return opt.collectionName
}

func (opt *flushOption) CheckInterval() time.Duration {
	return opt.interval
}

func NewFlushOption(collName string) *flushOption {
	return &flushOption{
		collectionName: collName,
		interval:       time.Millisecond * 200,
	}
}

type CompactOption interface {
	Request() *milvuspb.ManualCompactionRequest
}

type compactOption struct {
	collectionName string
}

func (opt *compactOption) Request() *milvuspb.ManualCompactionRequest {
	return &milvuspb.ManualCompactionRequest{
		CollectionName: opt.collectionName,
	}
}

func NewCompactOption(collectionName string) *compactOption {
	return &compactOption{
		collectionName: collectionName,
	}
}

type GetCompactionStateOption interface {
	Request() *milvuspb.GetCompactionStateRequest
}

type getCompactionStateOption struct {
	compactionID int64
}

func (opt *getCompactionStateOption) Request() *milvuspb.GetCompactionStateRequest {
	return &milvuspb.GetCompactionStateRequest{
		CompactionID: opt.compactionID,
	}
}

func NewGetCompactionStateOption(compactionID int64) *getCompactionStateOption {
	return &getCompactionStateOption{
		compactionID: compactionID,
	}
}
