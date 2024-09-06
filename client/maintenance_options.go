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

package client

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type LoadCollectionOption interface {
	Request() *milvuspb.LoadCollectionRequest
	CheckInterval() time.Duration
}

type loadCollectionOption struct {
	collectionName       string
	interval             time.Duration
	replicaNum           int
	loadFields           []string
	skipLoadDynamicField bool
}

func (opt *loadCollectionOption) Request() *milvuspb.LoadCollectionRequest {
	return &milvuspb.LoadCollectionRequest{
		CollectionName:       opt.collectionName,
		ReplicaNumber:        int32(opt.replicaNum),
		LoadFields:           opt.loadFields,
		SkipLoadDynamicField: opt.skipLoadDynamicField,
	}
}

func (opt *loadCollectionOption) CheckInterval() time.Duration {
	return opt.interval
}

func (opt *loadCollectionOption) WithReplica(num int) *loadCollectionOption {
	opt.replicaNum = num
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

func NewLoadCollectionOption(collectionName string) *loadCollectionOption {
	return &loadCollectionOption{
		collectionName: collectionName,
		replicaNum:     1,
		interval:       time.Millisecond * 200,
	}
}

type LoadPartitionsOption interface {
	Request() *milvuspb.LoadPartitionsRequest
	CheckInterval() time.Duration
}

var _ LoadPartitionsOption = (*loadPartitionsOption)(nil)

type loadPartitionsOption struct {
	collectionName       string
	partitionNames       []string
	interval             time.Duration
	replicaNum           int
	loadFields           []string
	skipLoadDynamicField bool
}

func (opt *loadPartitionsOption) Request() *milvuspb.LoadPartitionsRequest {
	return &milvuspb.LoadPartitionsRequest{
		CollectionName:       opt.collectionName,
		PartitionNames:       opt.partitionNames,
		ReplicaNumber:        int32(opt.replicaNum),
		LoadFields:           opt.loadFields,
		SkipLoadDynamicField: opt.skipLoadDynamicField,
	}
}

func (opt *loadPartitionsOption) CheckInterval() time.Duration {
	return opt.interval
}

func (opt *loadPartitionsOption) WithReplica(num int) *loadPartitionsOption {
	opt.replicaNum = num
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

func NewLoadPartitionsOption(collectionName string, partitionsNames []string) *loadPartitionsOption {
	return &loadPartitionsOption{
		collectionName: collectionName,
		partitionNames: partitionsNames,
		replicaNum:     1,
		interval:       time.Millisecond * 200,
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
