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

import "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

// CreatePartitionOption is the interface builds Create Partition request.
type CreatePartitionOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.CreatePartitionRequest
}

type createPartitionOpt struct {
	collectionName string
	partitionName  string
}

func (opt *createPartitionOpt) Request() *milvuspb.CreatePartitionRequest {
	return &milvuspb.CreatePartitionRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
	}
}

func NewCreatePartitionOption(collectionName string, partitionName string) *createPartitionOpt {
	return &createPartitionOpt{
		collectionName: collectionName,
		partitionName:  partitionName,
	}
}

// DropPartitionOption is the interface that builds Drop Partition request.
type DropPartitionOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.DropPartitionRequest
}

type dropPartitionOpt struct {
	collectionName string
	partitionName  string
}

func (opt *dropPartitionOpt) Request() *milvuspb.DropPartitionRequest {
	return &milvuspb.DropPartitionRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
	}
}

func NewDropPartitionOption(collectionName string, partitionName string) *dropPartitionOpt {
	return &dropPartitionOpt{
		collectionName: collectionName,
		partitionName:  partitionName,
	}
}

// HasPartitionOption is the interface builds HasPartition request.
type HasPartitionOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.HasPartitionRequest
}

var _ HasPartitionOption = (*hasPartitionOpt)(nil)

type hasPartitionOpt struct {
	collectionName string
	partitionName  string
}

func (opt *hasPartitionOpt) Request() *milvuspb.HasPartitionRequest {
	return &milvuspb.HasPartitionRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
	}
}

func NewHasPartitionOption(collectionName string, partitionName string) *hasPartitionOpt {
	return &hasPartitionOpt{
		collectionName: collectionName,
		partitionName:  partitionName,
	}
}

// ListPartitionsOption is the interface builds List Partition request.
type ListPartitionsOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.ShowPartitionsRequest
}

type listPartitionsOpt struct {
	collectionName string
}

func (opt *listPartitionsOpt) Request() *milvuspb.ShowPartitionsRequest {
	return &milvuspb.ShowPartitionsRequest{
		CollectionName: opt.collectionName,
		Type:           milvuspb.ShowType_All,
	}
}

func NewListPartitionOption(collectionName string) *listPartitionsOpt {
	return &listPartitionsOpt{
		collectionName: collectionName,
	}
}

type GetPartitionStatsOption interface {
	Request() *milvuspb.GetPartitionStatisticsRequest
}

type getPartitionStatsOpt struct {
	collectionName string
	partitionName  string
}

func (opt *getPartitionStatsOpt) Request() *milvuspb.GetPartitionStatisticsRequest {
	return &milvuspb.GetPartitionStatisticsRequest{
		CollectionName: opt.collectionName,
		PartitionName:  opt.partitionName,
	}
}

func NewGetPartitionStatsOption(collectionName string, partitionName string) *getPartitionStatsOpt {
	return &getPartitionStatsOpt{
		collectionName: collectionName,
		partitionName:  partitionName,
	}
}
