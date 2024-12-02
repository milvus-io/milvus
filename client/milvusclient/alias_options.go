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

// CreateCollectionOption is the interface builds CreateAliasRequest.
type CreateAliasOption interface {
	Request() *milvuspb.CreateAliasRequest
}

type createAliasOption struct {
	collectionName string
	alias          string
}

func (opt *createAliasOption) Request() *milvuspb.CreateAliasRequest {
	return &milvuspb.CreateAliasRequest{
		CollectionName: opt.collectionName,
		Alias:          opt.alias,
	}
}

func NewCreateAliasOption(collectionName, alias string) *createAliasOption {
	return &createAliasOption{
		collectionName: collectionName,
		alias:          alias,
	}
}

// DescribeAliasOption is the interface builds DescribeAliasOption.
type DescribeAliasOption interface {
	Request() *milvuspb.DescribeAliasRequest
}

type describeAliasOption struct {
	aliasName string
}

func (opt *describeAliasOption) Request() *milvuspb.DescribeAliasRequest {
	return &milvuspb.DescribeAliasRequest{
		Alias: opt.aliasName,
	}
}

func NewDescribeAliasOption(alias string) *describeAliasOption {
	return &describeAliasOption{
		aliasName: alias,
	}
}

// DropAliasOption is the interface builds DropAliasRequest.
type DropAliasOption interface {
	Request() *milvuspb.DropAliasRequest
}

type dropAliasOption struct {
	aliasName string
}

func (opt *dropAliasOption) Request() *milvuspb.DropAliasRequest {
	return &milvuspb.DropAliasRequest{
		Alias: opt.aliasName,
	}
}

func NewDropAliasOption(alias string) *dropAliasOption {
	return &dropAliasOption{
		aliasName: alias,
	}
}

// AlterAliasOption is the interface builds AlterAliasRequest.
type AlterAliasOption interface {
	Request() *milvuspb.AlterAliasRequest
}

type alterAliasOption struct {
	aliasName      string
	collectionName string
}

func (opt *alterAliasOption) Request() *milvuspb.AlterAliasRequest {
	return &milvuspb.AlterAliasRequest{
		Alias:          opt.aliasName,
		CollectionName: opt.collectionName,
	}
}

func NewAlterAliasOption(alias, collectionName string) *alterAliasOption {
	return &alterAliasOption{
		aliasName:      alias,
		collectionName: collectionName,
	}
}

// ListAliasesOption is the interface builds ListAliasesRequest.
type ListAliasesOption interface {
	Request() *milvuspb.ListAliasesRequest
}

type listAliasesOption struct {
	collectionName string
}

func (opt *listAliasesOption) Request() *milvuspb.ListAliasesRequest {
	return &milvuspb.ListAliasesRequest{
		CollectionName: opt.collectionName,
	}
}

func NewListAliasesOption(collectionName string) *listAliasesOption {
	return &listAliasesOption{
		collectionName: collectionName,
	}
}
