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

type UseDatabaseOption interface {
	DbName() string
}

type useDatabaseNameOpt struct {
	dbName string
}

func (opt *useDatabaseNameOpt) DbName() string {
	return opt.dbName
}

func NewUseDatabaseOption(dbName string) *useDatabaseNameOpt {
	return &useDatabaseNameOpt{
		dbName: dbName,
	}
}

// ListDatabaseOption is a builder interface for ListDatabase request.
type ListDatabaseOption interface {
	Request() *milvuspb.ListDatabasesRequest
}

type listDatabaseOption struct{}

func (opt *listDatabaseOption) Request() *milvuspb.ListDatabasesRequest {
	return &milvuspb.ListDatabasesRequest{}
}

func NewListDatabaseOption() *listDatabaseOption {
	return &listDatabaseOption{}
}

type CreateDatabaseOption interface {
	Request() *milvuspb.CreateDatabaseRequest
}

type createDatabaseOption struct {
	dbName string
}

func (opt *createDatabaseOption) Request() *milvuspb.CreateDatabaseRequest {
	return &milvuspb.CreateDatabaseRequest{
		DbName: opt.dbName,
	}
}

func NewCreateDatabaseOption(dbName string) *createDatabaseOption {
	return &createDatabaseOption{
		dbName: dbName,
	}
}

type DropDatabaseOption interface {
	Request() *milvuspb.DropDatabaseRequest
}

type dropDatabaseOption struct {
	dbName string
}

func (opt *dropDatabaseOption) Request() *milvuspb.DropDatabaseRequest {
	return &milvuspb.DropDatabaseRequest{
		DbName: opt.dbName,
	}
}

func NewDropDatabaseOption(dbName string) *dropDatabaseOption {
	return &dropDatabaseOption{
		dbName: dbName,
	}
}
