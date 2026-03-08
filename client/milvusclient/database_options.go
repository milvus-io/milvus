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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

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
	dbName     string
	Properties map[string]string
}

func (opt *createDatabaseOption) Request() *milvuspb.CreateDatabaseRequest {
	return &milvuspb.CreateDatabaseRequest{
		DbName:     opt.dbName,
		Properties: entity.MapKvPairs(opt.Properties),
	}
}

func (opt *createDatabaseOption) WithProperty(key string, val any) *createDatabaseOption {
	opt.Properties[key] = fmt.Sprintf("%v", val)
	return opt
}

func NewCreateDatabaseOption(dbName string) *createDatabaseOption {
	return &createDatabaseOption{
		dbName:     dbName,
		Properties: make(map[string]string),
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

type DescribeDatabaseOption interface {
	Request() *milvuspb.DescribeDatabaseRequest
}

type describeDatabaseOption struct {
	dbName string
}

func (opt *describeDatabaseOption) Request() *milvuspb.DescribeDatabaseRequest {
	return &milvuspb.DescribeDatabaseRequest{
		DbName: opt.dbName,
	}
}

func NewDescribeDatabaseOption(dbName string) *describeDatabaseOption {
	return &describeDatabaseOption{
		dbName: dbName,
	}
}

type AlterDatabasePropertiesOption interface {
	Request() *milvuspb.AlterDatabaseRequest
}

type alterDatabasePropertiesOption struct {
	dbName     string
	properties map[string]string
}

func (opt *alterDatabasePropertiesOption) Request() *milvuspb.AlterDatabaseRequest {
	return &milvuspb.AlterDatabaseRequest{
		DbName:     opt.dbName,
		Properties: entity.MapKvPairs(opt.properties),
	}
}

func (opt *alterDatabasePropertiesOption) WithProperty(key string, value any) *alterDatabasePropertiesOption {
	opt.properties[key] = fmt.Sprintf("%v", value)
	return opt
}

func NewAlterDatabasePropertiesOption(dbName string) *alterDatabasePropertiesOption {
	return &alterDatabasePropertiesOption{
		dbName:     dbName,
		properties: make(map[string]string),
	}
}

type DropDatabasePropertiesOption interface {
	Request() *milvuspb.AlterDatabaseRequest
}

type dropDatabasePropertiesOption struct {
	dbName string
	keys   []string
}

func (opt *dropDatabasePropertiesOption) Request() *milvuspb.AlterDatabaseRequest {
	return &milvuspb.AlterDatabaseRequest{
		DbName:     opt.dbName,
		DeleteKeys: opt.keys,
	}
}

func NewDropDatabasePropertiesOption(dbName string, propertyKeys ...string) *dropDatabasePropertiesOption {
	return &dropDatabasePropertiesOption{
		dbName: dbName,
		keys:   propertyKeys,
	}
}
