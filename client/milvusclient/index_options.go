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
	"github.com/milvus-io/milvus/client/v2/index"
)

type CreateIndexOption interface {
	Request() *milvuspb.CreateIndexRequest
}

type createIndexOption struct {
	collectionName string
	fieldName      string
	indexName      string
	indexDef       index.Index

	extraParams map[string]any
}

func (opt *createIndexOption) WithExtraParam(key string, value any) {
	opt.extraParams[key] = value
}

func (opt *createIndexOption) Request() *milvuspb.CreateIndexRequest {
	params := opt.indexDef.Params()
	for key, value := range opt.extraParams {
		params[key] = fmt.Sprintf("%v", value)
	}
	req := &milvuspb.CreateIndexRequest{
		CollectionName: opt.collectionName,
		FieldName:      opt.fieldName,
		IndexName:      opt.indexName,
		ExtraParams:    entity.MapKvPairs(params),
	}

	return req
}

func (opt *createIndexOption) WithIndexName(indexName string) *createIndexOption {
	opt.indexName = indexName
	return opt
}

func NewCreateIndexOption(collectionName string, fieldName string, index index.Index) *createIndexOption {
	return &createIndexOption{
		collectionName: collectionName,
		fieldName:      fieldName,
		indexDef:       index,
		extraParams:    make(map[string]any),
	}
}

type ListIndexOption interface {
	Request() *milvuspb.DescribeIndexRequest
	Matches(*milvuspb.IndexDescription) bool
}

var _ ListIndexOption = (*listIndexOption)(nil)

type listIndexOption struct {
	collectionName string
	fieldName      string
}

func (opt *listIndexOption) WithFieldName(fieldName string) *listIndexOption {
	opt.fieldName = fieldName
	return opt
}

func (opt *listIndexOption) Matches(idxDef *milvuspb.IndexDescription) bool {
	return opt.fieldName == "" || idxDef.GetFieldName() == opt.fieldName
}

func (opt *listIndexOption) Request() *milvuspb.DescribeIndexRequest {
	return &milvuspb.DescribeIndexRequest{
		CollectionName: opt.collectionName,
		FieldName:      opt.fieldName,
	}
}

func NewListIndexOption(collectionName string) *listIndexOption {
	return &listIndexOption{
		collectionName: collectionName,
	}
}

type DescribeIndexOption interface {
	Request() *milvuspb.DescribeIndexRequest
}

type describeIndexOption struct {
	collectionName string
	fieldName      string
	indexName      string
}

func (opt *describeIndexOption) Request() *milvuspb.DescribeIndexRequest {
	return &milvuspb.DescribeIndexRequest{
		CollectionName: opt.collectionName,
		IndexName:      opt.indexName,
	}
}

func NewDescribeIndexOption(collectionName string, indexName string) *describeIndexOption {
	return &describeIndexOption{
		collectionName: collectionName,
		indexName:      indexName,
	}
}

type DropIndexOption interface {
	Request() *milvuspb.DropIndexRequest
}

type dropIndexOption struct {
	collectionName string
	indexName      string
}

func (opt *dropIndexOption) Request() *milvuspb.DropIndexRequest {
	return &milvuspb.DropIndexRequest{
		CollectionName: opt.collectionName,
		IndexName:      opt.indexName,
	}
}

func NewDropIndexOption(collectionName string, indexName string) *dropIndexOption {
	return &dropIndexOption{
		collectionName: collectionName,
		indexName:      indexName,
	}
}

type AlterIndexPropertiesOption interface {
	Request() *milvuspb.AlterIndexRequest
}

type alterIndexPropertiesOption struct {
	collectionName string
	indexName      string
	properties     map[string]string
}

func (opt *alterIndexPropertiesOption) Request() *milvuspb.AlterIndexRequest {
	return &milvuspb.AlterIndexRequest{
		CollectionName: opt.collectionName,
		IndexName:      opt.indexName,
		ExtraParams:    entity.MapKvPairs(opt.properties),
	}
}

func (opt *alterIndexPropertiesOption) WithProperty(key string, value any) *alterIndexPropertiesOption {
	opt.properties[key] = fmt.Sprintf("%v", value)
	return opt
}

func NewAlterIndexPropertiesOption(collectionName string, indexName string) *alterIndexPropertiesOption {
	return &alterIndexPropertiesOption{
		collectionName: collectionName,
		indexName:      indexName,
		properties:     make(map[string]string),
	}
}

type DropIndexPropertiesOption interface {
	Request() *milvuspb.AlterIndexRequest
}

type dropIndexPropertiesOption struct {
	collectionName string
	indexName      string
	keys           []string
}

func (opt *dropIndexPropertiesOption) Request() *milvuspb.AlterIndexRequest {
	return &milvuspb.AlterIndexRequest{
		CollectionName: opt.collectionName,
		IndexName:      opt.indexName,
		DeleteKeys:     opt.keys,
	}
}

func NewDropIndexPropertiesOption(collectionName string, indexName string, keys ...string) *dropIndexPropertiesOption {
	return &dropIndexPropertiesOption{
		collectionName: collectionName,
		indexName:      indexName,
		keys:           keys,
	}
}
