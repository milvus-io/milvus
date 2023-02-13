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

package writer

import (
	"context"

	"github.com/milvus-io/milvus/cdc/core/util"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type CDCDataHandler interface {
	util.CDCMark

	CreateCollection(ctx context.Context, param *CreateCollectionParam) error
	DropCollection(ctx context.Context, param *DropCollectionParam) error
	Insert(ctx context.Context, param *InsertParam) error
	Delete(ctx context.Context, param *DeleteParam) error
}

type DefaultDataHandler struct {
	util.CDCMark
}

func (d *DefaultDataHandler) CreateCollection(ctx context.Context, param *CreateCollectionParam) error {
	return nil
}

func (d *DefaultDataHandler) DropCollection(ctx context.Context, param *DropCollectionParam) error {
	return nil
}

func (d *DefaultDataHandler) Insert(ctx context.Context, param *InsertParam) error {
	return nil
}

func (d *DefaultDataHandler) Delete(ctx context.Context, param *DeleteParam) error {
	return nil
}

type CreateCollectionParam struct {
	Schema           *entity.Schema
	ShardsNum        int32
	ConsistencyLevel commonpb.ConsistencyLevel
	Properties       []*commonpb.KeyValuePair
}

type DropCollectionParam struct {
	CollectionName string
}

type InsertParam struct {
	CollectionName string
	PartitionName  string
	Columns        []entity.Column
}

type DeleteParam struct {
	CollectionName string
	PartitionName  string
	Column         entity.Column
}
