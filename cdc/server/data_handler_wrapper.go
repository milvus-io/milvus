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

package server

import (
	"context"
	"github.com/milvus-io/milvus/cdc/core/writer"
)

type DataHandlerWrapper struct {
	writer.DefaultDataHandler
	taskID  string
	handler writer.CDCDataHandler
}

func NewDataHandlerWrapper(taskID string, handler writer.CDCDataHandler) writer.CDCDataHandler {
	return &DataHandlerWrapper{
		taskID:  taskID,
		handler: handler,
	}
}

func (d *DataHandlerWrapper) metric(collectionName string, apiType string, isErr bool) {
	if isErr {
		apiExecuteCountVec.WithLabelValues(d.taskID, collectionName, apiType, failStatusLabel).Inc()
		return
	}
	apiExecuteCountVec.WithLabelValues(d.taskID, collectionName, apiType, successStatusLabel).Inc()
}

func (d *DataHandlerWrapper) CreateCollection(ctx context.Context, param *writer.CreateCollectionParam) (err error) {
	defer func() {
		d.metric(param.Schema.CollectionName, "CreateCollection", err != nil)
	}()
	err = d.handler.CreateCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) DropCollection(ctx context.Context, param *writer.DropCollectionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "DropCollection", err != nil)
	}()
	err = d.handler.DropCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) Insert(ctx context.Context, param *writer.InsertParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "Insert", err != nil)
	}()
	err = d.handler.Insert(ctx, param)
	return
}

func (d *DataHandlerWrapper) Delete(ctx context.Context, param *writer.DeleteParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "Delete", err != nil)
	}()
	err = d.handler.Delete(ctx, param)
	return
}
