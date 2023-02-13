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
	"github.com/milvus-io/milvus/cdc/core/model"
)

type WriteType string

const (
	CreateCollection WriteType = "CreateCollection"
	DropCollection   WriteType = "DropCollection"
	Insert           WriteType = "Insert"
	Delete           WriteType = "Delete"
)

type CDCWriter interface {
	util.CDCMark

	// Write you MUST handle the error if the return value is not nil
	Write(context context.Context, data *model.CDCData, callback WriteCallback) error
	Flush(context context.Context)
}

type DefaultWriter struct {
	util.CDCMark
}

func (d *DefaultWriter) Write(context context.Context, data *model.CDCData, callback WriteCallback) error {
	return nil
}

func (d *DefaultWriter) Flush(context context.Context) {
}

type CallbackChannelInfo struct {
	Position *commonpb.KeyDataPair
	Ts       uint64
}

type WriteCallback interface {
	util.CDCMark

	// lastPosition
	OnFail(data *model.CDCData, err error)

	OnSuccess(collectionID int64, channelInfos map[string]CallbackChannelInfo)
}

type DefaultWriteCallBack struct {
	util.CDCMark
}

func (d *DefaultWriteCallBack) OnFail(data *model.CDCData, err error) {
}

func (d *DefaultWriteCallBack) OnSuccess(collectionID int64, channelInfos map[string]CallbackChannelInfo) {
}

type NotifyCollectionPositionChangeFunc func(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair)

type BufferOp interface {
	Apply(ctx context.Context, f NotifyCollectionPositionChangeFunc)
}

type BufferOpFunc func(context.Context, NotifyCollectionPositionChangeFunc)

func (b BufferOpFunc) Apply(ctx context.Context, f NotifyCollectionPositionChangeFunc) {
	b(ctx, f)
}

type CombinableBufferOpFunc struct {
}
