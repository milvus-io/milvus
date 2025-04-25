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

package pipeline

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

type insertNodeMsg struct {
	insertMsgs    []*InsertMsg
	deleteMsgs    []*DeleteMsg
	insertDatas   map[int64]*delegator.InsertData
	timeRange     TimeRange
	schema        *schemapb.CollectionSchema
	schemaVersion uint64
}

type deleteNodeMsg struct {
	deleteMsgs    []*DeleteMsg
	timeRange     TimeRange
	schema        *schemapb.CollectionSchema
	schemaVersion uint64
}

func (msg *insertNodeMsg) append(taskMsg msgstream.TsMsg) error {
	switch taskMsg.Type() {
	case commonpb.MsgType_Insert:
		insertMsg := taskMsg.(*InsertMsg)
		msg.insertMsgs = append(msg.insertMsgs, insertMsg)
		collector.Rate.Add(metricsinfo.InsertConsumeThroughput, float64(insertMsg.Size()))
	case commonpb.MsgType_Delete:
		deleteMsg := taskMsg.(*DeleteMsg)
		msg.deleteMsgs = append(msg.deleteMsgs, deleteMsg)
		collector.Rate.Add(metricsinfo.DeleteConsumeThroughput, float64(deleteMsg.Size()))
	case commonpb.MsgType_AddCollectionField:
		schemaMsg := taskMsg.(*adaptor.SchemaChangeMessageBody)
		body, err := schemaMsg.SchemaChangeMessage.Body()
		if err != nil {
			return err
		}
		msg.schema = body.GetSchema()
		msg.schemaVersion = taskMsg.BeginTs()
	default:
		return merr.WrapErrParameterInvalid("msgType is Insert or Delete", "not")
	}
	return nil
}
