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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// indexUpdate is all field indexes bound to ONE add-field DDL, plus the DDL BeginTs
// used as their shared monotonic apply barrier. Grouping per DDL here (at the source,
// where the DDL boundary is known) lets them fan out as one atomic update, so the
// barrier's monotonic gate does not drop all but the first.
type indexUpdate struct {
	fieldIndexes []*indexpb.FieldIndex
	barrierTs    uint64
}

type insertNodeMsg struct {
	insertMsgs      []*InsertMsg
	deleteMsgs      []*DeleteMsg
	timeRange       TimeRange
	schema          *schemapb.CollectionSchema
	schemaBarrierTs uint64
	indexUpdates    []indexUpdate
}

type deleteNodeMsg struct {
	deleteMsgs      []*DeleteMsg
	timeRange       TimeRange
	schema          *schemapb.CollectionSchema
	schemaBarrierTs uint64
	indexUpdates    []indexUpdate
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
		msg.schemaBarrierTs = taskMsg.BeginTs()
	case commonpb.MsgType_AlterCollection:
		putCollectionMsg := taskMsg.(*adaptor.AlterCollectionMessageBody)
		header := putCollectionMsg.AlterCollectionMessage.Header()
		if messageutil.IsSchemaChange(header) {
			body := putCollectionMsg.AlterCollectionMessage.MustBody()
			msg.schema = body.GetUpdates().GetSchema()
			msg.schemaBarrierTs = taskMsg.BeginTs()
			// An add-field DDL carries the index meta bound to the new fields on the
			// SAME message; fan it to segments' col_index_meta_ so the backfilled
			// field's brute-force search stops throwing. Same BeginTs as the schema,
			// so schema + index apply atomically and monotonically.
			if boundFieldIndexes := body.GetUpdates().GetBoundFieldIndexes(); len(boundFieldIndexes) > 0 {
				msg.indexUpdates = append(msg.indexUpdates, indexUpdate{
					fieldIndexes: boundFieldIndexes,
					barrierTs:    taskMsg.BeginTs(),
				})
			}
		}
	case commonpb.MsgType_ManualFlush:
		// ManualFlush is consumed in filterNode.filtrate(); no insert/delete payload here.
	default:
		return merr.WrapErrParameterInvalid("msgType is Insert or Delete", "not")
	}
	return nil
}
