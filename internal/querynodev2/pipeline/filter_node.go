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
	"context"
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// filterNode filter the invalid message of pipeline
type filterNode struct {
	*BaseNode
	collectionID     UniqueID
	manager          *DataManager
	channel          string
	InsertMsgPolicys []InsertMsgFilter
	DeleteMsgPolicys []DeleteMsgFilter

	delegator delegator.ShardDelegator
}

func (fNode *filterNode) Operate(in Msg) Msg {
	if in == nil {
		mlog.Debug(context.TODO(), "type assertion failed for Msg in filterNode because it's nil",
			mlog.String("name", fNode.Name()))
		return nil
	}

	streamMsgPack, ok := in.(*msgstream.MsgPack)
	if !ok {
		mlog.Warn(context.TODO(), "type assertion failed for MsgPack",
			mlog.String("msgType", reflect.TypeOf(in).Name()),
			mlog.String("name", fNode.Name()))
		return nil
	}

	metrics.QueryNodeConsumerMsgCount.
		WithLabelValues(paramtable.GetStringNodeID(), metrics.AllLabel, fmt.Sprint(fNode.collectionID)).
		Inc()

	metrics.QueryNodeConsumeTimeTickLag.
		WithLabelValues(paramtable.GetStringNodeID(), metrics.TimetickLabel, fmt.Sprint(fNode.collectionID)).
		Set(float64(tsoutil.SubByNow(streamMsgPack.EndTs)))

	// Get collection from collection manager
	collection := fNode.manager.Collection.Get(fNode.collectionID)
	if collection == nil {
		mlog.Fatal(context.TODO(), "collection not found in meta", mlog.FieldCollectionID(fNode.collectionID))
	}

	out := &insertNodeMsg{
		insertMsgs: []*InsertMsg{},
		deleteMsgs: []*DeleteMsg{},
		timeRange: TimeRange{
			timestampMin: streamMsgPack.BeginTs,
			timestampMax: streamMsgPack.EndTs,
		},
	}

	// add msg to out if msg pass check of filter
	for _, msg := range streamMsgPack.Msgs {
		err := fNode.filtrate(collection, msg)
		if err != nil {
			mlog.Debug(context.TODO(), "filter invalid message",
				mlog.String("message type", msg.Type().String()),
				mlog.String("channel", fNode.channel),
				mlog.FieldCollectionID(fNode.collectionID),
				mlog.Err(err),
			)
		} else {
			out.append(msg)
		}
	}
	fNode.delegator.TryCleanExcludedSegments(streamMsgPack.EndTs)
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).Inc()
	return out
}

// filtrate message with filter policy
func (fNode *filterNode) filtrate(c *Collection, msg msgstream.TsMsg) error {
	switch msg.Type() {
	case commonpb.MsgType_Insert:
		insertMsg := msg.(*msgstream.InsertMsg)
		metrics.QueryNodeConsumeCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).Add(float64(insertMsg.Size()))
		for _, policy := range fNode.InsertMsgPolicys {
			err := policy(fNode, c, insertMsg)
			if err != nil {
				return err
			}
		}

		// check segment whether excluded
		ok := fNode.delegator.VerifyExcludedSegments(insertMsg.SegmentID, insertMsg.EndTimestamp)
		if !ok {
			m := fmt.Sprintf("skip msg due to segment=%d has been excluded", insertMsg.GetSegmentID())
			return merr.WrapErrServiceInternal(m)
		}
		return nil

	case commonpb.MsgType_Delete:
		deleteMsg := msg.(*msgstream.DeleteMsg)
		metrics.QueryNodeConsumeCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).Add(float64(deleteMsg.Size()))
		for _, policy := range fNode.DeleteMsgPolicys {
			err := policy(fNode, c, deleteMsg)
			if err != nil {
				return err
			}
		}
	case commonpb.MsgType_AddCollectionField:
		schemaMsg := msg.(*adaptor.SchemaChangeMessageBody)
		header := schemaMsg.SchemaChangeMessage.Header()
		if header.GetCollectionId() != fNode.collectionID {
			return merr.WrapErrCollectionNotFound(header.GetCollectionId())
		}
		return nil
	case commonpb.MsgType_AlterCollection:
		putCollectionMsg := msg.(*adaptor.AlterCollectionMessageBody)
		header := putCollectionMsg.AlterCollectionMessage.Header()
		if header.GetCollectionId() != fNode.collectionID {
			return merr.WrapErrCollectionNotFound(header.GetCollectionId())
		}
		return nil
	case commonpb.MsgType_ManualFlush:
		// ManualFlush is handled by StreamingNode WAL flusher (fence + persist).
		// QueryNode only consumes the barrier so the pipeline advances.
		manualFlushMsg := msg.(*adaptor.ManualFlushMessageBody)
		header := manualFlushMsg.ManualFlushMessage.Header()
		if header.GetCollectionId() != fNode.collectionID {
			return merr.WrapErrCollectionNotFound(header.GetCollectionId())
		}
		return nil
	default:
		return merr.WrapErrParameterInvalid("msgType is Insert or Delete", "not")
	}
	return nil
}

func newFilterNode(
	collectionID int64,
	channel string,
	manager *DataManager,
	delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *filterNode {
	return &filterNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("FilterNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		manager:      manager,
		channel:      channel,
		delegator:    delegator,
		InsertMsgPolicys: []InsertMsgFilter{
			InsertNotAligned,
			InsertEmpty,
			InsertOutOfTarget,
		},
		DeleteMsgPolicys: []DeleteMsgFilter{
			DeleteNotAligned,
			DeleteEmpty,
			DeleteOutOfTarget,
		},
	}
}
