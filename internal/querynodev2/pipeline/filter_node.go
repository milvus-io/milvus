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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
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
	log := log.Ctx(context.TODO())
	if in == nil {
		log.Debug("type assertion failed for Msg in filterNode because it's nil",
			zap.String("name", fNode.Name()))
		return nil
	}

	streamMsgPack, ok := in.(*msgstream.MsgPack)
	if !ok {
		log.Warn("type assertion failed for MsgPack",
			zap.String("msgType", reflect.TypeOf(in).Name()),
			zap.String("name", fNode.Name()))
		return nil
	}

	metrics.QueryNodeConsumerMsgCount.
		WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel, fmt.Sprint(fNode.collectionID)).
		Inc()

	metrics.QueryNodeConsumeTimeTickLag.
		WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel, fmt.Sprint(fNode.collectionID)).
		Set(float64(tsoutil.SubByNow(streamMsgPack.EndTs)))

	// Get collection from collection manager
	collection := fNode.manager.Collection.Get(fNode.collectionID)
	if collection == nil {
		log.Fatal("collection not found in meta", zap.Int64("collectionID", fNode.collectionID))
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
			log.Debug("filter invalid message",
				zap.String("message type", msg.Type().String()),
				zap.String("channel", fNode.channel),
				zap.Int64("collectionID", fNode.collectionID),
				zap.Error(err),
			)
		} else {
			out.append(msg)
		}
	}
	fNode.delegator.TryCleanExcludedSegments(streamMsgPack.EndTs)
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Inc()
	return out
}

// filtrate message with filter policy
func (fNode *filterNode) filtrate(c *Collection, msg msgstream.TsMsg) error {
	switch msg.Type() {
	case commonpb.MsgType_Insert:
		insertMsg := msg.(*msgstream.InsertMsg)
		metrics.QueryNodeConsumeCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Add(float64(insertMsg.Size()))
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
		metrics.QueryNodeConsumeCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Add(float64(deleteMsg.Size()))
		for _, policy := range fNode.DeleteMsgPolicys {
			err := policy(fNode, c, deleteMsg)
			if err != nil {
				return err
			}
		}
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
