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

package querynode

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/cockroachdb/errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type primaryKey = storage.PrimaryKey
type int64PrimaryKey = storage.Int64PrimaryKey
type varCharPrimaryKey = storage.VarCharPrimaryKey

var newInt64PrimaryKey = storage.NewInt64PrimaryKey
var newVarCharPrimaryKey = storage.NewVarCharPrimaryKey

// deleteNode is the one of nodes in delta flow graph
type deleteNode struct {
	baseNode
	collectionID  UniqueID
	metaReplica   ReplicaInterface // historical
	deltaVchannel Channel
	dmlVchannel   Channel
}

// Name returns the name of deleteNode
func (dNode *deleteNode) Name() string {
	return fmt.Sprintf("dNode-%s", dNode.deltaVchannel)
}

func (dNode *deleteNode) IsValidInMsg(in []Msg) bool {
	if !dNode.baseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*deleteMsg)
	if !ok {
		log.Warn("type assertion failed for deleteMsg", zap.String("msgType", reflect.TypeOf(in[0]).Name()), zap.String("name", dNode.Name()))
		return false
	}
	return true
}

// Operate handles input messages, do delete operations
func (dNode *deleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	dMsg, ok := in[0].(*deleteMsg)
	if !ok {
		log.Warn("type assertion failed for deleteMsg", zap.String("msgType", reflect.TypeOf(in[0]).Name()), zap.String("name", dNode.Name()))
		return []Msg{}
	}

	delData := &deleteData{
		deleteIDs:        map[UniqueID][]primaryKey{},
		deleteTimestamps: map[UniqueID][]Timestamp{},
		deleteOffset:     map[UniqueID]int64{},
	}

	var spans []trace.Span
	for _, msg := range dMsg.deleteMessages {
		ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(msg.TraceCtx(), "Delete_Node")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	if dMsg.IsCloseMsg() {
		return []Msg{
			&serviceTimeMsg{BaseMsg: flowgraph.NewBaseMsg(true)},
		}
	}

	// 1. filter segment by bloom filter
	for i, delMsg := range dMsg.deleteMessages {
		traceID := spans[i].SpanContext().TraceID().String()
		log.Debug("delete in historical replica",
			zap.String("vchannel", dNode.deltaVchannel),
			zap.Int64("collectionID", delMsg.CollectionID),
			zap.String("collectionName", delMsg.CollectionName),
			zap.Int64("numPKs", delMsg.NumRows),
			zap.Int("numTS", len(delMsg.Timestamps)),
			zap.Uint64("timestampBegin", delMsg.BeginTs()),
			zap.Uint64("timestampEnd", delMsg.EndTs()),
			zap.Int("segmentNum", dNode.metaReplica.getSegmentNum(segmentTypeSealed)),
			zap.String("traceID", traceID),
		)

		if dNode.metaReplica.getSegmentNum(segmentTypeSealed) != 0 {
			err := processDeleteMessages(dNode.metaReplica, segmentTypeSealed, delMsg, delData, dNode.dmlVchannel)
			if err != nil {
				// error occurs when missing meta info or unexpected pk type, should not happen
				err = fmt.Errorf("deleteNode processDeleteMessages failed, collectionID = %d, err = %s, channel = %s", delMsg.CollectionID, err, dNode.deltaVchannel)
				log.Error(err.Error())
				panic(err)
			}
		}
	}

	// 2. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment, err := dNode.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
		if err != nil {
			// should not happen, segment should be created before
			log.Warn("failed to get segment",
				zap.Int64("collectionID", dNode.collectionID),
				zap.Int64("segmentID", segmentID),
				zap.String("vchannel", dNode.deltaVchannel),
			)
			continue
		}
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	// 3. do delete
	wg := sync.WaitGroup{}
	for segmentID := range delData.deleteOffset {
		segmentID := segmentID
		wg.Add(1)
		go func() {
			err := dNode.delete(delData, segmentID, &wg)
			if err != nil {
				// error occurs when segment cannot be found, calling cgo function delete failed and etc...
				log.Warn("failed to apply deletions to segment",
					zap.Int64("segmentID", segmentID),
					zap.Error(err),
				)

				// For cases: segment compacted, not loaded yet, or just released,
				// to ignore the error,
				// panic otherwise.
				if !errors.Is(err, ErrSegmentNotFound) && !errors.Is(err, ErrSegmentUnhealthy) {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: dMsg.timeRange,
	}

	return []Msg{res}
}

// delete will do delete operation at segment which id is segmentID
func (dNode *deleteNode) delete(deleteData *deleteData, segmentID UniqueID, wg *sync.WaitGroup) error {
	defer wg.Done()
	targetSegment, err := dNode.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
	if err != nil {
		return WrapSegmentNotFound(segmentID)
	}

	if targetSegment.getType() != segmentTypeSealed {
		return fmt.Errorf("unexpected segmentType when delete, segmentID = %d, segmentType = %s", segmentID, targetSegment.segmentType.String())
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err = targetSegment.segmentDelete(offset, ids, timestamps)
	if err != nil {
		return fmt.Errorf("segmentDelete failed, segmentID = %d, err=%w", segmentID, err)
	}

	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])),
		zap.Int64("segmentID", segmentID),
		zap.String("SegmentType", targetSegment.getType().String()),
		zap.String("vchannel", dNode.deltaVchannel))
	return nil
}

// newDeleteNode returns a new deleteNode
func newDeleteNode(metaReplica ReplicaInterface, collectionID UniqueID, deltaVchannel Channel) (*deleteNode, error) {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism.GetAsInt32()

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	dmlVChannel, err := funcutil.ConvertChannelName(deltaVchannel, Params.CommonCfg.RootCoordDelta.GetValue(), Params.CommonCfg.RootCoordDml.GetValue())
	if err != nil {
		log.Error("failed to convert deltaVChannel to dmlVChannel", zap.String("deltaVChannel", deltaVchannel), zap.Error(err))
		return nil, err
	}

	return &deleteNode{
		baseNode:      baseNode,
		collectionID:  collectionID,
		metaReplica:   metaReplica,
		deltaVchannel: deltaVchannel,
		dmlVchannel:   dmlVChannel,
	}, nil
}
