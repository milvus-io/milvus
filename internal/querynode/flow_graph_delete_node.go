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

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type primaryKey = storage.PrimaryKey
type int64PrimaryKey = storage.Int64PrimaryKey
type varCharPrimaryKey = storage.VarCharPrimaryKey

var newInt64PrimaryKey = storage.NewInt64PrimaryKey
var newVarCharPrimaryKey = storage.NewVarCharPrimaryKey

// deleteNode is the one of nodes in delta flow graph
type deleteNode struct {
	baseNode
	collectionID UniqueID
	metaReplica  ReplicaInterface // historical
}

// Name returns the name of deleteNode
func (dNode *deleteNode) Name() string {
	return "dNode"
}

// Operate handles input messages, do delete operations
func (dNode *deleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	dMsg, ok := in[0].(*deleteMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for deleteMsg because it's nil")
		} else {
			log.Warn("type assertion failed for deleteMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	delData := &deleteData{
		deleteIDs:        map[UniqueID][]primaryKey{},
		deleteTimestamps: map[UniqueID][]Timestamp{},
		deleteOffset:     map[UniqueID]int64{},
	}

	if dMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range dMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	collection, err := dNode.metaReplica.getCollectionByID(dNode.collectionID)
	if err != nil {
		// QueryNode should add collection before start flow graph
		panic(fmt.Errorf("%s getCollectionByID failed, collectionID = %d", dNode.Name(), dNode.collectionID))
	}
	collection.RLock()
	defer collection.RUnlock()

	// 1. filter segment by bloom filter
	for i, delMsg := range dMsg.deleteMessages {
		traceID, _, _ := trace.InfoFromSpan(spans[i])
		log.Debug("delete in historical replica",
			zap.Any("collectionID", delMsg.CollectionID),
			zap.Any("collectionName", delMsg.CollectionName),
			zap.Int64("numPKs", delMsg.NumRows),
			zap.Int("numTS", len(delMsg.Timestamps)),
			zap.Any("timestampBegin", delMsg.BeginTs()),
			zap.Any("timestampEnd", delMsg.EndTs()),
			zap.Any("segmentNum", dNode.metaReplica.getSegmentNum(segmentTypeSealed)),
			zap.Any("traceID", traceID),
		)

		if dNode.metaReplica.getSegmentNum(segmentTypeSealed) != 0 {
			err := processDeleteMessages(dNode.metaReplica, segmentTypeSealed, delMsg, delData)
			if err != nil {
				// error occurs when missing meta info or unexpected pk type, should not happen
				err = fmt.Errorf("deleteNode processDeleteMessages failed, collectionID = %d, err = %s", delMsg.CollectionID, err)
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
			err = fmt.Errorf("deleteNode getSegmentByID failed, err = %s", err)
			log.Error(err.Error())
			panic(err)
		}
		offset, err := segment.segmentPreDelete(len(pks))
		if err != nil {
			// error occurs when cgo function `PreDelete` failed
			err = fmt.Errorf("segmentPreDelete failed, segmentID = %d, err = %s", segmentID, err)
			log.Error(err.Error())
			panic(err)
		}
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
				err = fmt.Errorf("segment delete failed, segmentID = %d, err = %s", segmentID, err)
				log.Error(err.Error())
				panic(err)
			}
		}()
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: dMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

// delete will do delete operation at segment which id is segmentID
func (dNode *deleteNode) delete(deleteData *deleteData, segmentID UniqueID, wg *sync.WaitGroup) error {
	defer wg.Done()
	targetSegment, err := dNode.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
	if err != nil {
		return fmt.Errorf("getSegmentByID failed, err = %s", err)
	}

	if targetSegment.segmentType != segmentTypeSealed {
		return fmt.Errorf("unexpected segmentType when delete, segmentID = %d, segmentType = %s", segmentID, targetSegment.segmentType.String())
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err = targetSegment.segmentDelete(offset, ids, timestamps)
	if err != nil {
		return fmt.Errorf("segmentDelete failed, segmentID = %d", segmentID)
	}

	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID), zap.Any("SegmentType", targetSegment.segmentType))
	return nil
}

// newDeleteNode returns a new deleteNode
func newDeleteNode(metaReplica ReplicaInterface, collectionID UniqueID) *deleteNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		metaReplica:  metaReplica,
	}
}
