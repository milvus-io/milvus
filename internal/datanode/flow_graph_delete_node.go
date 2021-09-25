// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"encoding/binary"
	"errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode

	replica Replica
}

func (dn *deleteNode) Name() string {
	return "deleteNode"
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) Operate(in []Msg) []Msg {
	log.Debug("deleteNode Operating")

	if len(in) != 1 {
		log.Warn("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	_, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		return []Msg{}
	}

	return []Msg{}
}

// getSegmentsByPKs returns the bloom filter check result.
// If the key may exists in the segment, returns it in map.
// If the key not exists in the segment, the segment is filter out.
func getSegmentsByPKs(pks []int64, segments []*Segment) (map[int64][]int64, error) {
	if pks == nil {
		return nil, errors.New("pks is nil")
	}
	if segments == nil {
		return nil, errors.New("segments is nil")
	}
	results := make(map[int64][]int64)
	buf := make([]byte, 8)
	for _, segment := range segments {
		for _, pk := range pks {
			binary.BigEndian.PutUint64(buf, uint64(pk))
			exist := segment.pkFilter.Test(buf)
			if exist {
				results[segment.segmentID] = append(results[segment.segmentID], pk)
			}
		}
	}
	return results, nil
}

func newDeleteDNode(replica Replica) *deleteNode {
	baseNode := BaseNode{}
	baseNode.SetMaxParallelism(Params.FlowGraphMaxQueueLength)

	return &deleteNode{
		BaseNode: baseNode,
		replica:  replica,
	}
}
