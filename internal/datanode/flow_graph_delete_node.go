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
	"context"
	"encoding/binary"
	"errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type deleteNode struct {
	BaseNode

	replica Replica
}

func (ddn *deleteNode) Name() string {
	return "deletedNode"
}

func (ddn *deleteNode) Operate(in []Msg) []Msg {
	// log.Debug("DDNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	if len(in) == 0 {
		return []Msg{}
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		return []Msg{}
		// TODO: add error handling
	}

	if msMsg == nil {
		return []Msg{}
	}

	return []Msg{}
}

func getSegmentsByPKs(pks []int64, segments []*Segment) (map[int64][]int64, error) {
	if pks == nil {
		return nil, errors.New("pks is nil when getSegmentsByPKs")
	}
	if segments == nil {
		return nil, errors.New("segments is nil when getSegmentsByPKs")
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

func newDeleteDNode(ctx context.Context, replica Replica) *deleteNode {
	baseNode := BaseNode{}
	baseNode.SetMaxParallelism(Params.FlowGraphMaxQueueLength)

	return &deleteNode{
		BaseNode: baseNode,
		replica:  replica,
	}
}
