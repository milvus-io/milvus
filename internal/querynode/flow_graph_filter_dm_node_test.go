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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func getFilterDMNode(ctx context.Context) (*filterDmNode, error) {
	streaming, err := genSimpleStreaming(ctx)
	if err != nil {
		return nil, err
	}

	streaming.replica.addExcludedSegments(defaultCollectionID, nil)
	return newFilteredDmNode(streaming.replica, loadTypeCollection, defaultCollectionID, defaultPartitionID), nil
}

func TestFlowGraphFilterDmNode_filterDmNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fg, err := getFilterDMNode(ctx)
	assert.NoError(t, err)
	fg.Name()
}

func TestFlowGraphFilterDmNode_invalidLoadType(t *testing.T) {
	const invalidLoadType = -1
	fg := newFilteredDmNode(nil, invalidLoadType, defaultCollectionID, defaultPartitionID)
	assert.Nil(t, fg)
}

func TestFlowGraphFilterDmNode_filterInvalidInsertMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("valid test", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidInsertMessage(msg)
		assert.NotNil(t, res)
	})

	t.Run("test no collection", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		msg.CollectionID = UniqueID(1000)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test no partition", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		msg.PartitionID = UniqueID(1000)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.loadType = loadTypePartition
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test not target collection", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test not target partition", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.loadType = loadTypePartition
		fg.partitionID = UniqueID(1000)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test released partition", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		col, err := fg.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.addReleasedPartition(defaultPartitionID)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test no exclude segment", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.replica.removeExcludedSegments(defaultCollectionID)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test segment is exclude segment", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.replica.addExcludedSegments(defaultCollectionID, []*datapb.SegmentInfo{
			{
				ID:           defaultSegmentID,
				CollectionID: defaultCollectionID,
				PartitionID:  defaultPartitionID,
				DmlPosition: &internalpb.MsgPosition{
					Timestamp: Timestamp(1000),
				},
			},
		})
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test misaligned messages", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test no data", func(t *testing.T) {
		msg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		msg.RowIDs = make([]IntPrimaryKey, 0)
		msg.RowData = make([]*commonpb.Blob, 0)
		res := fg.filterInvalidInsertMessage(msg)
		assert.Nil(t, res)
	})
}

func TestFlowGraphFilterDmNode_filterInvalidDeleteMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("delete valid test", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.NotNil(t, res)
	})

	t.Run("test delete no collection", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		msg.CollectionID = UniqueID(1000)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete no partition", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		msg.PartitionID = UniqueID(1000)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.loadType = loadTypePartition
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete not target collection", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete not target partition", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		fg.loadType = loadTypePartition
		fg.partitionID = UniqueID(1000)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete released partition", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		col, err := fg.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.addReleasedPartition(defaultPartitionID)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete misaligned messages", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete no data", func(t *testing.T) {
		msg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		msg.PrimaryKeys = make([]IntPrimaryKey, 0)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})
}

func TestFlowGraphFilterDmNode_Operate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genFilterDMMsg := func() []flowgraph.Msg {
		iMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{iMsg}, 0, 1000, nil, nil)
		return []flowgraph.Msg{msg}
	}

	t.Run("valid test", func(t *testing.T) {
		msg := genFilterDMMsg()
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})

	t.Run("invalid input length", func(t *testing.T) {
		msg := genFilterDMMsg()
		fg, err := getFilterDMNode(ctx)
		assert.NoError(t, err)
		var m flowgraph.Msg
		msg = append(msg, m)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})
}
