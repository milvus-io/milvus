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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func getFilterDeleteNode(ctx context.Context) (*filterDeleteNode, error) {
	historical, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}

	historical.addExcludedSegments(defaultCollectionID, nil)
	return newFilteredDeleteNode(historical, defaultCollectionID), nil
}

func TestFlowGraphFilterDeleteNode_filterDeleteNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fg, err := getFilterDeleteNode(ctx)
	assert.NoError(t, err)
	fg.Name()
}

func TestFlowGraphFilterDeleteNode_filterInvalidDeleteMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("delete valid test", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.NotNil(t, res)
	})

	t.Run("test delete no collection", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msg.CollectionID = UniqueID(1003)
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete not target collection", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})

	t.Run("test delete no data", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		msg.Int64PrimaryKeys = make([]IntPrimaryKey, 0)
		res := fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
		msg.PrimaryKeys = storage.ParsePrimaryKeys2IDs([]primaryKey{})
		res = fg.filterInvalidDeleteMessage(msg)
		assert.Nil(t, res)
	})
}

func TestFlowGraphFilterDeleteNode_Operate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genFilterDeleteMsg := func() []flowgraph.Msg {
		dMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{dMsg}, 0, 1000, nil, nil)
		return []flowgraph.Msg{msg}
	}

	t.Run("valid test", func(t *testing.T) {
		msg := genFilterDeleteMsg()
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})

	t.Run("invalid input length", func(t *testing.T) {
		msg := genFilterDeleteMsg()
		fg, err := getFilterDeleteNode(ctx)
		assert.NoError(t, err)
		var m flowgraph.Msg
		msg = append(msg, m)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})
}
