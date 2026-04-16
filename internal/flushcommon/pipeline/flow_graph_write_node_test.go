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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// Regression coverage for PR #48928 review comment on flow_graph_write_node.go:98.
//
// Prior to this PR, BufferData / PrepareInsert failures inside writeNode.Operate
// caused an unrecoverable panic. The PR introduced a non-nil errHandler hook: on
// failure, Operate must call the handler with the underlying error AND return a
// close-msg so the flowgraph propagates shutdown. With a nil handler (DataNode
// compat) the old panic behavior is preserved.
//
// These tests pin that contract at the unit level. We exercise it through the
// BufferData failure branch (line 107-111). The PrepareInsert failure branch
// (line 98-101) invokes the identical `handleError(err)` + close-msg return
// pattern — it is the same two-line snippet, so covering one branch covers the
// contract semantics of the other. Triggering a real PrepareInsert error from
// unit scope is brittle because writebuffer.PrepareInsert is a package-level
// function and its failure modes depend on storage-layer validation details.
//
// The end-to-end "close-msg shuts down the flowgraph" propagation is already
// exercised by existing integration tests (TestWALFlusher, data_sync_service
// tests) which use close messages in the normal drop-collection path.
func TestWriteNode_ErrHandler(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "8"},
				},
			},
		},
	}

	newNode := func(t *testing.T, wbManager writebuffer.BufferManager, handler func(error)) *writeNode {
		t.Helper()
		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().GetSchema(mock.Anything).Return(collSchema).Maybe()
		cfg := &nodeConfig{
			vChannelName: "write-node-test-vchannel",
			metacache:    metaCache,
		}
		node, err := newWriteNode(context.Background(), wbManager, nil, cfg, handler)
		assert.NoError(t, err)
		return node
	}

	// Minimal valid InsertData so PrepareInsert succeeds and Operate proceeds to
	// BufferData — used by the BufferData failure cases.
	validFGMsg := func() *FlowGraphMsg {
		return &FlowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(false),
			StartPositions: []*msgpb.MsgPosition{{Timestamp: 1}},
			EndPositions:   []*msgpb.MsgPosition{{Timestamp: 2}},
			InsertData:     []*writebuffer.InsertData{
				// Non-nil slice short-circuits the PrepareInsert path (see line 94-105).
			},
		}
	}

	t.Run("close msg is passed through unchanged", func(t *testing.T) {
		wbManager := writebuffer.NewMockBufferManager(t)
		node := newNode(t, wbManager, nil)

		out := node.Operate([]Msg{&FlowGraphMsg{BaseMsg: flowgraph.NewBaseMsg(true)}})
		assert.Equal(t, 1, len(out))
		fg, ok := out[0].(*FlowGraphMsg)
		assert.True(t, ok)
		assert.True(t, fg.IsCloseMsg())
	})

	t.Run("BufferData failure with errHandler", func(t *testing.T) {
		injected := errors.New("injected buffer data error")
		wbManager := writebuffer.NewMockBufferManager(t)
		wbManager.EXPECT().
			BufferData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(injected).
			Once()

		var (
			handlerCalled int
			capturedErr   error
		)
		handler := func(err error) {
			handlerCalled++
			capturedErr = err
		}
		node := newNode(t, wbManager, handler)

		var out []Msg
		assert.NotPanics(t, func() {
			out = node.Operate([]Msg{validFGMsg()})
		})
		assert.Equal(t, 1, handlerCalled, "errHandler should be invoked exactly once")
		assert.ErrorIs(t, capturedErr, injected,
			"errHandler should receive the underlying BufferData error unwrapped")
		assert.Equal(t, 1, len(out))
		fg, ok := out[0].(*FlowGraphMsg)
		assert.True(t, ok)
		assert.True(t, fg.IsCloseMsg(),
			"on BufferData failure, the errHandler path must return a close msg to trigger flowgraph shutdown")
	})

	t.Run("BufferData failure with nil handler panics (DataNode compat)", func(t *testing.T) {
		wbManager := writebuffer.NewMockBufferManager(t)
		wbManager.EXPECT().
			BufferData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("boom")).
			Once()

		node := newNode(t, wbManager, nil)
		assert.Panics(t, func() {
			node.Operate([]Msg{validFGMsg()})
		}, "nil errHandler must preserve the legacy DataNode panic-on-failure contract")
	})
}
