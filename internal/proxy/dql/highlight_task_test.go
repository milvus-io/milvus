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

package dql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

func TestHighlightTask(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	t.Run("basic methods", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		t.Run("traceCtx", func(t *testing.T) {
			traceCtx := task.TraceCtx()
			assert.NotNil(t, traceCtx)
			assert.Equal(t, ctx, traceCtx)
		})

		t.Run("id", func(t *testing.T) {
			id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
			task.SetID(id)
			assert.Equal(t, id, task.ID())
		})

		t.Run("name", func(t *testing.T) {
			assert.Equal(t, HighlightTaskName, task.Name())
		})

		t.Run("type", func(t *testing.T) {
			assert.Equal(t, commonpb.MsgType_Undefined, task.Type())
		})

		t.Run("ts", func(t *testing.T) {
			ts := Timestamp(time.Now().UnixNano())
			task.SetTs(ts)
			assert.Equal(t, ts, task.BeginTs())
			assert.Equal(t, ts, task.EndTs())
		})
	})

	t.Run("OnEnqueue", func(t *testing.T) {
		task := &HighlightTask{
			ctx:                 ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{},
			Condition:           NewTaskCondition(ctx),
		}

		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.NotNil(t, task.Base)
		assert.Equal(t, commonpb.MsgType_Undefined, task.Base.MsgType)
		assert.Equal(t, paramtable.GetNodeID(), task.Base.SourceID)
	})

	t.Run("PreExecute", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		err := task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("getHighlightOnShardleader success", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base:  commonpbutil.NewMsgBase(),
				Topks: []int64{10},
				Tasks: []*querypb.HighlightTask{
					{},
				},
			},
			Condition: NewTaskCondition(ctx),
		}

		mockQN := mocks.NewMockQueryNodeClient(t)
		expectedResp := &querypb.GetHighlightResponse{
			Status: merr.Success(),
			Results: []*querypb.HighlightResult{
				{
					Fragments: []*querypb.HighlightFragment{
						{
							StartOffset: 0,
							EndOffset:   10,
							Offsets:     []int64{0, 5, 5, 10},
						},
					},
				},
			},
		}

		mockQN.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(expectedResp, nil)

		err := task.getHighlightOnShardleader(ctx, 1, mockQN, "test_channel")
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, expectedResp, task.result)
		assert.Equal(t, "test_channel", task.Channel)
	})

	t.Run("getHighlightOnShardleader rpc error", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		mockQN := mocks.NewMockQueryNodeClient(t)
		mockQN.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

		err := task.getHighlightOnShardleader(ctx, 1, mockQN, "test_channel")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rpc error")
	})

	t.Run("getHighlightOnShardleader status error", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		mockQN := mocks.NewMockQueryNodeClient(t)
		expectedResp := &querypb.GetHighlightResponse{
			Status: merr.Status(errors.New("status error")),
		}

		mockQN.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(expectedResp, nil)

		err := task.getHighlightOnShardleader(ctx, 1, mockQN, "test_channel")
		assert.Error(t, err)
	})

	t.Run("Execute success", func(t *testing.T) {
		task := &HighlightTask{
			ctx:            ctx,
			collectionName: "test_collection",
			collectionID:   100,
			dbName:         "default",
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base:  commonpbutil.NewMsgBase(),
				Topks: []int64{10, 20},
				Tasks: []*querypb.HighlightTask{
					{},
					{},
				},
			},
			Condition: NewTaskCondition(ctx),
		}

		mockLB := shardclient.NewMockLBPolicy(t)
		mockQN := mocks.NewMockQueryNodeClient(t)

		expectedResp := &querypb.GetHighlightResponse{
			Status: merr.Success(),
			Results: []*querypb.HighlightResult{
				{
					Fragments: []*querypb.HighlightFragment{
						{
							StartOffset: 0,
							EndOffset:   10,
							Offsets:     []int64{0, 5, 5, 10},
						},
					},
				},
			},
		}

		mockQN.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(expectedResp, nil)

		mockLB.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Run(
			func(ctx context.Context, workload shardclient.CollectionWorkLoad) {
				assert.Equal(t, "default", workload.Db)
				assert.Equal(t, "test_collection", workload.CollectionName)
				assert.Equal(t, int64(100), workload.CollectionID)
				assert.Equal(t, int64(4), workload.Nq) // len(topks) * len(tasks) = 2 * 2 = 4
				err := workload.Exec(ctx, 1, mockQN, "test_channel")
				assert.NoError(t, err)
			},
		).Return(nil)

		task.lb = mockLB

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
	})

	t.Run("Execute lb error", func(t *testing.T) {
		task := &HighlightTask{
			ctx:            ctx,
			collectionName: "test_collection",
			collectionID:   100,
			dbName:         "default",
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		mockLB := shardclient.NewMockLBPolicy(t)
		mockLB.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Return(errors.New("lb error"))

		task.lb = mockLB

		err := task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lb error")
	})

	t.Run("PostExecute", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		err := task.PostExecute(ctx)
		assert.NoError(t, err)
	})
}
