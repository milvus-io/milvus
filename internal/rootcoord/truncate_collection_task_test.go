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

package rootcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
)

func Test_markCollectionTask_Prapare(t *testing.T) {
	task := markCollectionTask{}
	err := task.Prepare(context.Background())
	assert.NoError(t, err)
}

func Test_markCollectionTask_Execute(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{}, fmt.Errorf("mock error")).Once()
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
		State: etcdpb.CollectionState_CollectionCreating,
	}, nil).Once()
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
		State: etcdpb.CollectionState_CollectionCreated,
	}, nil).Twice()
	meta.EXPECT().ChangeCollectionState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	t.Run("invalid", func(t *testing.T) {
		core := newTestCore(withMeta(meta), withInvalidProxyManager())
		task := markCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			},
			fromState: etcdpb.CollectionState_CollectionCreated,
			toState:   etcdpb.CollectionState_CollectionTruncating,
		}
		err := task.Execute(context.Background()) // get coll fail
		assert.Error(t, err)
		err = task.Execute(context.Background()) // coll state wrong
		assert.Error(t, err)
		err = task.Execute(context.Background()) // invalid proxy manager
		assert.Error(t, err)
	})
	t.Run("valid", func(t *testing.T) {
		core := newTestCore(withMeta(meta), withValidProxyManager())
		task := markCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			},
			fromState: etcdpb.CollectionState_CollectionCreated,
			toState:   etcdpb.CollectionState_CollectionTruncating,
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

func Test_truncateCollectionTask_Prepare(t *testing.T) {
	task := truncateCollectionTask{}
	err := task.Prepare(context.Background())
	assert.NoError(t, err)
}

func Test_truncateCollectionTask_Execute(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
		State: etcdpb.CollectionState_CollectionCreated,
	}, nil).Once()
	meta.EXPECT().ChangeCollectionState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	meta.EXPECT().ExchangeCollectionIDs(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	core := newTestCore(withMeta(meta), withValidProxyManager())
	mt := &markCollectionTask{
		baseTask: baseTask{core: core},
		Req: &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
		},
		fromState: etcdpb.CollectionState_CollectionCreated,
		toState:   etcdpb.CollectionState_CollectionTruncating,
	}
	err := mt.Execute(context.Background())
	assert.NoError(t, err)

	t.Run("invalid", func(t *testing.T) {
		core := newTestCore(withMeta(meta), withInvalidProxyManager())
		task := truncateCollectionTask{
			baseTask: baseTask{core: core},
			mt:       mt,
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
	t.Run("valid", func(t *testing.T) {
		core := newTestCore(withMeta(meta), withValidProxyManager())
		task := truncateCollectionTask{
			baseTask: baseTask{core: core},
			mt:       mt,
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}
