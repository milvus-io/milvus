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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func Test_dropAliasTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropAliasTask{
			Req: &milvuspb.DropAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &dropAliasTask{
			Req: &milvuspb.DropAliasRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias}},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_dropAliasTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		mockMeta := mockrootcoord.NewIMetaTable(t)
		mockMeta.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(111)
		alias := funcutil.GenRandomStr()
		core := newTestCore(withInvalidProxyManager(), withMeta(mockMeta))
		task := &dropAliasTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to drop alias", func(t *testing.T) {
		mockMeta := mockrootcoord.NewIMetaTable(t)
		mockMeta.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(111)
		mockMeta.EXPECT().DropAlias(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("failed to alter alias"))
		core := newTestCore(withValidProxyManager(), withMeta(mockMeta))
		alias := funcutil.GenRandomStr()
		task := &dropAliasTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		mockMeta := mockrootcoord.NewIMetaTable(t)
		mockMeta.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(111)
		mockMeta.EXPECT().DropAlias(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		core := newTestCore(withValidProxyManager(), withMeta(mockMeta))
		alias := funcutil.GenRandomStr()
		task := &dropAliasTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropAliasRequest{
				Base:  &commonpb.MsgBase{MsgType: commonpb.MsgType_DropAlias},
				Alias: alias,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}
