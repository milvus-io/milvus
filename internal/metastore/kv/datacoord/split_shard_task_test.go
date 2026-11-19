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

package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSplitShardTaskCatalog(t *testing.T) {
	ctx := context.Background()
	task := &datapb.SplitShardTask{
		TaskId:         7,
		CollectionId:   100,
		SourceVchannel: "by-dev-rootcoord-dml_0_100v0",
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "by-dev-rootcoord-dml_1_100v1", GenesisTimeTick: 11},
			{Vchannel: "by-dev-rootcoord-dml_2_100v2", GenesisTimeTick: 12},
		},
		SwitchTimeTick: 10,
		State:          datapb.SplitShardTaskState_SplitShardTaskWindowOpen,
	}
	key := SplitShardTaskPrefix + "/100/7"

	t.Run("save, list and drop use one key per task", func(t *testing.T) {
		stored := map[string]string{}
		kv := mocks.NewMetaKv(t)
		kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, k, v string) error {
			stored[k] = v
			return nil
		})
		kv.EXPECT().WalkWithPrefix(mock.Anything, SplitShardTaskPrefix+"/", mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, _ int, fn func([]byte, []byte) error) error {
				for k, v := range stored {
					if err := fn([]byte(k), []byte(v)); err != nil {
						return err
					}
				}
				return nil
			})
		kv.EXPECT().Remove(mock.Anything, key).RunAndReturn(func(_ context.Context, k string) error {
			delete(stored, k)
			return nil
		})
		catalog := NewCatalog(kv, "", "")

		assert.NoError(t, catalog.SaveSplitShardTask(ctx, nil))
		assert.Empty(t, stored)
		assert.NoError(t, catalog.SaveSplitShardTask(ctx, task))
		assert.Contains(t, stored, key)

		tasks, err := catalog.ListSplitShardTask(ctx)
		assert.NoError(t, err)
		assert.Len(t, tasks, 1)
		assert.True(t, proto.Equal(task, tasks[0]))

		assert.NoError(t, catalog.DropSplitShardTask(ctx, task))
		tasks, err = catalog.ListSplitShardTask(ctx)
		assert.NoError(t, err)
		assert.Empty(t, tasks)
	})

	t.Run("a record that cannot be decoded fails the list", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		kv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, _ int, fn func([]byte, []byte) error) error {
				return fn([]byte(key), []byte{0xff, 0xff})
			})
		_, err := NewCatalog(kv, "", "").ListSplitShardTask(ctx)
		assert.Error(t, err)
	})

	t.Run("a failed walk fails the list", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		kv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock"))
		_, err := NewCatalog(kv, "", "").ListSplitShardTask(ctx)
		assert.Error(t, err)
	})
}
