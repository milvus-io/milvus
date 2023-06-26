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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func Test_renameCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		task := &renameCollectionTask{
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_renameCollectionTask_Execute(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		core := newTestCore(withInvalidProxyManager())
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to rename collection", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.RenameCollectionFunc = func(ctx context.Context, oldName string, newName string, ts Timestamp) error {
			return errors.New("fail")
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &renameCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.RenameCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_RenameCollection,
				},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})
}
