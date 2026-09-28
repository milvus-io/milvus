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

package dml

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDeleteRunner_ConcurrentProduceInitializesSharedRequest(t *testing.T) {
	for _, base := range []*commonpb.MsgBase{nil, {MsgID: 42, Timestamp: 123, SourceID: 17}} {
		name := "nil base"
		if base != nil {
			name = "existing base"
		}
		t.Run(name, func(t *testing.T) {
			const producers = 32
			req := &milvuspb.DeleteRequest{Base: base, CollectionName: "test", Expr: "pk > 0"}
			expected := proto.Clone(req).(*milvuspb.DeleteRequest)
			if expected.Base == nil {
				expected.Base = &commonpb.MsgBase{}
			}
			expected.Base.MsgType = commonpb.MsgType_Delete
			expected.Base.SourceID = paramtable.GetNodeID()
			var next atomic.Uint64
			sched, err := scheduler.NewTaskScheduler(context.Background(), deleteProduceAllocatorFunc(func(context.Context) (Timestamp, error) { return next.Add(1), nil }))
			require.NoError(t, err)
			defer sched.Close()
			queue := sched.DmQueue
			queue.SetMaxTaskNum(producers)
			cache := NewMockCache(t)
			cache.On("GetCollectionID", mock.Anything, "", "test").Return(int64(10), nil)
			chMgr := channelmgr.NewMockChannelsMgr(t)
			chMgr.EXPECT().GetChannels(int64(10)).Return([]string{"p0"}, nil)
			runner := &DeleteRunner{
				req: req, queue: queue, collectionID: 10,
				vChannels: []string{"p0_10v0"}, metaCache: cache, chMgr: chMgr,
			}
			start := make(chan struct{})
			tasks := make([]*DeleteTask, producers)
			errs := make([]error, producers)
			var wg sync.WaitGroup
			for i := range producers {
				wg.Go(func() {
					<-start
					keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(i)}}}}
					tasks[i], errs[i] = runner.produce(context.Background(), keys, 20)
				})
			}
			close(start)
			wg.Wait()
			require.True(t, proto.Equal(expected, req), "only the request type and source may change")
			ids := make(map[UniqueID]struct{}, producers)
			for i, task := range tasks {
				require.NoError(t, errs[i])
				require.Same(t, req, task.req)
				require.NotContains(t, ids, task.ID())
				ids[task.ID()] = struct{}{}
				require.Equal(t, Timestamp(task.ID()), task.BeginTs())
				require.Equal(t, []int64{int64(i)}, task.primaryKeys.GetIntId().GetData())
				require.Equal(t, runner.vChannels, task.vChannels)
				require.Equal(t, []string{"p0"}, task.pChannels)
			}
		})
	}
}

type deleteProduceAllocatorFunc func(context.Context) (Timestamp, error)

func (f deleteProduceAllocatorFunc) AllocOne(ctx context.Context) (Timestamp, error) { return f(ctx) }
