// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// dmlAutoLoadWAL records writes in place of the streaming WAL.
type dmlAutoLoadWAL struct {
	streaming.WALAccesser
	err     error
	calls   int
	deleted []int64
}

func (w *dmlAutoLoadWAL) AppendMessages(_ context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
	w.calls++
	responses := types.NewAppendResponseN(len(msgs))
	if w.err != nil {
		responses.FillAllError(w.err)
		return responses
	}
	for _, msg := range msgs {
		body := message.MustAsMutableDeleteMessageV1(msg).MustBody()
		w.deleted = append(w.deleted, body.GetPrimaryKeys().GetIntId().GetData()...)
	}
	responses.FillAllResponse(types.AppendResponse{AppendResult: &types.AppendResult{TimeTick: 1000}})
	return responses
}

// ResolvePChannelInfo gives partial upserts the term they read under.
func (w *dmlAutoLoadWAL) ResolvePChannelInfo(_ context.Context, vchannel string) (types.PChannelInfo, error) {
	return types.PChannelInfo{Name: funcutil.ToPhysicalChannel(vchannel), Term: 9, AccessMode: types.AccessModeRW}, nil
}

type dmlAutoLoadEnv struct {
	pending sync.WaitGroup
	node    *Proxy
	client  *dqlRetryClient
	wal     *dmlAutoLoadWAL
}

// Use the real public Proxy entries, DeleteRunner, scheduler, DeleteTask, upsert
// PreExecute, legacy query client, shard retries, gRPC error transport and full DQL
// retries. The SN and QN servers, coordinator and WAL are test doubles. So are the
// query task's planning and rendering, and the upsert's append.
func newDMLAutoLoadEnv(t *testing.T, phase string, autoID bool) *dmlAutoLoadEnv {
	t.Helper()
	metacache.MockWriteSchemaForTest(t)
	node, client := newDQLRetryProxy(t, phase)
	client.server.retrieved = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}

	cache := node.metaCache.(*MockCache)
	schema, err := cache.GetCollectionSchema(node.ctx, "db", "collection")
	require.NoError(t, err)
	schema.PkField.AutoID = autoID
	cache.EXPECT().GetDatabaseInfo(mock.Anything, "db").Return(&databaseInfo{DBID: 9}, nil).Maybe()
	cache.EXPECT().GetPartitionInfo(mock.Anything, "db", "collection", "").Return(&partitionInfo{Name: "_default", PartitionID: 11}, nil).Maybe()
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "collection", "_default").Return(int64(11), nil).Maybe()
	chMgr := channelmgr.NewMockChannelsMgr(t)
	chMgr.EXPECT().GetVChannels(int64(100)).Return([]string{"v0"}, nil).Maybe()
	chMgr.EXPECT().GetChannels(int64(100)).Return([]string{"p0"}, nil).Maybe()
	node.chMgr = chMgr

	node.rowIDAllocator = &allocator.IDAllocator{}
	allocOne := mockey.Mock((*allocator.IDAllocator).AllocOne).Return(int64(42), nil).Build()
	t.Cleanup(func() { allocOne.UnPatch() })
	alloc := mockey.Mock((*allocator.IDAllocator).Alloc).Return(int64(42), int64(43), nil).Build()
	t.Cleanup(func() { alloc.UnPatch() })

	fakeWAL := &dmlAutoLoadWAL{}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })

	queryPre := mockey.Mock((*queryTask).PreExecute).To(func(qt *queryTask, _ context.Context) error {
		dql.SetQueryReadSchemaForTest(qt, schema)
		qt.CollectionID = 100
		return nil
	}).Build()
	t.Cleanup(func() { queryPre.UnPatch() })
	queryPost := mockey.Mock((*queryTask).PostExecute).Return(nil).Build()
	t.Cleanup(func() { queryPost.UnPatch() })
	queryResult := mockey.Mock((*queryTask).Result).To(func(*queryTask) *milvuspb.QueryResults {
		return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: autoLoadTestRow()}
	}).Build()
	t.Cleanup(func() { queryResult.UnPatch() })
	upsertWrite := mockey.Mock((*upsertTask).Execute).To(func(*upsertTask, context.Context) error {
		fakeWAL.calls++
		return fakeWAL.err
	}).Build()
	t.Cleanup(func() { upsertWrite.UnPatch() })

	require.NoError(t, node.sched.Start())
	return &dmlAutoLoadEnv{node: node, client: client, wal: fakeWAL}
}

func (e *dmlAutoLoadEnv) delete(t *testing.T, expr string) *milvuspb.MutationResult {
	t.Helper()
	resp, err := e.node.Delete(e.node.ctx, &milvuspb.DeleteRequest{DbName: "db", CollectionName: "collection", Expr: expr})
	require.NoError(t, err)
	return resp
}

func (e *dmlAutoLoadEnv) upsert(t *testing.T, partial bool) *milvuspb.MutationResult {
	t.Helper()
	resp, err := e.node.Upsert(e.node.ctx, &milvuspb.UpsertRequest{
		DbName: "db", CollectionName: "collection", NumRows: 1, PartialUpdate: partial, FieldsData: autoLoadTestRow(),
	})
	require.NoError(t, err)
	return resp
}

// The writes that read existing rows first.
var readingWrites = []struct {
	name   string
	autoID bool
	write  func(*testing.T, *dmlAutoLoadEnv) *milvuspb.MutationResult
}{
	{name: "expression delete", write: func(t *testing.T, e *dmlAutoLoadEnv) *milvuspb.MutationResult { return e.delete(t, "id > 0") }},
	{name: "autoID upsert", autoID: true, write: func(t *testing.T, e *dmlAutoLoadEnv) *milvuspb.MutationResult { return e.upsert(t, false) }},
	{name: "partial upsert", write: func(t *testing.T, e *dmlAutoLoadEnv) *milvuspb.MutationResult { return e.upsert(t, true) }},
}

func TestDMLReadLoadsUnloadedCollection(t *testing.T) {
	for _, tc := range readingWrites {
		t.Run(tc.name, func(t *testing.T) {
			env := newDMLAutoLoadEnv(t, "", tc.autoID)
			// The WAL owner has no View for the tenant, so GetQueryPlan answers VIEW_NOT_FOUND.
			env.client.server.loaded.Store(false)
			env.client.expectAutoLoad(t)

			resp := tc.write(t, env)

			require.NoError(t, merr.Error(resp.GetStatus()))
			require.EqualValues(t, 1, env.client.loads.Load())
			require.EqualValues(t, 1, env.client.checks.Load())
			require.EqualValues(t, 1, env.client.server.queries.Load(), "the first read runs after the load finished")
			require.Equal(t, 1, env.wal.calls)
		})
	}
}

// A release racing the read restarts it, and the write happens once.
func TestDMLReadRetriesAfterRelease(t *testing.T) {
	for _, tc := range readingWrites {
		for _, phase := range []string{"plan", "query"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				env := newDMLAutoLoadEnv(t, phase, tc.autoID)
				env.client.expectAutoLoad(t)

				resp := tc.write(t, env)

				require.NoError(t, merr.Error(resp.GetStatus()))
				require.True(t, env.client.server.injected.Load())
				require.EqualValues(t, 1, env.client.loads.Load())
				require.EqualValues(t, 2, env.client.checks.Load())
				require.Equal(t, 1, env.wal.calls)
			})
		}
	}
}

// Writes that read nothing must not load the tenant.
func TestDMLWithoutReadDoesNotLoad(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*testing.T, *dmlAutoLoadEnv) *milvuspb.MutationResult
	}{
		{"delete by primary key", func(t *testing.T, e *dmlAutoLoadEnv) *milvuspb.MutationResult { return e.delete(t, "id in [7, 8]") }},
		{"plain upsert", func(t *testing.T, e *dmlAutoLoadEnv) *milvuspb.MutationResult { return e.upsert(t, false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newDMLAutoLoadEnv(t, "", false)
			env.client.server.loaded.Store(false)

			resp := tc.write(t, env)

			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Zero(t, env.client.server.plans.Load())
			require.Equal(t, 1, env.wal.calls)
		})
	}
}

func TestDMLReadWithAutoLoadDisabled(t *testing.T) {
	for _, tc := range readingWrites {
		t.Run(tc.name, func(t *testing.T) {
			env := newDMLAutoLoadEnv(t, "", tc.autoID)
			require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
			env.client.server.loaded.Store(false)

			resp := tc.write(t, env)

			require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded)
			require.Zero(t, env.wal.calls)
		})
	}
}

// A write may have landed on some shards; repeating it is not safe even when the
// error looks like a retired view.
func TestDMLDoesNotRetryWrite(t *testing.T) {
	for _, tc := range readingWrites {
		t.Run(tc.name, func(t *testing.T) {
			env := newDMLAutoLoadEnv(t, "", tc.autoID)
			env.client.expectAutoLoad(t)
			env.wal.err = merr.Wrap(merr.ErrQueryViewNotReady, "write failed")

			resp := tc.write(t, env)

			require.Error(t, merr.Error(resp.GetStatus()))
			require.EqualValues(t, 1, env.client.server.queries.Load())
			require.EqualValues(t, 1, env.client.checks.Load())
			require.Equal(t, 1, env.wal.calls)
		})
	}
}

func TestDMLFailsWhenLoadCannotStart(t *testing.T) {
	for _, tc := range readingWrites {
		t.Run(tc.name, func(t *testing.T) {
			env := newDMLAutoLoadEnv(t, "", tc.autoID)
			env.client.server.loaded.Store(false)
			env.client.coordinator.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).
				Return(merr.Status(merr.WrapErrIndexNotFoundForCollection("collection")), nil).Once()

			resp := tc.write(t, env)

			require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrIndexNotFound)
			require.Zero(t, env.client.server.plans.Load())
			require.Zero(t, env.wal.calls)
		})
	}
}

func TestExpressionDeleteWritesTheRowsItRead(t *testing.T) {
	env := newDMLAutoLoadEnv(t, "", false)
	env.client.expectAutoLoad(t)

	resp := env.delete(t, "id > 0")

	require.NoError(t, merr.Error(resp.GetStatus()))
	require.EqualValues(t, 3, resp.GetDeleteCnt())
	require.Equal(t, []int64{1, 2, 3}, env.wal.deleted)
}

// An invalid expression fails before the read, so it must not load the tenant.
func TestInvalidExpressionDeleteDoesNotLoad(t *testing.T) {
	env := newDMLAutoLoadEnv(t, "", false)
	env.client.server.loaded.Store(false)

	resp := env.delete(t, "no_such_field > 0")

	require.Error(t, merr.Error(resp.GetStatus()))
	require.Zero(t, env.client.server.plans.Load())
	require.Zero(t, env.wal.calls)
}

// addReadyDMLCollection adds an independent, non-AutoID collection, whose full
// upsert needs no read. The actual DML queue and Upsert PreExecute remain in use.
func addReadyDMLCollection(t *testing.T, env *dmlAutoLoadEnv) {
	t.Helper()
	cache := env.node.metaCache.(*MockCache)
	original, err := cache.GetCollectionSchema(env.node.ctx, "db", "collection")
	require.NoError(t, err)
	collection := proto.Clone(original.CollectionSchema).(*schemapb.CollectionSchema)
	collection.Name = "ready"
	for _, field := range collection.Fields {
		field.AutoID = false
	}
	schema, err := newSchemaInfo(collection)
	require.NoError(t, err)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "ready").Return(int64(200), nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "ready", mock.Anything).Return(&collectionInfo{
		CollID: 200, VChannels: []string{"v1"}, Schema: schema,
	}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, "db", "ready").Return(schema, nil).Maybe()
	cache.EXPECT().GetPartitionInfo(mock.Anything, "db", "ready", "").Return(&partitionInfo{Name: "_default", PartitionID: 21}, nil).Maybe()
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "ready", "_default").Return(int64(21), nil).Maybe()
	chMgr := env.node.chMgr.(*channelmgr.MockChannelsMgr)
	chMgr.EXPECT().GetVChannels(int64(200)).Return([]string{"v1"}, nil).Maybe()
	chMgr.EXPECT().GetChannels(int64(200)).Return([]string{"p1"}, nil).Maybe()
}

func startDMLUpsert(ctx context.Context, env *dmlAutoLoadEnv, collection string, partial bool) <-chan *commonpb.Status {
	done := make(chan *commonpb.Status, 1)
	env.pending.Add(1)
	go func() {
		defer env.pending.Done()
		resp, err := env.node.Upsert(ctx, &milvuspb.UpsertRequest{
			DbName: "db", CollectionName: collection, NumRows: 1, PartialUpdate: partial, FieldsData: autoLoadTestRow(),
		})
		if err != nil {
			done <- merr.Status(err)
			return
		}
		done <- resp.GetStatus()
	}()
	return done
}

func TestDMLReadinessWaitDoesNotBlockOtherCollection(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, phase := range []string{"initial", "query"} {
			for _, outcome := range []string{"ready", "cancel", "deadline"} {
				t.Run(fmt.Sprintf("partial=%t/%s/%s", partial, phase, outcome), func(t *testing.T) {
					env := newDMLAutoLoadEnv(t, phase, !partial)
					addReadyDMLCollection(t, env)
					waiting, release := make(chan struct{}), make(chan struct{})
					var once sync.Once
					t.Cleanup(func() { once.Do(func() { close(release) }) })
					calls := 0
					env.client.coordinator.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
						func(ctx context.Context, req *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
							calls++
							require.EqualValues(t, 100, req.GetCollectionID())
							if phase == "query" && calls == 1 {
								return merr.Success(), nil
							}
							close(waiting)
							select {
							case <-release:
								env.client.server.loaded.Store(true)
								return merr.Success(), nil
							case <-ctx.Done():
								return nil, ctx.Err()
							}
						})
					ctx, cancel := context.WithCancel(env.node.ctx)
					if outcome == "deadline" {
						cancel()
						ctx, cancel = context.WithTimeout(env.node.ctx, time.Second)
					}
					defer cancel()
					t.Cleanup(func() {
						cancel()
						once.Do(func() { close(release) })
						env.pending.Wait()
						waitDMLQueueIdle(t, env.node)
					})
					cold := startDMLUpsert(ctx, env, "collection", partial)
					awaitAutoLoadSchedulerResult(t, waiting)
					// MaxTaskNum is one. B must reach its write while A is still
					// waiting, including a release after A's first admitted read.
					ready := startDMLUpsert(env.node.ctx, env, "ready", false)
					require.NoError(t, merr.Error(awaitAutoLoadSchedulerResult(t, ready)))
					require.Equal(t, 1, env.wal.calls)
					switch outcome {
					case "ready":
						once.Do(func() { close(release) })
					case "cancel":
						cancel()
					}
					status := awaitAutoLoadSchedulerResult(t, cold)
					// WaitToFinish may return on ctx.Done before the scheduler
					// finishes cleanup. Check no write happened after task exit.
					waitDMLQueueIdle(t, env.node)
					if outcome == "ready" {
						require.NoError(t, merr.Error(status))
						require.Equal(t, 2, env.wal.calls)
					} else {
						want := context.Canceled
						if outcome == "deadline" {
							want = context.DeadlineExceeded
						}
						require.Equal(t, merr.Code(want), status.GetCode())
						require.Equal(t, 1, env.wal.calls, "a canceled read must never reach the mutation")
					}
					if phase == "query" {
						require.True(t, env.client.server.injected.Load())
					}
				})
			}
		}
	}
}

func waitDMLQueueIdle(t *testing.T, node *Proxy) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, queue := range node.sched.GetMetrics() {
			if queue.Type == "dml" {
				return queue.PendingCount == 0 && queue.ExecutingCount == 0
			}
		}
		return false
	}, time.Second, time.Millisecond)
}
