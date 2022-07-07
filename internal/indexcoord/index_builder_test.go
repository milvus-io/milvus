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

package indexcoord

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/stretchr/testify/assert"
)

func createMetaTable() *metaTable {
	return &metaTable{
		indexBuildID2Meta: map[UniqueID]*Meta{
			1: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 1,
					State:        commonpb.IndexState_Unissued,
					NodeID:       1,
					MarkDeleted:  true,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			2: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 2,
					State:        commonpb.IndexState_Unissued,
					NodeID:       0,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			3: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 3,
					State:        commonpb.IndexState_Unissued,
					NodeID:       1,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			4: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 4,
					State:        commonpb.IndexState_InProgress,
					NodeID:       1,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			5: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 5,
					State:        commonpb.IndexState_InProgress,
					NodeID:       3,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			6: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 6,
					State:        commonpb.IndexState_Finished,
					NodeID:       2,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
			7: {
				indexMeta: &indexpb.IndexMeta{
					IndexBuildID: 7,
					State:        commonpb.IndexState_Failed,
					NodeID:       0,
					Req: &indexpb.BuildIndexRequest{
						NumRows: 100,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
					},
				},
			},
		},
		client: &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return true, nil
			},
		},
	}
}

func TestIndexBuilder(t *testing.T) {
	ctx := context.Background()

	ic := &IndexCoord{
		loopCtx:            ctx,
		reqTimeoutInterval: time.Second * 5,
		dataCoordClient: &DataCoordMock{
			Fail: false,
			Err:  false,
		},
		nodeManager: &NodeManager{
			nodeClients: map[UniqueID]types.IndexNode{
				4: &indexnode.Mock{
					Err:     false,
					Failure: false,
				},
			},
		},
	}

	ib := newIndexBuilder(ctx, ic, createMetaTable(), []UniqueID{1, 2})

	assert.Equal(t, 6, len(ib.tasks))
	assert.Equal(t, indexTaskDeleted, ib.tasks[1])
	assert.Equal(t, indexTaskInit, ib.tasks[2])
	assert.Equal(t, indexTaskRetry, ib.tasks[3])
	assert.Equal(t, indexTaskInProgress, ib.tasks[4])
	assert.Equal(t, indexTaskRetry, ib.tasks[5])
	assert.Equal(t, indexTaskDone, ib.tasks[6])

	ib.scheduleDuration = time.Millisecond * 500
	ib.Start()

	t.Run("enqueue", func(t *testing.T) {
		ib.meta.indexBuildID2Meta[8] = &Meta{
			indexMeta: &indexpb.IndexMeta{
				IndexBuildID: 8,
				State:        commonpb.IndexState_Unissued,
				NodeID:       0,
				Req: &indexpb.BuildIndexRequest{
					NumRows: 100,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "128",
						},
					},
				},
			},
		}
		ib.enqueue(8)
	})

	t.Run("node down", func(t *testing.T) {
		ib.nodeDown(1)
	})

	t.Run("updateStateByMeta", func(t *testing.T) {
		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: 2,
			State:        commonpb.IndexState_Finished,
			NodeID:       3,
		}
		ib.updateStateByMeta(indexMeta)

		indexMeta = &indexpb.IndexMeta{
			IndexBuildID: 3,
			State:        commonpb.IndexState_Finished,
			NodeID:       3,
		}
		ib.updateStateByMeta(indexMeta)

		indexMeta = &indexpb.IndexMeta{
			IndexBuildID: 4,
			State:        commonpb.IndexState_Failed,
			NodeID:       3,
		}
		ib.updateStateByMeta(indexMeta)

		indexMeta = &indexpb.IndexMeta{
			IndexBuildID: 5,
			State:        commonpb.IndexState_Unissued,
			NodeID:       3,
		}
		ib.updateStateByMeta(indexMeta)

		indexMeta = &indexpb.IndexMeta{
			IndexBuildID: 8,
			State:        commonpb.IndexState_Finished,
			NodeID:       3,
		}
		ib.updateStateByMeta(indexMeta)

		for {
			ib.taskMutex.Lock()
			if len(ib.tasks) == 1 {
				ib.taskMutex.Unlock()
				break
			}
			ib.taskMutex.Unlock()
			time.Sleep(time.Second)
		}

		ib.taskMutex.RLock()
		assert.Equal(t, indexTaskInProgress, ib.tasks[5])
		ib.taskMutex.RUnlock()
	})

	ib.Stop()

	t.Run("save meta error", func(t *testing.T) {
		mt := createMetaTable()
		mt.client = &mockETCDKV{
			compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
				return true, errors.New("error")
			},
		}
		ib2 := newIndexBuilder(ctx, ic, mt, []UniqueID{1, 2})
		ib2.scheduleDuration = time.Millisecond * 500
		ib2.Start()
		time.Sleep(time.Second)
		ib2.Stop()

		assert.Equal(t, 6, len(ib2.tasks))
		assert.Equal(t, indexTaskDeleted, ib2.tasks[1])
		assert.Equal(t, indexTaskInit, ib2.tasks[2])
		assert.Equal(t, indexTaskRetry, ib2.tasks[3])
		assert.Equal(t, indexTaskInProgress, ib2.tasks[4])
		assert.Equal(t, indexTaskRetry, ib2.tasks[5])
		assert.Equal(t, indexTaskDone, ib2.tasks[6])
	})
}

func TestIndexBuilder_Error(t *testing.T) {
	ctx := context.Background()

	t.Run("PeekClient fail", func(t *testing.T) {
		ic := &IndexCoord{
			loopCtx:            ctx,
			reqTimeoutInterval: time.Second * 5,
			dataCoordClient: &DataCoordMock{
				Fail: false,
				Err:  false,
			},
			nodeManager: &NodeManager{},
		}
		mt := &metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						State:        commonpb.IndexState_Unissued,
						NodeID:       0,
						MarkDeleted:  false,
						Req: &indexpb.BuildIndexRequest{
							NumRows: 100,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key:   "dim",
									Value: "128",
								},
							},
						},
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}

		ib := newIndexBuilder(ic.loopCtx, ic, mt, []UniqueID{})
		ib.scheduleDuration = time.Millisecond * 500
		ib.Start()
		time.Sleep(time.Second)
		ib.Stop()
	})

	t.Run("update version fail", func(t *testing.T) {
		ic := &IndexCoord{
			loopCtx:            ctx,
			reqTimeoutInterval: time.Second * 5,
			dataCoordClient: &DataCoordMock{
				Fail: false,
				Err:  false,
			},
			nodeManager: &NodeManager{
				nodeClients: map[UniqueID]types.IndexNode{
					1: &indexnode.Mock{
						Err:     false,
						Failure: false,
					},
				},
			},
		}
		mt := &metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						State:        commonpb.IndexState_Unissued,
						NodeID:       0,
						MarkDeleted:  false,
						Req: &indexpb.BuildIndexRequest{
							NumRows: 100,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key:   "dim",
									Value: "128",
								},
							},
						},
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, errors.New("error")
				},
			},
		}

		ib := newIndexBuilder(ic.loopCtx, ic, mt, []UniqueID{})
		ib.scheduleDuration = time.Second
		ib.Start()
		time.Sleep(time.Second)
		ib.Stop()
	})

	t.Run("acquire lock fail", func(t *testing.T) {
		ic := &IndexCoord{
			loopCtx:            ctx,
			reqTimeoutInterval: time.Second * 5,
			dataCoordClient: &DataCoordMock{
				Fail: false,
				Err:  true,
			},
			nodeManager: &NodeManager{
				nodeClients: map[UniqueID]types.IndexNode{
					1: &indexnode.Mock{
						Err:     false,
						Failure: false,
					},
				},
			},
		}
		mt := &metaTable{
			indexBuildID2Meta: map[UniqueID]*Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 1,
						State:        commonpb.IndexState_Unissued,
						NodeID:       0,
						MarkDeleted:  false,
						Req: &indexpb.BuildIndexRequest{
							NumRows: 100,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key:   "dim",
									Value: "128",
								},
							},
						},
					},
				},
			},
			client: &mockETCDKV{
				compareVersionAndSwap: func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
					return true, nil
				},
			},
		}
		ib := newIndexBuilder(ic.loopCtx, ic, mt, []UniqueID{})
		ib.scheduleDuration = time.Second
		ib.Start()
		time.Sleep(time.Second)
		ib.Stop()
	})
}
