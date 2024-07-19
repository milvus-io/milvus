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

package flusherimpl

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/util" // TODO: move util to flushcommon
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
)

var (
	pipelineParams *util.PipelineParams
	initOnce       sync.Once
)

func initPipelineParams() {
	initOnce.Do(func() {
		rsc := resource.Resource()
		syncMgr := syncmgr.NewSyncManager(rsc.ChunkManager())
		coordBroker := broker.NewCoordBroker(rsc.DataCoordClient(), 0 /*TODO: fix paramtable.Get().StreamingNodeCfg.GetNodeID()*/)
		pipelineParams = &util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             coordBroker,
			SyncMgr:            syncMgr,
			ChunkManager:       rsc.ChunkManager(),
			WriteBufferManager: writebuffer.NewManager(syncMgr),
			CheckpointUpdater:  util.NewChannelCheckpointUpdater(coordBroker),
			Allocator:          rsc.IDAllocator(),
		}
	})
}

func GetPipelineParams() *util.PipelineParams {
	initPipelineParams()
	return pipelineParams
}
