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

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// getPipelineParams initializes the pipeline parameters.
func getPipelineParams(chunkManager storage.ChunkManager) *util.PipelineParams {
	var (
		rsc         = resource.Resource()
		syncMgr     = syncmgr.NewSyncManager(chunkManager)
		wbMgr       = writebuffer.NewManager(syncMgr)
		coordBroker = broker.NewCoordBroker(rsc.DataCoordClient(), paramtable.GetNodeID())
		cpUpdater   = util.NewChannelCheckpointUpdater(coordBroker)
	)
	return &util.PipelineParams{
		Ctx:                context.Background(),
		Broker:             coordBroker,
		SyncMgr:            syncMgr,
		ChunkManager:       chunkManager,
		WriteBufferManager: wbMgr,
		CheckpointUpdater:  cpUpdater,
		Allocator:          idalloc.NewMAllocator(rsc.IDAllocator()),
		FlushMsgHandler:    newFlushMsgHandler(wbMgr),
	}
}
