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

package util

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type PipelineParams struct {
	Ctx                context.Context
	Broker             broker.Broker
	SyncMgr            syncmgr.SyncManager
	TimeTickSender     StatsUpdater        // reference to TimeTickSender
	CompactionExecutor compaction.Executor // reference to compaction executor
	MsgStreamFactory   dependency.Factory
	DispClient         msgdispatcher.Client
	ChunkManager       storage.ChunkManager
	Session            *sessionutil.Session
	WriteBufferManager writebuffer.BufferManager
	CheckpointUpdater  *ChannelCheckpointUpdater
	Allocator          allocator.Interface
	FlushMsgHandler    flusher.FlushMsgHandler
}

// TimeRange is a range of timestamp contains the min-timestamp and max-timestamp
type TimeRange struct {
	TimestampMin typeutil.Timestamp
	TimestampMax typeutil.Timestamp
}

func StartTracer(msg msgstream.TsMsg, name string) (context.Context, trace.Span) {
	ctx := msg.TraceCtx()
	if ctx == nil {
		ctx = context.Background()
	}
	sp := trace.SpanFromContext(ctx)
	if sp.SpanContext().IsValid() {
		return ctx, sp
	}
	return otel.Tracer(typeutil.DataNodeRole).Start(ctx, name)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
