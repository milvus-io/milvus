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

package coordinator

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// registerShardSplit connects the two coordinators that take part in a shard
// split. RootCoord owns the SplitShard ack callback and the collection
// metadata; DataCoord decides to split, records the split and declares the
// targets in the DataView. Neither package imports the other.
//
//   - DataCoord issues a split through RootCoord's BroadcastSplitShard, which
//     plans it against the collection metadata under the resource key of the
//     collection.
//   - The ack callback runs DataCoord's first step before RootCoord publishes
//     the routing, and DataCoord's last step after it. The last step waits
//     for the consumer ack of the copy on the source, which the broadcaster
//     reports to DataCoord through the consumer ack observer.
//
// The hooks run on every cluster that consumes the message. Only the issuer
// is specific to the cluster that splits.
func registerShardSplit(dataCoord *datacoord.Server, rootCoord *rootcoord.Core) {
	registry.RegisterConsumerAckObserver(message.MessageTypeSplitShard, dataCoord.ObserveSplitShardConsumerAck)
	rootcoord.RegisterSplitShardAckHook(dataCoord.SplitShardAckHook)
	rootcoord.RegisterSplitShardAppliedHook(dataCoord.SplitShardAppliedHook)
	dataCoord.SetSplitShardIssuer(func(ctx context.Context, request split.Request, admit func(ctx context.Context) error) error {
		_, err := rootCoord.BroadcastSplitShard(ctx, splitShardRequestOf(request, admit))
		return err
	})
}

// splitShardRequestOf carries DataCoord's admission into RootCoord's plan,
// which runs it under the exclusive resource key of the collection.
func splitShardRequestOf(request split.Request, admit func(ctx context.Context) error) *rootcoord.SplitShardRequest {
	return &rootcoord.SplitShardRequest{
		CollectionID: request.CollectionID,
		SplitTaskID:  request.SplitTaskID,
		Source:       request.Source,
		Targets:      request.Targets,
		Admit:        admit,
	}
}
