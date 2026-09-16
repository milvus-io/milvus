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

	"github.com/milvus-io/milvus/internal/schemaevolution"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type schemaInstallBroadcaster struct {
	broadcaster.BroadcastAPI
	core *Core
}

func (b *schemaInstallBroadcaster) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	alter, err := message.AsBroadcastAlterCollectionMessageV2(msg)
	if err != nil || !messageutil.IsSchemaChange(alter.Header()) || b.core.schemaInstallGate == nil {
		return b.BroadcastAPI.Broadcast(ctx, msg)
	}

	collectionID := alter.Header().GetCollectionId()
	if err := b.core.checkSchemaInstallVersionContract(ctx); err != nil {
		// Version admission is pre-cut and runs before the install gate closes.
		// A mixed-version deployment can therefore finish its upgrade without
		// recovering or aborting a schema broadcast task.
		return nil, err
	}
	if err := b.core.schemaInstallGate.PrepareSchemaInstall(ctx, collectionID); err != nil {
		// Broadcast has not started, so this is an unambiguous pre-cut abort.
		// Prepare itself intentionally leaves the gate closed because the same
		// method is also used by post-cut ACK recovery.
		b.core.schemaInstallGate.AbortSchemaInstall(ctx, collectionID)
		return nil, err
	}

	result, err := b.BroadcastAPI.Broadcast(ctx, msg)
	if err != nil && broadcaster.IsBroadcastTaskNotCreated(err) {
		b.core.schemaInstallGate.AbortSchemaInstall(ctx, collectionID)
	}
	return result, err
}

func (c *Core) checkSchemaInstallVersionContract(ctx context.Context) error {
	provider := c.schemaInstallVersionProvider
	if provider == nil {
		provider = c.session
	}
	return schemaevolution.CheckPhase0VersionContract(ctx, provider)
}

func (c *Core) wrapSchemaInstallBroadcaster(api broadcaster.BroadcastAPI) broadcaster.BroadcastAPI {
	if c.schemaInstallGate == nil {
		return api
	}
	return &schemaInstallBroadcaster{
		BroadcastAPI: api,
		core:         c,
	}
}
