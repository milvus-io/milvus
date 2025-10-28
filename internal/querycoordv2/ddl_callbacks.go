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

package querycoordv2

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(s *Server) {
	ddlCallback := &DDLCallbacks{
		Server: s,
	}
	ddlCallback.registerLoadConfigCallbacks()
	ddlCallback.registerResourceGroupCallbacks()
}

type DDLCallbacks struct {
	*Server
}

// registerLoadConfigCallbacks registers the load config callbacks.
func (c *DDLCallbacks) registerLoadConfigCallbacks() {
	registry.RegisterAlterLoadConfigV2AckCallback(c.alterLoadConfigV2AckCallback)
	registry.RegisterDropLoadConfigV2AckCallback(c.dropLoadConfigV2AckCallback)
}

func (c *DDLCallbacks) registerResourceGroupCallbacks() {
	registry.RegisterAlterResourceGroupV2AckCallback(c.alterResourceGroupV2AckCallback)
	registry.RegisterDropResourceGroupV2AckCallback(c.dropResourceGroupV2AckCallback)
}

// startBroadcastWithCollectionIDLock starts a broadcast with collection id lock.
func (c *Server) startBroadcastWithCollectionIDLock(ctx context.Context, collectionID int64) (broadcaster.BroadcastAPI, error) {
	coll, err := c.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start broadcast with collection lock")
	}
	return broadcaster, nil
}
