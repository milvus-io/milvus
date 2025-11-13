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

package datacoord

import (
	"context"

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
	ddlCallback.registerIndexCallbacks()
}

type DDLCallbacks struct {
	*Server
}

func (c *DDLCallbacks) registerIndexCallbacks() {
	registry.RegisterCreateIndexV2AckCallback(c.createIndexV2AckCallback)
	registry.RegisterAlterIndexV2AckCallback(c.alterIndexV2AckCallback)
	registry.RegisterDropIndexV2AckCallback(c.dropIndexV2Callback)
}

// startBroadcastWithCollectionID starts a broadcast with collection name.
func (s *Server) startBroadcastWithCollectionID(ctx context.Context, collectionID int64) (broadcaster.BroadcastAPI, error) {
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewSharedDBNameResourceKey(dbName), message.NewExclusiveCollectionNameResourceKey(dbName, collectionName))
	if err != nil {
		return nil, err
	}
	return broadcaster, nil
}
